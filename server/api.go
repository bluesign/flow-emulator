/*
 * Flow Emulator
 *
 * Copyright 2019 Dapper Labs, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package server

import (
	"encoding/json"
	"github.com/onflow/flow-emulator/server/adapters"
	flowgo "github.com/onflow/flow-go/model/flow"
	"net/http"

	fvmerrors "github.com/onflow/flow-go/fvm/errors"

	"github.com/gorilla/mux"

	"golang.org/x/exp/slices"

	"github.com/onflow/flow-emulator/storage"
)

type BlockResponse struct {
	Height  int    `json:"height"`
	BlockId string `json:"blockId"`
	Context string `json:"context,omitempty"`
}

type EmulatorAPIServer struct {
	router  *mux.Router
	server  *EmulatorServer
	adapter *adapters.AccessAdapter
	store   storage.Store
}

func NewEmulatorAPIServer(server *EmulatorServer, adapter *adapters.AccessAdapter, store storage.Store) *EmulatorAPIServer {
	router := mux.NewRouter().StrictSlash(true)
	r := &EmulatorAPIServer{router: router,
		server:  server,
		adapter: adapter,
		store:   store,
	}

	router.HandleFunc("/emulator/newBlock", r.CommitBlock)

	router.HandleFunc("/emulator/snapshots", r.SnapshotCreate).Methods("POST")
	router.HandleFunc("/emulator/snapshots", r.SnapshotList).Methods("GET")
	router.HandleFunc("/emulator/snapshots/{name}", r.SnapshotJump).Methods("PUT")

	router.HandleFunc("/emulator/storages/{address}", r.Storage)

	router.HandleFunc("/emulator/config", r.Config)

	return r
}

func (m EmulatorAPIServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.router.ServeHTTP(w, r)
}

func (m EmulatorAPIServer) Config(w http.ResponseWriter, _ *http.Request) {
	type ConfigInfo struct {
		ServiceKey string `json:"service_key"`
	}

	c := ConfigInfo{
		ServiceKey: m.server.blockchain.ServiceKey().PublicKey.String(),
	}

	s, _ := json.MarshalIndent(c, "", "\t")
	_, _ = w.Write(s)
}

func (m EmulatorAPIServer) CommitBlock(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	_, err := m.adapter.Emulator().CommitBlock()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	block, _, err := m.adapter.Emulator().GetLatestBlock()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	blockResponse := &BlockResponse{
		Height:  int(block.Header.Height),
		BlockId: block.ID().String(),
	}

	err = json.NewEncoder(w).Encode(blockResponse)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

}
func (m EmulatorAPIServer) SnapshotList(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	snapshotProvider, isSnapshotProvider := m.store.(storage.SnapshotProvider)
	if !isSnapshotProvider || !snapshotProvider.SupportSnapshotsWithCurrentConfig() {
		m.server.logger.Error().Msg("State management is not available with current storage adapters")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	snapshots, err := snapshotProvider.Snapshots()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	bytes, err := json.Marshal(snapshots)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(bytes)

}

func (m EmulatorAPIServer) reloadBlockchainFromSnapshot(name string, snapshotProvider storage.Store, w http.ResponseWriter) {
	blockchain, err := configureBlockchain(m.server.logger, m.server.config, snapshotProvider)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	m.adapter.SetEmulator(blockchain)

	block, _, err := m.adapter.Emulator().GetLatestBlock()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	blockResponse := &BlockResponse{
		Height:  int(block.Header.Height),
		BlockId: block.Header.ID().String(),
		Context: name,
	}

	bytes, err := json.Marshal(blockResponse)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(bytes)

}

func (m EmulatorAPIServer) SnapshotJump(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	name := vars["name"]

	snapshotProvider, isSnapshotProvider := m.store.(storage.SnapshotProvider)
	if !isSnapshotProvider || !snapshotProvider.SupportSnapshotsWithCurrentConfig() {
		m.server.logger.Error().Msg("State management is not available with current storage adapters")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	snapshots, err := snapshotProvider.Snapshots()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if !slices.Contains(snapshots, name) {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	err = snapshotProvider.JumpToSnapshot(name, false)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	m.reloadBlockchainFromSnapshot(name, m.store, w)

}

func (m EmulatorAPIServer) SnapshotCreate(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	name := r.FormValue("name")

	if name == "" {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	snapshotProvider, isSnapshotProvider := m.store.(storage.SnapshotProvider)
	if !isSnapshotProvider || !snapshotProvider.SupportSnapshotsWithCurrentConfig() {
		m.server.logger.Error().Msg("State management is not available with current storage adapters")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	snapshots, err := snapshotProvider.Snapshots()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if slices.Contains(snapshots, name) {
		w.WriteHeader(http.StatusConflict)
		return
	}
	err = snapshotProvider.JumpToSnapshot(name, true)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	m.reloadBlockchainFromSnapshot(name, m.store, w)

}

func (m EmulatorAPIServer) Storage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	address := vars["address"]

	addr := flowgo.HexToAddress(address)

	accountStorage, err := m.adapter.Emulator().GetAccountStorage(addr)
	if err != nil {
		if fvmerrors.IsAccountNotFoundError(err) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = json.NewEncoder(w).Encode(accountStorage)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
