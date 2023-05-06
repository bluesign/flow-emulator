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

package blockchain

import (
	"github.com/onflow/cadence/runtime"
	"github.com/onflow/cadence/runtime/interpreter"
	"github.com/onflow/flow-go/access"
	flowgo "github.com/onflow/flow-go/model/flow"

	"github.com/onflow/flow-emulator/types"
)

type CoverageReportCapable interface {
	CoverageReport() *runtime.CoverageReport
	SetCoverageReport(coverageReport *runtime.CoverageReport)
	ResetCoverageReport()
}

type DebuggingCapable interface {
	SetDebugger(*interpreter.Debugger)
	EndDebugging()
	// Deprecated: Needed for the debugger right now, do NOT use for other purposes.
	// TODO: refactor
	GetAccountUnsafe(address flowgo.Address) (*flowgo.Account, error)
}

type SnapshotCapable interface {
	Snapshots() ([]string, error)
	CreateSnapshot(name string) error
	LoadSnapshot(name string) error
}

type RollbackCapable interface {
	RollbackToBlockHeight(height uint64) error
}

type AccessProvider interface {
	Ping() error
	GetNetworkParameters() access.NetworkParameters

	GetLatestBlock() (*flowgo.Block, error)
	GetBlockByID(id flowgo.Identifier) (*flowgo.Block, error)
	GetBlockByHeight(height uint64) (*flowgo.Block, error)

	GetCollectionByID(colID flowgo.Identifier) (*flowgo.LightCollection, error)

	GetTransaction(txID flowgo.Identifier) (*flowgo.TransactionBody, error)
	GetTransactionResult(txID flowgo.Identifier) (*access.TransactionResult, error)
	GetTransactionResultByIndex(blockID flowgo.Identifier, index uint32) (*access.TransactionResult, error)
	GetTransactionsByBlockID(blockID flowgo.Identifier) ([]*flowgo.TransactionBody, error)
	GetTransactionResultsByBlockID(blockID flowgo.Identifier) ([]*access.TransactionResult, error)

	GetAccount(address flowgo.Address) (*flowgo.Account, error)
	GetAccountAtBlockHeight(address flowgo.Address, blockHeight uint64) (*flowgo.Account, error)
	GetAccountByIndex(uint) (*flowgo.Account, error)

	GetEventsByHeight(blockHeight uint64, eventType string) ([]flowgo.Event, error)
	GetEventsForBlockIDs(eventType string, blockIDs []flowgo.Identifier) ([]flowgo.BlockEvents, error)
	GetEventsForHeightRange(eventType string, startHeight, endHeight uint64) ([]flowgo.BlockEvents, error)

	ExecuteScript(script []byte, arguments [][]byte) (*types.ScriptResult, error)
	ExecuteScriptAtBlockHeight(script []byte, arguments [][]byte, blockHeight uint64) (*types.ScriptResult, error)
	ExecuteScriptAtBlockID(script []byte, arguments [][]byte, id flowgo.Identifier) (*types.ScriptResult, error)

	GetAccountStorage(address flowgo.Address) (*types.AccountStorage, error)

	SendTransaction(tx *flowgo.TransactionBody) error
	AddTransaction(tx flowgo.TransactionBody) error
}

type AutoMineCapable interface {
	EnableAutoMine()
	DisableAutoMine()
}

type ExecutionCapable interface {
	ExecuteAndCommitBlock() (*flowgo.Block, []*types.TransactionResult, error)
	ExecuteNextTransaction() (*types.TransactionResult, error)
	ExecuteBlock() ([]*types.TransactionResult, error)
	CommitBlock() (*flowgo.Block, error)
}

// Emulator defines the method set of an emulated blockchain.
type Emulator interface {
	ServiceKey() ServiceKey

	AccessProvider

	CoverageReportCapable
	DebuggingCapable
	SnapshotCapable
	RollbackCapable
	AutoMineCapable
	ExecutionCapable
}
