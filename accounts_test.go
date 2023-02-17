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

package emulator_test

import (
	"fmt"
	convert "github.com/onflow/flow-emulator/convert/sdk"
	"testing"

	flowsdk "github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/crypto"
	"github.com/onflow/flow-go-sdk/templates"
	"github.com/onflow/flow-go-sdk/test"
	fvmerrors "github.com/onflow/flow-go/fvm/errors"
	flowgo "github.com/onflow/flow-go/model/flow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/onflow/flow-emulator"
)

const testContract = "pub contract Test {}"

func TestGetAccount(t *testing.T) {

	t.Parallel()

	t.Run("Get account at latest block height", func(t *testing.T) {

		t.Parallel()

		b, err := emulator.NewBlockchain(
			emulator.WithSimpleAddresses(),
			emulator.WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)

		acc, err := b.GetAccount(b.ServiceKey().Address)
		assert.NoError(t, err)

		assert.Equal(t, uint64(0), acc.Keys[0].SeqNumber)
	})

	t.Run("Get account at specified block height", func(t *testing.T) {

		t.Parallel()

		b, err := emulator.NewBlockchain(
			emulator.WithSimpleAddresses(),
			emulator.WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)

		serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

		acc, err := b.GetAccount(b.ServiceKey().Address)
		assert.NoError(t, err)

		assert.Equal(t, uint64(0), acc.Keys[0].SeqNumber)
		contract := templates.Contract{
			Name:   "Test",
			Source: testContract,
		}

		tx := templates.AddAccountContract(serviceAddress, contract)

		tx.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(serviceAddress)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		flowTransaction := *convert.SDKTransactionToFlow(*tx)
		err = b.AddTransaction(flowTransaction)
		require.NoError(t, err)

		result, err := b.ExecuteNextTransaction()
		assert.NoError(t, err)
		assertTransactionSucceeded(t, result)

		bl, err := b.CommitBlock()
		assert.NoError(t, err)

		accNow, err := b.GetAccountAtBlockHeight(b.ServiceKey().Address, bl.Header.Height)
		assert.NoError(t, err)

		accPrev, err := b.GetAccountAtBlockHeight(b.ServiceKey().Address, bl.Header.Height-uint64(1))
		assert.NoError(t, err)

		assert.Equal(t, accNow.Keys[0].SeqNumber, uint64(1))
		assert.Equal(t, accPrev.Keys[0].SeqNumber, uint64(0))
	})
}

func TestCreateAccount(t *testing.T) {

	t.Parallel()

	accountKeys := test.AccountKeyGenerator()

	t.Run("Simple addresses", func(t *testing.T) {
		b, err := emulator.NewBlockchain(
			emulator.WithSimpleAddresses(),
			emulator.WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)

		accountKey := accountKeys.New()
		serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

		tx, err := templates.CreateAccount(
			[]*flowsdk.AccountKey{accountKey},
			nil,
			serviceAddress,
		)
		assert.NoError(t, err)

		tx.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(serviceAddress)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		flowTransaction := *convert.SDKTransactionToFlow(*tx)
		err = b.AddTransaction(flowTransaction)
		require.NoError(t, err)

		result, err := b.ExecuteNextTransaction()
		assert.NoError(t, err)
		assertTransactionSucceeded(t, result)

		_, err = b.CommitBlock()
		assert.NoError(t, err)

		account, err := lastCreatedAccount(b, result)
		require.NoError(t, err)

		assert.Equal(t, "0000000000000005", account.Address.Hex())
		assert.Equal(t, uint64(0), account.Balance)
		require.Len(t, account.Keys, 1)
		assert.Equal(t, accountKey.PublicKey.Encode(), account.Keys[0].PublicKey.Encode())
		assert.Empty(t, account.Contracts)
	})

	t.Run("Single public keys", func(t *testing.T) {
		b, err := emulator.NewBlockchain(
			emulator.WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)

		accountKey := accountKeys.New()
		serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

		tx, err := templates.CreateAccount(
			[]*flowsdk.AccountKey{accountKey},
			nil,
			serviceAddress,
		)
		assert.NoError(t, err)

		tx.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(serviceAddress)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		flowTransaction := *convert.SDKTransactionToFlow(*tx)
		err = b.AddTransaction(flowTransaction)
		require.NoError(t, err)

		result, err := b.ExecuteNextTransaction()
		assert.NoError(t, err)
		assertTransactionSucceeded(t, result)

		_, err = b.CommitBlock()
		assert.NoError(t, err)

		account, err := lastCreatedAccount(b, result)
		require.NoError(t, err)

		assert.Equal(t, uint64(0), account.Balance)
		require.Len(t, account.Keys, 1)
		assert.Equal(t, accountKey.PublicKey.Encode(), account.Keys[0].PublicKey.Encode())
		assert.Empty(t, account.Contracts)
	})

	t.Run("Multiple public keys", func(t *testing.T) {
		b, err := emulator.NewBlockchain(
			emulator.WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)

		accountKeyA := accountKeys.New()
		accountKeyB := accountKeys.New()
		serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

		tx, err := templates.CreateAccount(
			[]*flowsdk.AccountKey{accountKeyA, accountKeyB},
			nil,
			serviceAddress,
		)
		assert.NoError(t, err)

		tx.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(serviceAddress)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		flowTransaction := *convert.SDKTransactionToFlow(*tx)
		err = b.AddTransaction(flowTransaction)
		require.NoError(t, err)

		result, err := b.ExecuteNextTransaction()
		require.NoError(t, err)
		assertTransactionSucceeded(t, result)

		_, err = b.CommitBlock()
		assert.NoError(t, err)

		account, err := lastCreatedAccount(b, result)
		require.NoError(t, err)

		assert.Equal(t, uint64(0), account.Balance)
		require.Len(t, account.Keys, 2)
		assert.Equal(t, accountKeyA.PublicKey.Encode(), account.Keys[0].PublicKey.Encode())
		assert.Equal(t, accountKeyB.PublicKey.Encode(), account.Keys[1].PublicKey.Encode())
		assert.Empty(t, account.Contracts)
	})

	t.Run("Public keys and contract", func(t *testing.T) {
		b, err := emulator.NewBlockchain(
			emulator.WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)

		accountKeyA := accountKeys.New()
		accountKeyB := accountKeys.New()
		serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

		contracts := []templates.Contract{
			{
				Name:   "Test",
				Source: testContract,
			},
		}

		tx, err := templates.CreateAccount(
			[]*flowsdk.AccountKey{accountKeyA, accountKeyB},
			contracts,
			serviceAddress,
		)
		assert.NoError(t, err)

		tx.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(serviceAddress)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		flowTransaction := *convert.SDKTransactionToFlow(*tx)
		err = b.AddTransaction(flowTransaction)
		require.NoError(t, err)

		result, err := b.ExecuteNextTransaction()
		require.NoError(t, err)
		assertTransactionSucceeded(t, result)

		_, err = b.CommitBlock()
		assert.NoError(t, err)

		account, err := lastCreatedAccount(b, result)
		require.NoError(t, err)

		assert.Equal(t, uint64(0), account.Balance)
		require.Len(t, account.Keys, 2)
		assert.Equal(t, accountKeyA.PublicKey.Encode(), account.Keys[0].PublicKey.Encode())
		assert.Equal(t, accountKeyB.PublicKey.Encode(), account.Keys[1].PublicKey.Encode())
		assert.Equal(t,
			map[string][]byte{
				"Test": []byte(testContract),
			},
			account.Contracts,
		)
	})

	t.Run("Public keys and two contracts", func(t *testing.T) {
		b, err := emulator.NewBlockchain(
			emulator.WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)
		serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

		codeA := `
		  pub contract Test1 {
			  pub fun a(): Int {
				  return 1
			  }
		  }
		`
		codeB := `
		  pub contract Test2 {
			  pub fun b(): Int {
				  return 2
			  }
		  }
		`

		accountKey := accountKeys.New()

		contracts := []templates.Contract{
			{
				Name:   "Test1",
				Source: codeA,
			},
			{
				Name:   "Test2",
				Source: codeB,
			},
		}

		tx, err := templates.CreateAccount(
			[]*flowsdk.AccountKey{accountKey},
			contracts,
			serviceAddress,
		)
		assert.NoError(t, err)

		tx.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(serviceAddress)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		flowTransaction := *convert.SDKTransactionToFlow(*tx)
		err = b.AddTransaction(flowTransaction)
		require.NoError(t, err)

		result, err := b.ExecuteNextTransaction()
		require.NoError(t, err)
		assertTransactionSucceeded(t, result)

		_, err = b.CommitBlock()
		assert.NoError(t, err)

		account, err := lastCreatedAccount(b, result)
		require.NoError(t, err)

		assert.Equal(t, uint64(0), account.Balance)
		require.Len(t, account.Keys, 1)
		assert.Equal(t, accountKey.PublicKey.Encode(), account.Keys[0].PublicKey.Encode())
		assert.Equal(t,
			map[string][]byte{
				"Test1": []byte(codeA),
				"Test2": []byte(codeB),
			},
			account.Contracts,
		)
	})

	t.Run("Code and no keys", func(t *testing.T) {
		b, err := emulator.NewBlockchain(
			emulator.WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)
		serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

		contracts := []templates.Contract{
			{
				Name:   "Test",
				Source: testContract,
			},
		}

		tx, err := templates.CreateAccount(
			nil,
			contracts,
			serviceAddress,
		)
		assert.NoError(t, err)

		tx.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(serviceAddress)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		flowTransaction := *convert.SDKTransactionToFlow(*tx)
		err = b.AddTransaction(flowTransaction)
		assert.NoError(t, err)

		result, err := b.ExecuteNextTransaction()
		require.NoError(t, err)
		assertTransactionSucceeded(t, result)

		_, err = b.CommitBlock()
		assert.NoError(t, err)

		account, err := lastCreatedAccount(b, result)
		require.NoError(t, err)

		assert.Equal(t, uint64(0), account.Balance)
		assert.Empty(t, account.Keys)
		assert.Equal(t,
			map[string][]byte{
				"Test": []byte(testContract),
			},
			account.Contracts,
		)
	})

	t.Run("Event emitted", func(t *testing.T) {
		b, err := emulator.NewBlockchain(
			emulator.WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)

		accountKey := accountKeys.New()
		serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

		contracts := []templates.Contract{
			{
				Name:   "Test",
				Source: testContract,
			},
		}

		tx, err := templates.CreateAccount(
			[]*flowsdk.AccountKey{accountKey},
			contracts,
			serviceAddress,
		)
		assert.NoError(t, err)

		tx.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(serviceAddress)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		flowTransaction := *convert.SDKTransactionToFlow(*tx)
		err = b.AddTransaction(flowTransaction)
		assert.NoError(t, err)

		result, err := b.ExecuteNextTransaction()
		require.NoError(t, err)
		assertTransactionSucceeded(t, result)

		block, err := b.CommitBlock()
		require.NoError(t, err)

		events, err := b.GetEventsByHeight(block.Header.Height, flowsdk.EventAccountCreated)
		require.NoError(t, err)
		require.Len(t, events, 1)

		sdkEvent, err := convert.FlowEventToSDK(events[0])
		require.NoError(t, err)

		accountEvent := flowsdk.AccountCreatedEvent(sdkEvent)

		account, err := b.GetAccount(convert.SDKAddressToFlow(accountEvent.Address()))
		assert.NoError(t, err)

		assert.Equal(t, uint64(0), account.Balance)
		require.Len(t, account.Keys, 1)
		assert.Equal(t, accountKey.PublicKey, account.Keys[0].PublicKey)
		assert.Equal(t,
			map[string][]byte{
				"Test": []byte(testContract),
			},
			account.Contracts,
		)
	})

	t.Run("Invalid hash algorithm", func(t *testing.T) {
		b, err := emulator.NewBlockchain(
			emulator.WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)
		serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

		accountKey := accountKeys.New()
		accountKey.SetHashAlgo(crypto.SHA3_384) // SHA3_384 is invalid for ECDSA_P256

		tx, err := templates.CreateAccount(
			[]*flowsdk.AccountKey{accountKey},
			nil,
			serviceAddress,
		)
		assert.NoError(t, err)

		tx.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(serviceAddress)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		flowTransaction := *convert.SDKTransactionToFlow(*tx)
		err = b.AddTransaction(flowTransaction)
		assert.NoError(t, err)

		result, err := b.ExecuteNextTransaction()
		require.NoError(t, err)

		assert.True(t, result.Reverted())
	})

	t.Run("Invalid code", func(t *testing.T) {
		b, err := emulator.NewBlockchain(
			emulator.WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)

		serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

		contracts := []templates.Contract{
			{
				Name:   "Test",
				Source: "not a valid script",
			},
		}

		tx, err := templates.CreateAccount(
			nil,
			contracts,
			serviceAddress,
		)
		assert.NoError(t, err)

		tx.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(serviceAddress)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		flowTransaction := *convert.SDKTransactionToFlow(*tx)
		err = b.AddTransaction(flowTransaction)
		assert.NoError(t, err)

		result, err := b.ExecuteNextTransaction()
		require.NoError(t, err)

		assert.True(t, result.Reverted())
	})

	t.Run("Invalid contract name", func(t *testing.T) {
		b, err := emulator.NewBlockchain(
			emulator.WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)
		serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

		contracts := []templates.Contract{
			{
				Name:   "Test2",
				Source: testContract,
			},
		}

		tx, err := templates.CreateAccount(
			nil,
			contracts,
			serviceAddress,
		)
		assert.NoError(t, err)

		tx.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(serviceAddress)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		flowTransaction := *convert.SDKTransactionToFlow(*tx)
		err = b.AddTransaction(flowTransaction)
		assert.NoError(t, err)

		result, err := b.ExecuteNextTransaction()
		require.NoError(t, err)

		assert.True(t, result.Reverted())
	})
}

func TestAddAccountKey(t *testing.T) {

	t.Parallel()

	accountKeys := test.AccountKeyGenerator()

	t.Run("Valid key", func(t *testing.T) {
		b, err := emulator.NewBlockchain(
			emulator.WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)

		newAccountKey, newSigner := accountKeys.NewWithSigner()
		serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

		tx1, err := templates.AddAccountKey(serviceAddress, newAccountKey)
		assert.NoError(t, err)

		tx1.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(serviceAddress)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx1.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		flowTransaction := *convert.SDKTransactionToFlow(*tx1)
		err = b.AddTransaction(flowTransaction)
		assert.NoError(t, err)

		result, err := b.ExecuteNextTransaction()
		assert.NoError(t, err)
		assertTransactionSucceeded(t, result)

		_, err = b.CommitBlock()
		assert.NoError(t, err)

		script := []byte("transaction { execute {} }")

		var newKeyID = 1 // new key with have ID 1
		var newKeySequenceNum uint64 = 0

		tx2 := flowsdk.NewTransaction().
			SetScript(script).
			SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, newKeyID, newKeySequenceNum).
			SetPayer(serviceAddress)

		err = tx2.SignEnvelope(serviceAddress, newKeyID, newSigner)
		assert.NoError(t, err)

		flowTransaction2 := *convert.SDKTransactionToFlow(*tx2)
		err = b.AddTransaction(flowTransaction2)
		require.NoError(t, err)

		result, err = b.ExecuteNextTransaction()
		require.NoError(t, err)
		assertTransactionSucceeded(t, result)

		_, err = b.CommitBlock()
		assert.NoError(t, err)
	})

	t.Run("Invalid hash algorithm", func(t *testing.T) {
		b, err := emulator.NewBlockchain(
			emulator.WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)

		accountKey := accountKeys.New()
		accountKey.SetHashAlgo(crypto.SHA3_384) // SHA3_384 is invalid for ECDSA_P256
		serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

		tx, err := templates.AddAccountKey(serviceAddress, accountKey)
		assert.NoError(t, err)

		tx.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(serviceAddress)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		flowTransaction := *convert.SDKTransactionToFlow(*tx)
		err = b.AddTransaction(flowTransaction)
		assert.NoError(t, err)

		result, err := b.ExecuteNextTransaction()
		assert.NoError(t, err)
		assert.True(t, result.Reverted())
	})
}

func TestRemoveAccountKey(t *testing.T) {

	t.Parallel()

	b, err := emulator.NewBlockchain(
		emulator.WithStorageLimitEnabled(false),
	)
	require.NoError(t, err)

	accountKeys := test.AccountKeyGenerator()
	serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

	newAccountKey, newSigner := accountKeys.NewWithSigner()

	// create transaction that adds public key to account keys
	tx1, err := templates.AddAccountKey(serviceAddress, newAccountKey)
	assert.NoError(t, err)

	// create transaction that adds public key to account keys
	tx1.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
		SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
		SetPayer(serviceAddress)

	// sign with service key
	signer, err := b.ServiceKey().Signer()
	require.NoError(t, err)

	err = tx1.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
	require.NoError(t, err)

	// submit tx1 (should succeed)
	flowTransaction := *convert.SDKTransactionToFlow(*tx1)
	err = b.AddTransaction(flowTransaction)
	assert.NoError(t, err)

	result, err := b.ExecuteNextTransaction()
	assert.NoError(t, err)
	assertTransactionSucceeded(t, result)

	_, err = b.CommitBlock()
	assert.NoError(t, err)

	account, err := b.GetAccount(b.ServiceKey().Address)
	assert.NoError(t, err)

	require.Len(t, account.Keys, 2)
	assert.False(t, account.Keys[0].Revoked)
	assert.False(t, account.Keys[1].Revoked)

	// create transaction that removes service key
	tx2 := templates.RemoveAccountKey(serviceAddress, 0)

	tx2.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
		SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
		SetPayer(serviceAddress)

	// sign with service key
	signer, err = b.ServiceKey().Signer()
	assert.NoError(t, err)
	err = tx2.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
	assert.NoError(t, err)

	// submit tx2 (should succeed)
	flowTransaction2 := *convert.SDKTransactionToFlow(*tx2)
	err = b.AddTransaction(flowTransaction2)
	assert.NoError(t, err)

	result, err = b.ExecuteNextTransaction()
	assert.NoError(t, err)
	assertTransactionSucceeded(t, result)

	_, err = b.CommitBlock()
	assert.NoError(t, err)

	account, err = b.GetAccount(b.ServiceKey().Address)
	assert.NoError(t, err)

	// key at index 0 should be revoked
	require.Len(t, account.Keys, 2)
	assert.True(t, account.Keys[0].Revoked)
	assert.False(t, account.Keys[1].Revoked)

	// create transaction that removes remaining account key
	tx3 := templates.RemoveAccountKey(serviceAddress, 0)

	tx3.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
		SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
		SetPayer(serviceAddress)

	// sign with service key (that has been removed)
	signer, err = b.ServiceKey().Signer()
	assert.NoError(t, err)
	err = tx3.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
	assert.NoError(t, err)

	// submit tx3 (should fail)
	flowTransaction3 := *convert.SDKTransactionToFlow(*tx3)
	err = b.AddTransaction(flowTransaction3)
	assert.NoError(t, err)

	result, err = b.ExecuteNextTransaction()
	assert.NoError(t, err)

	var sigErr fvmerrors.CodedError
	assert.ErrorAs(t, result.Error, &sigErr)
	assert.True(t, fvmerrors.HasErrorCode(result.Error, fvmerrors.ErrCodeInvalidProposalSignatureError))

	_, err = b.CommitBlock()
	assert.NoError(t, err)

	account, err = b.GetAccount(b.ServiceKey().Address)
	assert.NoError(t, err)

	// key at index 1 should not be revoked
	require.Len(t, account.Keys, 2)
	assert.True(t, account.Keys[0].Revoked)
	assert.False(t, account.Keys[1].Revoked)

	// create transaction that removes remaining account key
	tx4 := templates.RemoveAccountKey(serviceAddress, 1)

	tx4.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
		SetProposalKey(serviceAddress, account.Keys[1].Index, account.Keys[1].SeqNumber).
		SetPayer(serviceAddress)

	// sign with remaining account key
	err = tx4.SignEnvelope(serviceAddress, account.Keys[1].Index, newSigner)
	assert.NoError(t, err)

	// submit tx4 (should succeed)
	flowTransaction4 := *convert.SDKTransactionToFlow(*tx4)
	err = b.AddTransaction(flowTransaction4)
	assert.NoError(t, err)

	result, err = b.ExecuteNextTransaction()
	assert.NoError(t, err)
	assertTransactionSucceeded(t, result)

	_, err = b.CommitBlock()
	assert.NoError(t, err)

	account, err = b.GetAccount(b.ServiceKey().Address)
	assert.NoError(t, err)

	// all keys should be revoked
	for _, key := range account.Keys {
		assert.True(t, key.Revoked)
	}
}

func TestUpdateAccountCode(t *testing.T) {

	t.Parallel()

	const codeA = `
      pub contract Test {
          pub fun a(): Int {
              return 1
          }
      }
    `

	const codeB = `
      pub contract Test {
          pub fun b(): Int {
              return 2
          }
      }
    `

	accountKeys := test.AccountKeyGenerator()

	accountKeyB, signerB := accountKeys.NewWithSigner()

	t.Run("Valid signature", func(t *testing.T) {
		b, err := emulator.NewBlockchain(
			emulator.WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)
		serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

		contracts := []templates.Contract{
			{
				Name:   "Test",
				Source: codeA,
			},
		}

		accountAddressB, err := b.CreateAccount(
			[]*flowsdk.AccountKey{accountKeyB},
			contracts,
		)
		require.NoError(t, err)

		account, err := b.GetAccount(convert.SDKAddressToFlow(accountAddressB))
		require.NoError(t, err)

		assert.Equal(t,
			map[string][]byte{
				"Test": []byte(codeA),
			},
			account.Contracts,
		)

		tx := templates.UpdateAccountContract(
			accountAddressB,
			templates.Contract{
				Name:   "Test",
				Source: codeB,
			},
		)

		tx.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(serviceAddress)

		err = tx.SignPayload(accountAddressB, 0, signerB)
		assert.NoError(t, err)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		flowTransaction := *convert.SDKTransactionToFlow(*tx)
		err = b.AddTransaction(flowTransaction)
		require.NoError(t, err)

		result, err := b.ExecuteNextTransaction()
		require.NoError(t, err)
		assertTransactionSucceeded(t, result)

		_, err = b.CommitBlock()
		assert.NoError(t, err)

		account, err = b.GetAccount(convert.SDKAddressToFlow(accountAddressB))
		assert.NoError(t, err)

		assert.Equal(t, codeB, string(account.Contracts["Test"]))
	})

	t.Run("Invalid signature", func(t *testing.T) {
		b, err := emulator.NewBlockchain(
			emulator.WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)
		serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

		contracts := []templates.Contract{
			{
				Name:   "Test",
				Source: codeA,
			},
		}

		accountAddressB, err := b.CreateAccount(
			[]*flowsdk.AccountKey{accountKeyB},
			contracts,
		)
		require.NoError(t, err)

		account, err := b.GetAccount(convert.SDKAddressToFlow(accountAddressB))
		require.NoError(t, err)

		assert.Equal(t, codeA, string(account.Contracts["Test"]))

		tx := templates.UpdateAccountContract(
			accountAddressB,
			templates.Contract{
				Name:   "Test",
				Source: codeB,
			},
		)

		tx.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(serviceAddress)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		flowTransaction := *convert.SDKTransactionToFlow(*tx)
		err = b.AddTransaction(flowTransaction)
		assert.NoError(t, err)

		result, err := b.ExecuteNextTransaction()
		assert.NoError(t, err)

		assert.True(t, fvmerrors.HasErrorCode(result.Error, fvmerrors.ErrCodeAccountAuthorizationError))

		_, err = b.CommitBlock()
		assert.NoError(t, err)

		account, err = b.GetAccount(convert.SDKAddressToFlow(accountAddressB))
		assert.NoError(t, err)

		// code should not be updated
		assert.Equal(t, codeA, string(account.Contracts["Test"]))
	})
}

func TestImportAccountCode(t *testing.T) {

	t.Parallel()

	b, err := emulator.NewBlockchain(
		emulator.WithStorageLimitEnabled(false),
	)
	require.NoError(t, err)
	serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

	accountContracts := []templates.Contract{
		{
			Name: "Computer",
			Source: `
              pub contract Computer {
                  pub fun answer(): Int {
                      return 42
                  }
              }
	        `,
		},
	}

	address, err := b.CreateAccount(nil, accountContracts)
	assert.NoError(t, err)

	script := []byte(fmt.Sprintf(`
		// address imports can omit leading zeros
		import 0x%s

		transaction {
		  execute {
			let answer = Computer.answer()
			if answer != 42 {
				panic("?!")
			}
		  }
		}
	`, address))

	tx := flowsdk.NewTransaction().
		SetScript(script).
		SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
		SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
		SetPayer(serviceAddress)

	signer, err := b.ServiceKey().Signer()
	require.NoError(t, err)

	err = tx.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
	require.NoError(t, err)

	flowTransaction := *convert.SDKTransactionToFlow(*tx)
	err = b.AddTransaction(flowTransaction)
	assert.NoError(t, err)

	result, err := b.ExecuteNextTransaction()
	assert.NoError(t, err)
	assertTransactionSucceeded(t, result)
}

func TestAccountAccess(t *testing.T) {

	t.Parallel()

	b, err := emulator.NewBlockchain(
		emulator.WithStorageLimitEnabled(false),
	)
	require.NoError(t, err)
	serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

	// Create first account and deploy a contract A
	// which has a field
	// which only other code in the same should be allowed to access

	accountContracts := []templates.Contract{
		{
			Name: "A",
			Source: `
				pub contract A {
					access(account) let a: Int

					init() {
						self.a = 1
					}
				}
			`,
		},
	}

	accountKeys := test.AccountKeyGenerator()

	accountKey1, signer1 := accountKeys.NewWithSigner()

	address1, err := b.CreateAccount(
		[]*flowsdk.AccountKey{accountKey1},
		accountContracts,
	)
	assert.NoError(t, err)

	// Deploy another contract B to the same account
	// which accesses the field in contract A
	// which allows access to code in the same account

	tx := templates.AddAccountContract(
		address1,
		templates.Contract{
			Name: "B",
			Source: fmt.Sprintf(`
				    import A from 0x%s

					pub contract B {
						pub fun use() {
							let b = A.a
						}
					}
				`,
				address1.Hex(),
			),
		},
	)

	tx.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
		SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
		SetPayer(serviceAddress)

	err = tx.SignPayload(address1, 0, signer1)
	assert.NoError(t, err)

	signer, err := b.ServiceKey().Signer()
	require.NoError(t, err)

	err = tx.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
	require.NoError(t, err)

	flowTransaction := *convert.SDKTransactionToFlow(*tx)
	err = b.AddTransaction(flowTransaction)
	require.NoError(t, err)

	result, err := b.ExecuteNextTransaction()
	require.NoError(t, err)
	assertTransactionSucceeded(t, result)

	_, err = b.CommitBlock()
	require.NoError(t, err)

	// Create another account 2

	accountKey2, signer2 := accountKeys.NewWithSigner()

	address2, err := b.CreateAccount(
		[]*flowsdk.AccountKey{accountKey2},
		nil,
	)
	assert.NoError(t, err)

	// Deploy a contract C to the second account
	// which accesses the field in contract A of the first account
	// which allows access to code in the same account

	tx2 := templates.AddAccountContract(
		address2,
		templates.Contract{
			Name: "C",
			Source: fmt.Sprintf(`
				    import A from 0x%s

					pub contract C {
						pub fun use() {
							let b = A.a
						}
					}
				`,
				address1.Hex(),
			),
		},
	)

	tx2.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
		SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
		SetPayer(serviceAddress)

	err = tx2.SignPayload(address2, 0, signer2)
	require.NoError(t, err)

	signer, err = b.ServiceKey().Signer()
	assert.NoError(t, err)
	err = tx2.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
	require.NoError(t, err)

	flowTransaction2 := *convert.SDKTransactionToFlow(*tx2)
	err = b.AddTransaction(flowTransaction2)
	require.NoError(t, err)

	result, err = b.ExecuteNextTransaction()
	require.NoError(t, err)

	require.False(t, result.Succeeded())
	require.Error(t, result.Error)

	require.Contains(t, result.Error.Error(), "error: cannot access `a`: field has account access")
}
