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
	flowgo "github.com/onflow/flow-go/model/flow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/onflow/flow-emulator"
)

func TestCommitBlock(t *testing.T) {

	t.Parallel()

	b, err := emulator.NewBlockchain(
		emulator.WithStorageLimitEnabled(false),
	)
	require.NoError(t, err)
	serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

	addTwoScript, _ := deployAndGenerateAddTwoScript(t, b)

	tx1 := flowsdk.NewTransaction().
		SetScript([]byte(addTwoScript)).
		SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
		SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
		SetPayer(serviceAddress).
		AddAuthorizer(serviceAddress)

	signer, err := b.ServiceKey().Signer()
	require.NoError(t, err)

	err = tx1.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
	require.NoError(t, err)

	// Add tx1 to pending block
	flowTransaction := *convert.SDKTransactionToFlow(*tx1)
	err = b.AddTransaction(flowTransaction)
	assert.NoError(t, err)

	tx1Result, err := b.GetTransactionResult(convert.SDKIdentifierToFlow(tx1.ID()))
	assert.NoError(t, err)
	assert.Equal(t, flowgo.TransactionStatusPending, tx1Result.Status)

	tx2 := flowsdk.NewTransaction().
		SetScript([]byte(`transaction { execute { panic("revert!") } }`)).
		SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
		SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
		SetPayer(serviceAddress).
		AddAuthorizer(serviceAddress)

	signer, err = b.ServiceKey().Signer()
	require.NoError(t, err)

	err = tx2.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
	require.NoError(t, err)

	// Add tx2 to pending block
	flowTransaction2 := *convert.SDKTransactionToFlow(*tx2)
	err = b.AddTransaction(flowTransaction2)
	require.NoError(t, err)

	tx2Result, err := b.GetTransactionResult(convert.SDKIdentifierToFlow(tx2.ID()))
	assert.NoError(t, err)
	assert.Equal(t, flowgo.TransactionStatusPending, tx2Result.Status)

	// Execute tx1
	result, err := b.ExecuteNextTransaction()
	assert.NoError(t, err)
	assert.True(t, result.Succeeded())

	// Execute tx2
	result, err = b.ExecuteNextTransaction()
	assert.NoError(t, err)
	assert.True(t, result.Reverted())

	// Commit tx1 and tx2 into new block
	_, err = b.CommitBlock()
	assert.NoError(t, err)

	// tx1 status becomes TransactionStatusSealed
	tx1Result, err = b.GetTransactionResult(convert.SDKIdentifierToFlow(tx1.ID()))
	require.NoError(t, err)
	assert.Equal(t, flowgo.TransactionStatusSealed, tx1Result.Status)

	// tx2 status also becomes TransactionStatusSealed, even though it is reverted
	tx2Result, err = b.GetTransactionResult(convert.SDKIdentifierToFlow(tx2.ID()))
	require.NoError(t, err)
	assert.Equal(t, flowgo.TransactionStatusSealed, tx2Result.Status)
	assert.NotEmpty(t, tx2Result.ErrorMessage)
}

func TestBlockView(t *testing.T) {

	t.Parallel()

	const nBlocks = 3

	b, err := emulator.NewBlockchain()
	require.NoError(t, err)
	serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

	t.Run("genesis should have 0 view", func(t *testing.T) {
		block, _, err := b.GetBlockByHeight(0)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), block.Header.Height)
		assert.Equal(t, uint64(0), block.Header.View)
	})

	addTwoScript, _ := deployAndGenerateAddTwoScript(t, b)

	// create a few blocks, each with one transaction
	for i := 0; i < nBlocks; i++ {

		tx := flowsdk.NewTransaction().
			SetScript([]byte(addTwoScript)).
			SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(serviceAddress).
			AddAuthorizer(serviceAddress)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		// Add tx to pending block
		flowTransaction := *convert.SDKTransactionToFlow(*tx)
		err = b.AddTransaction(flowTransaction)
		assert.NoError(t, err)

		// execute and commit the block
		_, _, err = b.ExecuteAndCommitBlock()
		require.NoError(t, err)
	}

	for height := uint64(1); height <= nBlocks+1; height++ {
		block, _, err := b.GetBlockByHeight(height)
		require.NoError(t, err)

		maxView := height * emulator.MaxViewIncrease
		t.Run(fmt.Sprintf("block %d should have view <%d", height, maxView), func(t *testing.T) {
			assert.Equal(t, height, block.Header.Height)
			assert.LessOrEqual(t, block.Header.View, maxView)
		})
	}
}
