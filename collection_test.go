package emulator_test

import (
	"testing"

	flowsdk "github.com/onflow/flow-go-sdk"
	flowgo "github.com/onflow/flow-go/model/flow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/onflow/flow-emulator"
	convert "github.com/onflow/flow-emulator/convert/sdk"
)

func TestCollections(t *testing.T) {

	t.Parallel()

	t.Run("Empty block", func(t *testing.T) {

		t.Parallel()

		b, err := emulator.NewBlockchain()
		require.NoError(t, err)

		block, err := b.CommitBlock()
		require.NoError(t, err)

		// block should not contain any collections
		assert.Empty(t, block.Payload.Guarantees)
	})

	t.Run("Non-empty block", func(t *testing.T) {

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

		tx2 := flowsdk.NewTransaction().
			SetScript([]byte(addTwoScript)).
			SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(serviceAddress).
			AddAuthorizer(serviceAddress)

		err = tx2.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		// generate a list of transactions
		transactions := []*flowsdk.Transaction{tx1, tx2}

		// add all transactions to block
		for _, tx := range transactions {
			flowTransaction := *convert.SDKTransactionToFlow(*tx)
			err = b.AddTransaction(flowTransaction)
			require.NoError(t, err)
		}

		block, _, err := b.ExecuteAndCommitBlock()
		require.NoError(t, err)

		// block should contain at least one collection
		assert.NotEmpty(t, block.Payload.Guarantees)

		i := 0
		for _, guarantee := range block.Payload.Guarantees {
			collection, err := b.GetCollectionByID(guarantee.ID())
			require.NoError(t, err)

			for _, txID := range collection.Transactions {
				assert.Equal(t, transactions[i].ID().String(), txID.String())
				i++
			}
		}
	})
}
