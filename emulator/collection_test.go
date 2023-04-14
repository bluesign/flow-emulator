package emulator

import (
	convert "github.com/onflow/flow-emulator/utils/convert/sdk"
	"testing"

	flowsdk "github.com/onflow/flow-go-sdk"
	flowgo "github.com/onflow/flow-go/model/flow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollections(t *testing.T) {

	t.Parallel()

	t.Run("Empty block", func(t *testing.T) {

		t.Parallel()

		b, err := NewBlockchain()
		require.NoError(t, err)

		block, err := b.CommitBlock()
		require.NoError(t, err)

		// block should not contain any collections
		assert.Empty(t, block.Payload.Guarantees)
	})

	t.Run("Non-empty block", func(t *testing.T) {

		t.Parallel()

		b, err := NewBlockchain(
			WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)

		addTwoScript, _ := deployAndGenerateAddTwoScript(t, b)

		tx1 := flowsdk.NewTransaction().
			SetScript([]byte(addTwoScript)).
			SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(b.ServiceKey().Address, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(b.ServiceKey().Address).
			AddAuthorizer(b.ServiceKey().Address)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx1.SignEnvelope(b.ServiceKey().Address, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		tx2 := flowsdk.NewTransaction().
			SetScript([]byte(addTwoScript)).
			SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(b.ServiceKey().Address, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(b.ServiceKey().Address).
			AddAuthorizer(b.ServiceKey().Address)

		err = tx2.SignEnvelope(b.ServiceKey().Address, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		// generate a list of transactions
		transactions := []*flowsdk.Transaction{tx1, tx2}

		// add all transactions to block
		for _, tx := range transactions {
			err = b.AddTransaction(*tx)
			require.NoError(t, err)
		}

		block, _, err := b.ExecuteAndCommitBlock()
		require.NoError(t, err)

		// block should contain at least one collection
		assert.NotEmpty(t, block.Payload.Guarantees)

		i := 0
		for _, guarantee := range block.Payload.Guarantees {
			collection, err := b.GetCollection(convert.FlowIdentifierToSDK(guarantee.ID()))
			require.NoError(t, err)

			for _, txID := range collection.TransactionIDs {
				assert.Equal(t, transactions[i].ID(), txID)
				i++
			}
		}
	})
}
