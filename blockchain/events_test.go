package blockchain_test

import (
	"context"
	"fmt"
	"github.com/onflow/flow-emulator/adapters"
	"github.com/onflow/flow-emulator/blockchain"
	"github.com/rs/zerolog"
	"testing"

	"github.com/onflow/cadence/runtime/common"
	"github.com/onflow/flow-go-sdk/templates"
	flowgo "github.com/onflow/flow-go/model/flow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/onflow/cadence"
	flowsdk "github.com/onflow/flow-go-sdk"
)

func TestEventEmitted(t *testing.T) {

	t.Parallel()

	t.Run("EmittedFromScript", func(t *testing.T) {

		t.Parallel()

		// Emitting events in scripts is not supported

		b, err := blockchain.New()
		require.NoError(t, err)

		script := []byte(`
			pub event MyEvent(x: Int, y: Int)

			pub fun main() {
			  emit MyEvent(x: 1, y: 2)
			}
		`)

		result, err := b.ExecuteScript(script, nil)
		assert.NoError(t, err)
		require.NoError(t, result.Error)
		require.Empty(t, result.Events)
	})

	t.Run("EmittedFromAccount", func(t *testing.T) {

		t.Parallel()

		b, err := blockchain.New(
			blockchain.WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)

		logger := zerolog.Nop()
		adapter := adapters.NewSDKAdapter(&logger, b)

		accountContracts := []templates.Contract{
			{
				Name: "Test",
				Source: `
                    pub contract Test {
						pub event MyEvent(x: Int, y: Int)

						pub fun emitMyEvent(x: Int, y: Int) {
							emit MyEvent(x: x, y: y)
						}
					}
				`,
			},
		}

		publicKey := b.ServiceKey().AccountKey()

		address, err := adapter.CreateAccount(
			context.Background(),
			[]*flowsdk.AccountKey{publicKey},
			accountContracts,
		)
		assert.NoError(t, err)

		script := []byte(fmt.Sprintf(`
			import 0x%s

			transaction {
				execute {
					Test.emitMyEvent(x: 1, y: 2)
				}
			}
		`, address.Hex()))

		tx := flowsdk.NewTransaction().
			SetScript(script).
			SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(b.ServiceKey().Address, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(b.ServiceKey().Address)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx.SignEnvelope(b.ServiceKey().Address, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		err = adapter.SendTransaction(context.Background(), *tx)
		assert.NoError(t, err)

		result, err := b.ExecuteNextTransaction()
		assert.NoError(t, err)
		assert.True(t, result.Succeeded())

		block, err := b.CommitBlock()
		require.NoError(t, err)

		addr, _ := common.BytesToAddress(address.Bytes())
		location := common.AddressLocation{
			Address: addr,
			Name:    "Test",
		}
		expectedType := location.TypeID(nil, "Test.MyEvent")

		events, err := adapter.GetEventsForHeightRange(context.Background(), string(expectedType), block.Header.Height, block.Header.Height)
		require.NoError(t, err)
		require.Len(t, events, 1)

		actualEvent := events[0].Events[0]
		decodedEvent := actualEvent.Value
		expectedID := flowsdk.Event{TransactionID: tx.ID(), EventIndex: 0}.ID()

		assert.Equal(t, string(expectedType), actualEvent.Type)
		assert.Equal(t, expectedID, actualEvent.ID())
		assert.Equal(t, cadence.NewInt(1), decodedEvent.Fields[0])
		assert.Equal(t, cadence.NewInt(2), decodedEvent.Fields[1])

		events, err = adapter.GetEventsForBlockIDs(context.Background(), string(expectedType), []flowsdk.Identifier{flowsdk.Identifier(block.Header.ID())})
		require.NoError(t, err)
		require.Len(t, events, 1)

		actualEvent = events[0].Events[0]
		decodedEvent = actualEvent.Value
		expectedID = flowsdk.Event{TransactionID: tx.ID(), EventIndex: 0}.ID()

		assert.Equal(t, string(expectedType), actualEvent.Type)
		assert.Equal(t, expectedID, actualEvent.ID())
		assert.Equal(t, cadence.NewInt(1), decodedEvent.Fields[0])
		assert.Equal(t, cadence.NewInt(2), decodedEvent.Fields[1])

	})
}
