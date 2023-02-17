package emulator_test

import (
	"fmt"
	jsoncdc "github.com/onflow/cadence/encoding/json"
	convert "github.com/onflow/flow-emulator/convert/sdk"
	"testing"

	"github.com/onflow/cadence/runtime/common"
	"github.com/onflow/flow-go-sdk/templates"
	flowgo "github.com/onflow/flow-go/model/flow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/onflow/cadence"
	flowsdk "github.com/onflow/flow-go-sdk"

	emulator "github.com/onflow/flow-emulator"
)

func TestEventEmitted(t *testing.T) {

	t.Parallel()

	t.Run("EmittedFromScript", func(t *testing.T) {

		t.Parallel()

		// Emitting events in scripts is not supported

		b, err := emulator.NewBlockchain()
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

		b, err := emulator.NewBlockchain(
			emulator.WithStorageLimitEnabled(false),
		)
		require.NoError(t, err)
		serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

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

		address, err := b.CreateAccount(
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
		assert.True(t, result.Succeeded())

		block, err := b.CommitBlock()
		require.NoError(t, err)

		addr, _ := common.BytesToAddress(address.Bytes())
		location := common.AddressLocation{
			Address: addr,
			Name:    "Test",
		}
		expectedType := location.TypeID(nil, "Test.MyEvent")

		events, err := b.GetEventsByHeight(block.Header.Height, string(expectedType))
		require.NoError(t, err)
		require.Len(t, events, 1)

		actualEvent := events[0]

		decodedEventValue, err := jsoncdc.Decode(nil, actualEvent.Payload)
		require.NoError(t, err)

		decodedEvent := decodedEventValue.(cadence.Event)

		expectedID := flowgo.Event{TransactionID: convert.SDKIdentifierToFlow(tx.ID()), EventIndex: 0}.ID()

		assert.Equal(t, string(expectedType), string(actualEvent.Type))
		assert.Equal(t, expectedID, actualEvent.ID())
		assert.Equal(t, cadence.NewInt(1), decodedEvent.Fields[0])
		assert.Equal(t, cadence.NewInt(2), decodedEvent.Fields[1])
	})
}
