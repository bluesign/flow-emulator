package emulator_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/onflow/cadence"
	flowsdk "github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/templates"
	flowgo "github.com/onflow/flow-go/model/flow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/onflow/flow-emulator"
	convert "github.com/onflow/flow-emulator/convert/sdk"
	"github.com/onflow/flow-emulator/storage/sqlite"
)

func TestInitialization(t *testing.T) {

	t.Parallel()

	t.Run("should inject initial state when initialized with empty store", func(t *testing.T) {

		t.Parallel()

		file, err := os.CreateTemp("", "sqlite-test")
		require.Nil(t, err)
		store, err := sqlite.New(file.Name())
		require.Nil(t, err)
		defer store.Close()

		b, _ := emulator.NewBlockchain(emulator.WithStore(store))

		serviceAcct, err := b.GetAccount(convert.SDKAddressToFlow(flowsdk.ServiceAddress(flowsdk.Emulator)))
		require.NoError(t, err)

		assert.NotNil(t, serviceAcct)

		latestBlock, _, err := b.GetLatestBlock()
		require.NoError(t, err)

		assert.EqualValues(t, 0, latestBlock.Header.Height)
		assert.Equal(t,
			flowgo.Genesis(flowgo.Emulator).ID(),
			latestBlock.ID(),
		)
	})

	t.Run("should restore state when initialized with non-empty store", func(t *testing.T) {

		t.Parallel()

		file, err := os.CreateTemp("", "sqlite-test")
		require.Nil(t, err)
		store, err := sqlite.New(file.Name())
		require.Nil(t, err)
		defer store.Close()

		b, _ := emulator.NewBlockchain(emulator.WithStore(store), emulator.WithStorageLimitEnabled(false))
		serviceAddress := convert.FlowAddressToSDK(b.ServiceKey().Address)

		contracts := []templates.Contract{
			{
				Name:   "Counting",
				Source: counterScript,
			},
		}

		counterAddress, err := b.CreateAccount(nil, contracts)
		require.NoError(t, err)

		// Submit a transaction adds some ledger state and event state
		script := fmt.Sprintf(
			`
                import 0x%s

                transaction {

                  prepare(acct: AuthAccount) {

                    let counter <- Counting.createCounter()
                    counter.add(1)

                    acct.save(<-counter, to: /storage/counter)

                    acct.link<&Counting.Counter>(/public/counter, target: /storage/counter)
                  }
                }
            `,
			counterAddress,
		)

		tx := flowsdk.NewTransaction().
			SetScript([]byte(script)).
			SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
			SetProposalKey(serviceAddress, b.ServiceKey().Index, b.ServiceKey().SequenceNumber).
			SetPayer(serviceAddress).
			AddAuthorizer(serviceAddress)

		signer, err := b.ServiceKey().Signer()
		require.NoError(t, err)

		err = tx.SignEnvelope(serviceAddress, b.ServiceKey().Index, signer)
		require.NoError(t, err)

		flowTransaction := *convert.SDKTransactionToFlow(*tx)
		err = b.AddTransaction(flowTransaction)
		require.NoError(t, err)

		result, err := b.ExecuteNextTransaction()
		assert.NoError(t, err)
		require.True(t, result.Succeeded())

		block, err := b.CommitBlock()
		assert.NoError(t, err)
		require.NotNil(t, block)

		minedTx, err := b.GetTransaction(convert.SDKIdentifierToFlow(tx.ID()))
		require.NoError(t, err)

		minedEvents, err := b.GetEventsByHeight(block.Header.Height, "")
		require.NoError(t, err)

		// Create a new blockchain with the same store
		b, _ = emulator.NewBlockchain(emulator.WithStore(store))

		t.Run("should be able to read blocks", func(t *testing.T) {
			latestBlock, _, err := b.GetLatestBlock()
			require.NoError(t, err)

			assert.Equal(t, block.ID(), latestBlock.ID())

			blockByHeight, _, err := b.GetBlockByHeight(block.Header.Height)
			require.NoError(t, err)

			assert.Equal(t, block.ID(), blockByHeight.ID())

			blockByHash, _, err := b.GetBlockByID(block.ID())
			require.NoError(t, err)

			assert.Equal(t, block.ID(), blockByHash.ID())
		})

		t.Run("should be able to read transactions", func(t *testing.T) {
			txByHash, err := b.GetTransaction(convert.SDKIdentifierToFlow(tx.ID()))
			require.NoError(t, err)

			assert.Equal(t, minedTx, txByHash)
		})

		t.Run("should be able to read events", func(t *testing.T) {
			gotEvents, err := b.GetEventsByHeight(block.Header.Height, "")
			require.NoError(t, err)

			assert.Equal(t, minedEvents, gotEvents)
		})

		t.Run("should be able to read ledger state", func(t *testing.T) {
			readScript := fmt.Sprintf(
				`
                  import 0x%s

                  pub fun main(): Int {
                      return getAccount(0x%s).getCapability(/public/counter)!.borrow<&Counting.Counter>()?.count ?? 0
                  }
                `,
				counterAddress,
				b.ServiceKey().Address,
			)

			result, err := b.ExecuteScript([]byte(readScript), nil)
			require.NoError(t, err)

			assert.Equal(t, cadence.NewInt(1), result.Value)
		})
	})
}
