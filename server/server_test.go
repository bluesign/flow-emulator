package server

import (
	"fmt"
	"sync"
	"testing"

	"github.com/hashicorp/go-multierror"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestExecuteScript(t *testing.T) {
	logger := zerolog.Nop()
	server := NewEmulatorServer(&logger, &Config{})
	require.NotNil(t, server)

	const code = `
      pub fun main(): String {
	      return "Hello"
      }
    `
	result, err := server.blockchain.ExecuteScript([]byte(code), nil)
	require.NoError(t, err)

	require.JSONEq(t, `{"type":"String","value":"Hello"}`, result.Value.String())

}

func TestGetStorage(t *testing.T) {
	logger := zerolog.Nop()
	server := NewEmulatorServer(&logger, &Config{})
	require.NotNil(t, server)
	address := server.blockchain.ServiceKey().Address

	var result *multierror.Error
	errchan := make(chan error, 10)
	wg := sync.WaitGroup{}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			_, err := server.blockchain.GetAccountStorage(address)
			errchan <- err
			wg.Done()
		}()
	}

	wg.Wait()
	close(errchan)

	for err := range errchan {
		result = multierror.Append(result, err)
	}

	require.NoError(t, result.ErrorOrNil())
}

func TestExecuteScriptImportingContracts(t *testing.T) {
	conf := &Config{
		WithContracts: true,
	}

	logger := zerolog.Nop()
	server := NewEmulatorServer(&logger, conf)
	require.NotNil(t, server)
	serviceAccount := server.blockchain.ServiceKey().Address.Hex()

	code := fmt.Sprintf(
		`
	      import ExampleNFT, NFTStorefront from 0x%s

          pub fun main() {
		      let collection <- ExampleNFT.createEmptyCollection()
		      destroy collection

		      NFTStorefront
		  }
        `,
		serviceAccount,
	)

	_, err := server.blockchain.ExecuteScript([]byte(code), nil)
	require.NoError(t, err)

}

func TestCustomChainID(t *testing.T) {

	conf := &Config{
		WithContracts: true,
		ChainID:       "flow-mainnet",
	}
	logger := zerolog.Nop()
	server := NewEmulatorServer(&logger, conf)

	serviceAccount := server.blockchain.ServiceKey().Address.Hex()

	require.Equal(t, serviceAccount, "e467b9dd11fa00df")
}
