package blockchain_test

import (
	"github.com/onflow/flow-emulator/blockchain"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRuntimeLogs(t *testing.T) {

	t.Parallel()

	b, err := blockchain.NewBlockchain()
	require.NoError(t, err)

	script := []byte(`
		pub fun main() {
			log("elephant ears")
		}
	`)

	result, err := b.ExecuteScript(script, nil)
	assert.NoError(t, err)
	assert.Equal(t, []string{`"elephant ears"`}, result.Logs)
}
