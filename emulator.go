package emulator

import "github.com/onflow/flow-emulator/blockchain"
import "github.com/onflow/flow-emulator/emulator"

func New(opts ...blockchain.Option) (emulator.Emulator, error) {
	return blockchain.New(opts...)
}
