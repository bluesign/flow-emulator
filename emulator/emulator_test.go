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

package tests_test

import (
	"fmt"
	"github.com/onflow/flow-emulator/emulator"
	"testing"

	"github.com/onflow/cadence"
	flowsdk "github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/templates"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/onflow/flow-emulator/types"
)

const counterScript = `

  pub contract Counting {

      pub event CountIncremented(count: Int)

      pub resource Counter {
          pub var count: Int

          init() {
              self.count = 0
          }

          pub fun add(_ count: Int) {
              self.count = self.count + count
              emit CountIncremented(count: self.count)
          }
      }

      pub fun createCounter(): @Counter {
          return <-create Counter()
      }
  }
`

// generateAddTwoToCounterScript generates a script that increments a counter.
// If no counter exists, it is created.
func generateAddTwoToCounterScript(counterAddress flowsdk.Address) string {
	return fmt.Sprintf(
		`
            import 0x%s

            transaction {
                prepare(signer: AuthAccount) {
                    var counter = signer.borrow<&Counting.Counter>(from: /storage/counter)
                    if counter == nil {
                        signer.save(<-Counting.createCounter(), to: /storage/counter)
                        signer.link<&Counting.Counter>(/public/counter, target: /storage/counter)
                        counter = signer.borrow<&Counting.Counter>(from: /storage/counter)
                    }
                    counter?.add(2)
                }
            }
        `,
		counterAddress,
	)
}

func deployAndGenerateAddTwoScript(t *testing.T, b *emulator.Blockchain) (string, flowsdk.Address) {
	contracts := []templates.Contract{
		{
			Name:   "Counting",
			Source: counterScript,
		},
	}

	counterAddress, err := b.CreateAccount(
		nil,
		contracts,
	)
	require.NoError(t, err)

	return generateAddTwoToCounterScript(counterAddress), counterAddress
}

func generateGetCounterCountScript(counterAddress flowsdk.Address, accountAddress flowsdk.Address) string {
	return fmt.Sprintf(
		`
            import 0x%s

            pub fun main(): Int {
                return getAccount(0x%s).getCapability(/public/counter)!.borrow<&Counting.Counter>()?.count ?? 0
            }
        `,
		counterAddress,
		accountAddress,
	)
}

func assertTransactionSucceeded(t *testing.T, result *types.TransactionResult) {
	if !assert.True(t, result.Succeeded()) {
		t.Error(result.Error)
	}
}

func lastCreatedAccount(b *emulator.Blockchain, result *types.TransactionResult) (*flowsdk.Account, error) {
	address, err := lastCreatedAccountAddress(result)
	if err != nil {
		return nil, err
	}

	return b.GetAccount(address)
}

func lastCreatedAccountAddress(result *types.TransactionResult) (flowsdk.Address, error) {
	for _, event := range result.Events {
		if event.Type == flowsdk.EventAccountCreated {
			return flowsdk.Address(event.Value.Fields[0].(cadence.Address)), nil
		}
	}

	return flowsdk.Address{}, fmt.Errorf("no account created in this result")
}
