package contracts

import (
	"fmt"
	"github.com/onflow/flow-emulator/blockchain"
	"github.com/onflow/flow-emulator/convert"
	flowsdk "github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/templates"
	"github.com/onflow/flow-go/fvm"
	flowgo "github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-nft/lib/go/contracts"
	fusd "github.com/onflow/fusd/lib/go/contracts"
	nftstorefront "github.com/onflow/nft-storefront/lib/go/contracts"
)

type DeployDescription struct {
	Name        string
	Address     flowsdk.Address
	Description string
}

func DeployContracts(b *blockchain.Blockchain) ([]DeployDescription, error) {
	ftAddress := flowsdk.HexToAddress(fvm.FungibleTokenAddress(b.GetChain()).Hex())
	serviceAddress := b.ServiceKey().Address
	serviceAddressSDK := flowsdk.HexToAddress(b.ServiceKey().Address.Hex())

	toDeploy := []struct {
		name        string
		description string
		source      []byte
	}{
		{
			name:        "FUSD",
			description: "💵  FUSD contract",
			source:      fusd.FUSD(ftAddress.String()),
		},
		{
			name:        "NonFungibleToken",
			description: "✨  NFT contract",
			source:      contracts.NonFungibleToken(),
		},
		{
			name:        "MetadataViews",
			description: "✨  Metadata views contract",
			source:      contracts.MetadataViews(ftAddress, serviceAddressSDK),
		},
		{
			name:        "ExampleNFT",
			description: "✨  Example NFT contract",
			source:      contracts.ExampleNFT(serviceAddressSDK, serviceAddressSDK),
		},
		{
			name:        "NFTStorefrontV2",
			description: "✨   NFT Storefront contract v2",
			source:      nftstorefront.NFTStorefront(2, ftAddress.String(), serviceAddressSDK.String()),
		},
		{
			name:        "NFTStorefront",
			description: "✨   NFT Storefront contract",
			source:      nftstorefront.NFTStorefront(1, ftAddress.String(), serviceAddressSDK.String()),
		},
	}

	for _, c := range toDeploy {
		err := deployContract(b, c.name, c.source)
		if err != nil {
			return nil, err
		}
	}

	serviceAcct, err := b.GetAccount(flowgo.Address(serviceAddress))
	if err != nil {
		return nil, err
	}

	deployDescriptions := make([]DeployDescription, 0)
	for _, c := range toDeploy {
		_, ok := serviceAcct.Contracts[c.name]
		if !ok {
			continue
		}
		deployDescriptions = append(
			deployDescriptions,
			DeployDescription{
				Name:        c.name,
				Address:     serviceAddressSDK,
				Description: c.description,
			},
		)
	}

	return deployDescriptions, nil
}

func deployContract(b *blockchain.Blockchain, name string, contract []byte) error {

	serviceKey := b.ServiceKey()
	serviceAddressSDK := flowsdk.HexToAddress(serviceKey.Address.Hex())

	if serviceKey.PrivateKey == nil {
		return fmt.Errorf("not able to deploy contracts without set private key")
	}

	latestBlock, err := b.GetLatestBlock()
	if err != nil {
		return err
	}

	tx := templates.AddAccountContract(serviceAddressSDK, templates.Contract{
		Name:   name,
		Source: string(contract),
	})

	tx.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
		SetReferenceBlockID(flowsdk.Identifier(latestBlock.ID())).
		SetProposalKey(serviceAddressSDK, serviceKey.Index, serviceKey.SequenceNumber).
		SetPayer(serviceAddressSDK)

	signer, err := serviceKey.Signer()
	if err != nil {
		return err
	}

	err = tx.SignEnvelope(serviceAddressSDK, serviceKey.Index, signer)
	if err != nil {
		return err
	}

	err = b.AddTransaction(*convert.SDKTransactionToFlow(*tx))
	if err != nil {
		return err
	}

	_, results, err := b.ExecuteAndCommitBlock()
	if err != nil {
		return err
	}

	lastResult := results[len(results)-1]
	if !lastResult.Succeeded() {
		return lastResult.Error
	}

	return nil
}
