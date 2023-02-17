// Package emulator provides an emulated version of the Flow blockchain that can be used
// for development purposes.
//
// This package can be used as a library or as a standalone application.
//
// When used as a library, this package provides tools to write programmatic tests for
// Flow applications.
//
// When used as a standalone application, this package implements the Flow Access API
// and is fully-compatible with Flow gRPC client libraries.
package emulator

import (
	"context"
	"errors"
	"fmt"
	"github.com/logrusorgru/aurora"
	"github.com/onflow/flow-go/fvm/derived"
	"github.com/onflow/flow-go/fvm/tracing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"sync"
	"time"

	"github.com/onflow/cadence"
	"github.com/onflow/cadence/runtime"
	"github.com/onflow/cadence/runtime/common"
	"github.com/onflow/cadence/runtime/interpreter"
	"github.com/onflow/flow-go-sdk"
	sdk "github.com/onflow/flow-go-sdk"
	sdkcrypto "github.com/onflow/flow-go-sdk/crypto"
	"github.com/onflow/flow-go-sdk/templates"
	"github.com/onflow/flow-go/access"
	"github.com/onflow/flow-go/crypto"
	"github.com/onflow/flow-go/crypto/hash"
	"github.com/onflow/flow-go/engine/execution/state/delta"
	"github.com/onflow/flow-go/fvm"
	fvmcrypto "github.com/onflow/flow-go/fvm/crypto"
	"github.com/onflow/flow-go/fvm/environment"
	fvmerrors "github.com/onflow/flow-go/fvm/errors"
	reusableRuntime "github.com/onflow/flow-go/fvm/runtime"
	"github.com/onflow/flow-go/fvm/state"
	flowgo "github.com/onflow/flow-go/model/flow"
	"github.com/rs/zerolog"

	"github.com/onflow/flow-emulator/convert"
	sdkconvert "github.com/onflow/flow-emulator/convert/sdk"
	"github.com/onflow/flow-emulator/storage"
	"github.com/onflow/flow-emulator/storage/sqlite"
	"github.com/onflow/flow-emulator/types"
)

var _ Emulator = &Blockchain{}

// Blockchain emulates the functionality of the Flow blockchain.
type Blockchain struct {
	// committed chain state: blocks, transactions, registers, events
	storage storage.Store

	// mutex protecting pending block
	mu sync.RWMutex

	// pending block containing block info, register state, pending transactions
	pendingBlock *pendingBlock

	// used to execute transactions and scripts
	vm    *fvm.VirtualMachine
	vmCtx fvm.Context

	transactionValidator *access.TransactionValidator

	serviceKey ServiceKey
	autoMine   bool
	logger     *zerolog.Logger

	debugger               *interpreter.Debugger
	activeDebuggingSession bool
	currentCode            string
	currentScriptID        string
}

type ServiceKey struct {
	Index          int
	Address        flowgo.Address
	SequenceNumber uint64
	PrivateKey     sdkcrypto.PrivateKey
	PublicKey      sdkcrypto.PublicKey
	HashAlgo       sdkcrypto.HashAlgorithm
	SigAlgo        sdkcrypto.SignatureAlgorithm
	Weight         int
}

func (s ServiceKey) Signer() (sdkcrypto.Signer, error) {
	return sdkcrypto.NewInMemorySigner(s.PrivateKey, s.HashAlgo)
}

func (s ServiceKey) AccountKey() *sdk.AccountKey {

	var publicKey sdkcrypto.PublicKey
	if s.PublicKey != nil {
		publicKey = s.PublicKey
	}

	if s.PrivateKey != nil {
		publicKey = s.PrivateKey.PublicKey()
	}

	return &sdk.AccountKey{
		Index:          s.Index,
		PublicKey:      publicKey,
		SigAlgo:        s.SigAlgo,
		HashAlgo:       s.HashAlgo,
		Weight:         s.Weight,
		SequenceNumber: s.SequenceNumber,
	}
}

const defaultServiceKeyPrivateKeySeed = "elephant ears space cowboy octopus rodeo potato cannon pineapple"
const DefaultServiceKeySigAlgo = sdkcrypto.ECDSA_P256
const DefaultServiceKeyHashAlgo = sdkcrypto.SHA3_256

func DefaultServiceKey() ServiceKey {
	return GenerateDefaultServiceKey(DefaultServiceKeySigAlgo, DefaultServiceKeyHashAlgo)
}

func GenerateDefaultServiceKey(
	sigAlgo sdkcrypto.SignatureAlgorithm,
	hashAlgo sdkcrypto.HashAlgorithm,
) ServiceKey {
	privateKey, err := sdkcrypto.GeneratePrivateKey(
		sigAlgo,
		[]byte(defaultServiceKeyPrivateKeySeed),
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to generate default service key: %s", err.Error()))
	}

	return ServiceKey{
		PrivateKey: privateKey,
		SigAlgo:    sigAlgo,
		HashAlgo:   hashAlgo,
	}
}

// config is a set of configuration options for an emulated blockchain.
type config struct {
	ServiceKey                   ServiceKey
	Store                        storage.Store
	SimpleAddresses              bool
	GenesisTokenSupply           cadence.UFix64
	TransactionMaxGasLimit       uint64
	ScriptGasLimit               uint64
	TransactionExpiry            uint
	StorageLimitEnabled          bool
	TransactionFeesEnabled       bool
	ContractRemovalEnabled       bool
	MinimumStorageReservation    cadence.UFix64
	StorageMBPerFLOW             cadence.UFix64
	Logger                       *zerolog.Logger
	TransactionValidationEnabled bool
	ChainID                      flowgo.ChainID
	AutoMine                     bool
}

func (conf config) GetStore() storage.Store {
	if conf.Store == nil {
		store, err := sqlite.New(":memory:")
		if err != nil {
			panic("Cannot initialize memory storage")
		}
		conf.Store = store
	}
	return conf.Store
}

func (conf config) GetChainID() flowgo.ChainID {
	if conf.SimpleAddresses {
		return flowgo.MonotonicEmulator
	}

	return conf.ChainID
}

func (conf config) GetServiceKey() ServiceKey {
	// set up service key
	serviceKey := conf.ServiceKey
	serviceKey.Address = conf.GetChainID().Chain().ServiceAddress()
	serviceKey.Weight = sdk.AccountKeyWeightThreshold

	return serviceKey
}

const defaultGenesisTokenSupply = "1000000000.0"
const defaultScriptGasLimit = 100000
const defaultTransactionMaxGasLimit = flowgo.DefaultMaxTransactionGasLimit

// defaultConfig is the default configuration for an emulated blockchain.
var defaultConfig = func() config {
	genesisTokenSupply, err := cadence.NewUFix64(defaultGenesisTokenSupply)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse default genesis token supply: %s", err.Error()))
	}

	return config{
		ServiceKey:                   DefaultServiceKey(),
		Store:                        nil,
		SimpleAddresses:              false,
		GenesisTokenSupply:           genesisTokenSupply,
		ScriptGasLimit:               defaultScriptGasLimit,
		TransactionMaxGasLimit:       defaultTransactionMaxGasLimit,
		MinimumStorageReservation:    fvm.DefaultMinimumStorageReservation,
		StorageMBPerFLOW:             fvm.DefaultStorageMBPerFLOW,
		TransactionExpiry:            0, // TODO: replace with sensible default
		StorageLimitEnabled:          true,
		Logger:                       nil,
		TransactionValidationEnabled: true,
		ChainID:                      flowgo.Emulator,
		AutoMine:                     false,
	}
}()

// Option is a function applying a change to the emulator config.
type Option func(*config)

// WithLogger sets the logger
func WithLogger(
	logger *zerolog.Logger,
) Option {
	return func(c *config) {
		c.Logger = logger
	}
}

// WithServicePublicKey sets the service key from a public key.
func WithServicePublicKey(
	servicePublicKey sdkcrypto.PublicKey,
	sigAlgo sdkcrypto.SignatureAlgorithm,
	hashAlgo sdkcrypto.HashAlgorithm,
) Option {
	return func(c *config) {
		c.ServiceKey = ServiceKey{
			PublicKey: servicePublicKey,
			SigAlgo:   sigAlgo,
			HashAlgo:  hashAlgo,
		}
	}
}

// WithServicePrivateKey sets the service key from private key.
func WithServicePrivateKey(
	privateKey sdkcrypto.PrivateKey,
	sigAlgo sdkcrypto.SignatureAlgorithm,
	hashAlgo sdkcrypto.HashAlgorithm,
) Option {
	return func(c *config) {
		c.ServiceKey = ServiceKey{
			PrivateKey: privateKey,
			PublicKey:  privateKey.PublicKey(),
			HashAlgo:   hashAlgo,
			SigAlgo:    sigAlgo,
		}
	}
}

// WithStore sets the persistent storage provider.
func WithStore(store storage.Store) Option {
	return func(c *config) {
		c.Store = store
	}
}

// WithSimpleAddresses enables simple addresses, which are sequential starting with 0x01.
func WithSimpleAddresses() Option {
	return func(c *config) {
		c.SimpleAddresses = true
	}
}

// WithGenesisTokenSupply sets the genesis token supply.
func WithGenesisTokenSupply(supply cadence.UFix64) Option {
	return func(c *config) {
		c.GenesisTokenSupply = supply
	}
}

// WithTransactionMaxGasLimit sets the maximum gas limit for transactions.
//
// Individual transactions will still be bounded by the limit they declare.
// This function sets the maximum limit that any transaction can declare.
//
// This limit does not affect script executions. Use WithScriptGasLimit
// to set the gas limit for script executions.
func WithTransactionMaxGasLimit(maxLimit uint64) Option {
	return func(c *config) {
		c.TransactionMaxGasLimit = maxLimit
	}
}

// WithScriptGasLimit sets the gas limit for scripts.
//
// This limit does not affect transactions, which declare their own limit.
// Use WithTransactionMaxGasLimit to set the maximum gas limit for transactions.
func WithScriptGasLimit(limit uint64) Option {
	return func(c *config) {
		c.ScriptGasLimit = limit
	}
}

// WithTransactionExpiry sets the transaction expiry measured in blocks.
//
// If set to zero, transaction expiry is disabled and the reference block ID field
// is not required.
func WithTransactionExpiry(expiry uint) Option {
	return func(c *config) {
		c.TransactionExpiry = expiry
	}
}

// WithStorageLimitEnabled enables/disables limiting account storage used to their storage capacity.
//
// If set to false, accounts can store any amount of data,
// otherwise they can only store as much as their storage capacity.
// The default is true.
func WithStorageLimitEnabled(enabled bool) Option {
	return func(c *config) {
		c.StorageLimitEnabled = enabled
	}
}

// WithMinimumStorageReservation sets the minimum account balance.
//
// The cost of creating new accounts is also set to this value.
// The default is taken from fvm.DefaultMinimumStorageReservation
func WithMinimumStorageReservation(minimumStorageReservation cadence.UFix64) Option {
	return func(c *config) {
		c.MinimumStorageReservation = minimumStorageReservation
	}
}

// WithStorageMBPerFLOW sets the cost of a megabyte of storage in FLOW
//
// the default is taken from fvm.DefaultStorageMBPerFLOW
func WithStorageMBPerFLOW(storageMBPerFLOW cadence.UFix64) Option {
	return func(c *config) {
		c.StorageMBPerFLOW = storageMBPerFLOW
	}
}

// WithTransactionFeesEnabled enables/disables transaction fees.
//
// If set to false transactions don't cost any flow.
// The default is false.
func WithTransactionFeesEnabled(enabled bool) Option {
	return func(c *config) {
		c.TransactionFeesEnabled = enabled
	}
}

// WithContractRemovalEnabled restricts/allows removal of already deployed contracts.
//
// The default is provided by on-chain value.
func WithContractRemovalEnabled(enabled bool) Option {
	return func(c *config) {
		c.ContractRemovalEnabled = enabled
	}
}

// WithTransactionValidationEnabled enables/disables transaction validation.
//
// If set to false, the emulator will not verify transaction signatures or validate sequence numbers.
//
// The default is true.
func WithTransactionValidationEnabled(enabled bool) Option {
	return func(c *config) {
		c.TransactionValidationEnabled = enabled
	}
}

// WithChainID sets chain type for address generation
// The default is emulator.
func WithChainID(chainID flowgo.ChainID) Option {
	return func(c *config) {
		c.ChainID = chainID
	}
}

// NewBlockchain instantiates a new emulated blockchain with the provided options.
func NewBlockchain(opts ...Option) (*Blockchain, error) {

	// apply options to the default config
	conf := defaultConfig
	for _, opt := range opts {
		opt(&conf)
	}

	b := &Blockchain{
		storage:                conf.GetStore(),
		serviceKey:             conf.GetServiceKey(),
		debugger:               nil,
		activeDebuggingSession: false,
		logger:                 conf.Logger,
	}

	var err error

	blocks := newBlocks(b)

	b.vm, b.vmCtx, err = configureFVM(b, conf, blocks)
	if err != nil {
		return nil, err
	}

	latestBlock, latestLedgerView, err := configureLedger(conf, b.storage, b.vm, b.vmCtx)
	if err != nil {
		return nil, err
	}

	b.pendingBlock = newPendingBlock(latestBlock, latestLedgerView)
	b.transactionValidator = configureTransactionValidator(conf, blocks)

	return b, nil
}

func configureFVM(blockchain *Blockchain, conf config, blocks *blocks) (*fvm.VirtualMachine, fvm.Context, error) {
	vm := fvm.NewVirtualMachine()

	fvmOptions := []fvm.Option{
		fvm.WithChain(conf.GetChainID().Chain()),
		fvm.WithBlocks(blocks),
		fvm.WithContractDeploymentRestricted(false),
		fvm.WithContractRemovalRestricted(!conf.ContractRemovalEnabled),
		fvm.WithGasLimit(conf.ScriptGasLimit),
		fvm.WithCadenceLogging(true),
		fvm.WithAccountStorageLimit(conf.StorageLimitEnabled),
		fvm.WithTransactionFeesEnabled(conf.TransactionFeesEnabled),
		fvm.WithReusableCadenceRuntimePool(
			reusableRuntime.NewReusableCadenceRuntimePool(1, runtime.Config{Debugger: blockchain.debugger}),
		),
	}

	if conf.Logger != nil {
		fvmLogger := conf.Logger.With().Str("module", "fvm").Logger().Level(zerolog.InfoLevel)
		fvmOptions = append(
			fvmOptions, fvm.WithLogger(fvmLogger))
	}

	if !conf.TransactionValidationEnabled {
		fvmOptions = append(
			fvmOptions,
			fvm.WithAuthorizationChecksEnabled(false),
			fvm.WithSequenceNumberCheckAndIncrementEnabled(false))
	}

	ctx := fvm.NewContext(
		fvmOptions...,
	)

	return vm, ctx, nil
}

func configureLedger(
	conf config,
	store storage.Store,
	vm *fvm.VirtualMachine,
	ctx fvm.Context,
) (*flowgo.Block, *delta.View, error) {
	latestBlock, err := store.LatestBlock(context.Background())
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// storage is empty, bootstrap new ledger state
			return configureNewLedger(conf, store, vm, ctx)
		}

		// internal storage error, fail fast
		return nil, nil, err
	}

	// storage contains data, load state from storage
	return configureExistingLedger(&latestBlock, store)
}

func configureNewLedger(
	conf config,
	store storage.Store,
	vm *fvm.VirtualMachine,
	ctx fvm.Context,
) (*flowgo.Block, *delta.View, error) {
	genesisLedgerView := store.LedgerViewByHeight(context.Background(), 0)
	err := bootstrapLedger(
		vm,
		ctx,
		genesisLedgerView,
		conf,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to bootstrap execution state: %w", err)
	}

	// commit the genesis block to storage
	genesis := flowgo.Genesis(conf.GetChainID())

	err = store.CommitBlock(
		context.Background(),
		*genesis,
		nil,
		nil,
		nil,
		genesisLedgerView.Delta(),
		nil,
	)
	if err != nil {
		return nil, nil, err
	}

	// get empty ledger view
	ledgerView := store.LedgerViewByHeight(context.Background(), 0)

	return genesis, ledgerView, nil
}

func configureExistingLedger(
	latestBlock *flowgo.Block,
	store storage.Store,
) (*flowgo.Block, *delta.View, error) {
	latestLedgerView := store.LedgerViewByHeight(context.Background(), latestBlock.Header.Height)

	return latestBlock, latestLedgerView, nil
}

func bootstrapLedger(
	vm *fvm.VirtualMachine,
	ctx fvm.Context,
	ledger state.View,
	conf config,
) error {
	accountKey := conf.GetServiceKey().AccountKey()
	publicKey, _ := crypto.DecodePublicKey(
		accountKey.SigAlgo,
		accountKey.PublicKey.Encode(),
	)

	ctx = fvm.NewContextFromParent(
		ctx,
		fvm.WithAccountStorageLimit(false),
	)

	flowAccountKey := flowgo.AccountPublicKey{
		PublicKey: publicKey,
		SignAlgo:  accountKey.SigAlgo,
		HashAlgo:  accountKey.HashAlgo,
		Weight:    fvm.AccountKeyWeightThreshold,
	}

	bootstrap := configureBootstrapProcedure(conf, flowAccountKey, conf.GenesisTokenSupply)

	err := vm.Run(ctx, bootstrap, ledger)
	if err != nil {
		return err
	}

	return nil
}

func configureBootstrapProcedure(conf config, flowAccountKey flowgo.AccountPublicKey, supply cadence.UFix64) *fvm.BootstrapProcedure {
	options := make([]fvm.BootstrapProcedureOption, 0)
	options = append(options,
		fvm.WithInitialTokenSupply(supply),
		fvm.WithRestrictedAccountCreationEnabled(false),
	)
	if conf.StorageLimitEnabled {
		options = append(options,
			fvm.WithAccountCreationFee(conf.MinimumStorageReservation),
			fvm.WithMinimumStorageReservation(conf.MinimumStorageReservation),
			fvm.WithStorageMBPerFLOW(conf.StorageMBPerFLOW),
		)
	}
	if conf.TransactionFeesEnabled {
		// This enables variable transaction fees AND execution effort metering
		// as described in Variable Transaction Fees: Execution Effort FLIP: https://github.com/onflow/flow/pull/753)
		// TODO: In the future this should be an injectable parameter. For now this is hard coded
		// as this is the first iteration of variable execution fees.
		options = append(options,
			fvm.WithTransactionFee(fvm.BootstrapProcedureFeeParameters{
				SurgeFactor:         cadence.UFix64(100_000_000), // 1.0
				InclusionEffortCost: cadence.UFix64(100),         // 1E-6
				ExecutionEffortCost: cadence.UFix64(499_000_000), // 4.99
			}),
			fvm.WithExecutionEffortWeights(map[common.ComputationKind]uint64{
				common.ComputationKindStatement:          1569,
				common.ComputationKindLoop:               1569,
				common.ComputationKindFunctionInvocation: 1569,
				environment.ComputationKindGetValue:      808,
				environment.ComputationKindCreateAccount: 2837670,
				environment.ComputationKindSetValue:      765,
			}),
		)
	}
	return fvm.Bootstrap(
		flowAccountKey,
		options...,
	)
}

func configureTransactionValidator(conf config, blocks *blocks) *access.TransactionValidator {
	return access.NewTransactionValidator(
		blocks,
		conf.GetChainID().Chain(),
		access.TransactionValidationOptions{
			Expiry:                       conf.TransactionExpiry,
			ExpiryBuffer:                 0,
			AllowEmptyReferenceBlockID:   conf.TransactionExpiry == 0,
			AllowUnknownReferenceBlockID: false,
			MaxGasLimit:                  conf.TransactionMaxGasLimit,
			CheckScriptsParse:            true,
			MaxTransactionByteSize:       flowgo.DefaultMaxTransactionByteSize,
			MaxCollectionByteSize:        flowgo.DefaultMaxCollectionByteSize,
		},
	)
}

func (b *Blockchain) newFVMContextFromHeader(header *flowgo.Header) fvm.Context {
	return fvm.NewContextFromParent(
		b.vmCtx,
		fvm.WithBlockHeader(header),
		fvm.WithReusableCadenceRuntimePool(
			reusableRuntime.NewReusableCadenceRuntimePool(1, runtime.Config{Debugger: b.debugger}),
		),
	)
}

func (b *Blockchain) CurrentScript() (string, string) {
	return b.currentScriptID, b.currentCode
}

// ServiceKey returns the service private key for this blockchain.
func (b *Blockchain) ServiceKey() ServiceKey {
	serviceAccount, err := b.GetAccount(b.serviceKey.Address)
	if err != nil {
		return b.serviceKey
	}

	if len(serviceAccount.Keys) > 0 {
		b.serviceKey.Index = 0
		b.serviceKey.SequenceNumber = serviceAccount.Keys[0].SeqNumber
		b.serviceKey.Weight = serviceAccount.Keys[0].Weight
	}

	return b.serviceKey
}

// PendingBlockID returns the ID of the pending block.
func (b *Blockchain) PendingBlockID() flowgo.Identifier {
	return b.pendingBlock.ID()
}

// PendingBlockView returns the view of the pending block.
func (b *Blockchain) PendingBlockView() uint64 {
	return b.pendingBlock.view
}

// PendingBlockTimestamp returns the Timestamp of the pending block.
func (b *Blockchain) PendingBlockTimestamp() time.Time {
	return b.pendingBlock.Block().Header.Timestamp
}

func (b *Blockchain) EnableAutoMine() {
	b.autoMine = true
}

func (b *Blockchain) DisableAutoMine() {
	b.autoMine = false
}

func (b *Blockchain) Ping() error {
	b.logger.Debug().Msg("🎁  Ping called")
	return nil
}

func (b *Blockchain) GetChain() flowgo.Chain {
	return b.vmCtx.Chain
}

func (b *Blockchain) GetNetworkParameters() access.NetworkParameters {
	b.logger.Debug().
		Str("chainID", b.GetChain().ChainID().String()).
		Msg("🎁  GetNetworkParameters called")

	return access.NetworkParameters{
		ChainID: b.GetChain().ChainID(),
	}
}

// GetLatestBlock gets the latest sealed block.
func (b *Blockchain) GetLatestBlock() (*flowgo.Block, flowgo.BlockStatus, error) {
	block, err := b.storage.LatestBlock(context.Background())
	if err != nil {
		return nil, flowgo.BlockStatusUnknown, &StorageError{err}
	}

	b.logger.Debug().Fields(map[string]any{
		"blockHeight": block.Header.Height,
		"blockID":     block.ID().String(),
	}).Msg("🎁  GetLatestBlock called")

	return &block, flowgo.BlockStatusSealed, nil
}

func (b *Blockchain) getBlockByHeight(height uint64) (*flowgo.Block, error) {
	block, err := b.storage.BlockByHeight(context.Background(), height)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, &BlockNotFoundByHeightError{Height: height}
		}
		return nil, err
	}

	return block, nil
}

// GetBlockByHeight gets a block by height.
func (b *Blockchain) GetBlockByHeight(height uint64) (*flowgo.Block, flowgo.BlockStatus, error) {
	block, err := b.getBlockByHeight(height)
	if err != nil {
		return nil, flowgo.BlockStatusUnknown, err
	}

	b.logger.Debug().Fields(map[string]any{
		"blockHeight": block.Header.Height,
		"blockID":     block.ID().String(),
	}).Msg("🎁  GetBlockByHeight called")

	return block, flowgo.BlockStatusSealed, nil
}

// GetBlockByID gets a block by ID.
func (b *Blockchain) GetBlockByID(id flowgo.Identifier) (*flowgo.Block, flowgo.BlockStatus, error) {
	block, err := b.storage.BlockByID(context.Background(), id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, flowgo.BlockStatusUnknown, &BlockNotFoundByIDError{ID: id}
		}
		return nil, flowgo.BlockStatusUnknown, &StorageError{err}
	}

	b.logger.Debug().Fields(map[string]any{
		"blockHeight": block.Header.Height,
		"blockID":     block.ID().String(),
	}).Msg("🎁  GetBlockByID called")

	return block, flowgo.BlockStatusSealed, nil
}

func (b *Blockchain) GetCollectionByID(colID flowgo.Identifier) (*flowgo.LightCollection, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	col, err := b.storage.CollectionByID(context.Background(), colID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, &CollectionNotFoundError{ID: colID}
		}
		return nil, &StorageError{err}
	}

	b.logger.Debug().Str("colID", colID.String()).
		Msg("📚  GetCollectionByID called")

	return &col, nil
}

// GetTransaction gets an existing transaction by ID.
//
// The function first looks in the pending block, then the current blockchain state.
func (b *Blockchain) GetTransaction(txID flowgo.Identifier) (*flowgo.TransactionBody, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	pendingTx := b.pendingBlock.GetTransaction(txID)
	if pendingTx != nil {
		return pendingTx, nil
	}

	tx, err := b.storage.TransactionByID(context.Background(), txID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, &TransactionNotFoundError{ID: txID}
		}
		return nil, &StorageError{err}
	}

	b.logger.Debug().
		Str("txID", txID.String()).
		Msg("💵  GetTransaction called")

	return &tx, nil
}

func (b *Blockchain) GetTransactionResult(txID flowgo.Identifier) (*access.TransactionResult, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.pendingBlock.ContainsTransaction(txID) {
		return &access.TransactionResult{
			Status: flowgo.TransactionStatusPending,
		}, nil
	}

	storedResult, err := b.storage.TransactionResultByID(context.Background(), txID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return &access.TransactionResult{
				Status: flowgo.TransactionStatusUnknown,
			}, nil
		}
		return nil, &StorageError{err}
	}

	//TODO: bluesign: something missing here (blockHeight, blockID..)
	result := access.TransactionResult{
		Status:        flowgo.TransactionStatusSealed,
		StatusCode:    uint(storedResult.ErrorCode),
		ErrorMessage:  storedResult.ErrorMessage,
		Events:        storedResult.Events,
		TransactionID: txID,
		BlockHeight:   0,
	}

	b.logger.Debug().
		Str("txID", txID.String()).
		Msg("📝  GetTransactionResult called")

	return &result, nil
}

// GetAccount returns the account for the given address.
func (b *Blockchain) GetAccount(address flowgo.Address) (*flowgo.Account, error) {

	b.logger.Debug().
		Str("address", address.String()).
		Msg("👤  GetAccount called")

	latestBlock, _, err := b.GetLatestBlock()
	if err != nil {
		return nil, err
	}
	return b.GetAccountAtBlockHeight(address, latestBlock.Header.Height)
}

// GetAccountAtBlockHeight returns the account for the given address at specified block height.
func (b *Blockchain) GetAccountAtBlockHeight(address flowgo.Address, blockHeight uint64) (*flowgo.Account, error) {
	b.logger.Debug().
		Str("address", address.String()).
		Uint64("height", blockHeight).
		Msg("👤  GetAccountAtBlockHeight called")

	account, err := b.vm.GetAccount(
		b.vmCtx,
		address,
		b.storage.LedgerViewByHeight(context.Background(), blockHeight),
	)

	if fvmerrors.IsAccountNotFoundError(err) {
		return nil, &AccountNotFoundError{Address: address}
	}

	return account, nil
}

// GetAccountByIndex  returns the account for the given index.
func (b *Blockchain) GetAccountByIndex(index uint) (*flowgo.Account, error) {

	generator := flow.NewAddressGenerator(sdk.ChainID(b.vmCtx.Chain.ChainID()))

	generator.SetIndex(index)

	account, err := b.GetAccountUnsafe(sdkconvert.SDKAddressToFlow(generator.Address()))
	if err != nil {
		return nil, err
	}

	return account, nil
}

// Deprecated: Needed for the debugger right now, do NOT use for other purposes.
// TODO: refactor
func (b *Blockchain) GetAccountUnsafe(address flowgo.Address) (*flowgo.Account, error) {
	latestBlock, _, err := b.GetLatestBlock()
	if err != nil {
		return nil, err
	}
	return b.GetAccountAtBlockHeight(address, latestBlock.Header.Height)
}

// GetEventsByHeight returns the events in the block at the given height, optionally filtered by type.
func (b *Blockchain) GetEventsByHeight(blockHeight uint64, eventType string) ([]flowgo.Event, error) {
	flowEvents, err := b.storage.EventsByHeight(context.Background(), blockHeight, eventType)
	if err != nil {
		return nil, err
	}
	return flowEvents, err
}

func validateEventType(eventType string) error {
	if len(strings.TrimSpace(eventType)) == 0 {
		return fmt.Errorf("invalid query: eventType must not be empty")
	}
	return nil
}

func (b *Blockchain) GetEventsForBlockIDs(
	eventType string,
	blockIDs []flowgo.Identifier,
) ([]flowgo.BlockEvents, error) {

	err := validateEventType(eventType)
	if err != nil {
		return nil, err
	}

	results := make([]flowgo.BlockEvents, 0)
	eventCount := 0
	for _, blockID := range blockIDs {
		block, _, err := b.GetBlockByID(blockID)
		if err != nil {
			return nil, err
		}

		events, err := b.GetEventsByHeight(block.Header.Height, eventType)
		if err != nil {
			return nil, err
		}

		result := flowgo.BlockEvents{
			BlockID:        block.Header.ID(),
			BlockHeight:    block.Header.Height,
			BlockTimestamp: block.Header.Timestamp,
			Events:         events,
		}

		results = append(results, result)
		eventCount += len(events)
	}
	b.logger.Debug().Fields(map[string]any{
		"eventType":  eventType,
		"eventCount": eventCount,
	}).Msg("🎁  GetEventsForBlockIDs called")
	return results, nil

}

// GetEventsForHeightRange returns events matching a query.
func (b *Blockchain) GetEventsForHeightRange(
	eventType string,
	startHeight, endHeight uint64,
) ([]flowgo.BlockEvents, error) {

	err := validateEventType(eventType)
	if err != nil {
		return nil, err
	}

	latestBlock, _, err := b.GetLatestBlock()
	if err != nil {
		return nil, err
	}

	// if end height is not set, use the latest block height
	// if end height is higher than latest, use latest
	if endHeight == 0 || endHeight > latestBlock.Header.Height {
		endHeight = latestBlock.Header.Height
	}

	// check for invalid queries
	if startHeight > endHeight {
		return nil, NewInvalidArgumentError("startHeight > endHeight")
	}

	results := make([]flowgo.BlockEvents, 0)
	eventCount := 0

	for height := startHeight; height <= endHeight; height++ {
		block, _, err := b.GetBlockByHeight(height)
		if err != nil {
			return nil, err
		}

		events, err := b.GetEventsByHeight(height, eventType)
		if err != nil {
			return nil, err
		}

		result := flowgo.BlockEvents{
			BlockID:        block.ID(),
			BlockHeight:    block.Header.Height,
			BlockTimestamp: block.Header.Timestamp,
			Events:         events,
		}

		results = append(results, result)
		eventCount += len(events)
	}

	b.logger.Debug().Fields(map[string]any{
		"eventType":   eventType,
		"startHeight": startHeight,
		"endHeight":   endHeight,
		"eventCount":  eventCount,
	}).Msg("🎁  GetEventsForHeightRange called")

	return results, nil
}

func (b *Blockchain) executeScriptAtBlock(script []byte, arguments [][]byte, requestedBlock *flowgo.Block) (*types.ScriptResult, error) {

	requestedLedgerView := b.storage.LedgerViewByHeight(context.Background(), requestedBlock.Header.Height)

	header := requestedBlock.Header

	blockContext := fvm.NewContextFromParent(
		b.vmCtx,
		fvm.WithBlockHeader(header),
	)

	scriptProc := fvm.Script(script).WithArguments(arguments...)

	err := b.vm.Run(blockContext, scriptProc, requestedLedgerView)
	if err != nil {
		return nil, err
	}

	hasher := hash.NewSHA3_256()
	scriptID := sdk.HashToID(hasher.ComputeHash(script))

	events, err := sdkconvert.FlowEventsToSDK(scriptProc.Events)
	if err != nil {
		return nil, err
	}

	var scriptError error = nil
	var convertedValue cadence.Value = nil

	if scriptProc.Err == nil {
		convertedValue = scriptProc.Value
	} else {
		scriptError = convert.VMErrorToEmulator(scriptProc.Err)
	}

	return &types.ScriptResult{
		ScriptID:        scriptID,
		Value:           convertedValue,
		Error:           scriptError,
		Logs:            scriptProc.Logs,
		Events:          events,
		ComputationUsed: scriptProc.GasUsed,
	}, nil

}
func (b *Blockchain) printTransactionResult(result *types.TransactionResult) {
	if result.Succeeded() {
		b.logger.Info().
			Str("txID", result.TransactionID.String()).
			Uint64("computationUsed", result.ComputationUsed).
			Msg("⭐  Transaction executed")
	} else {
		b.logger.Warn().
			Str("txID", result.TransactionID.String()).
			Uint64("computationUsed", result.ComputationUsed).
			Msg("❗  Transaction reverted")
	}

	for _, log := range result.Logs {
		b.logger.Info().Msgf(
			"%s %s",
			logPrefix("LOG", result.TransactionID, aurora.BlueFg),
			log,
		)
	}

	for _, event := range result.Events {
		b.logger.Debug().Msgf(
			"%s %s",
			logPrefix("EVT", result.TransactionID, aurora.GreenFg),
			event,
		)
	}

	if !result.Succeeded() {
		b.logger.Warn().Msgf(
			"%s %s",
			logPrefix("ERR", result.TransactionID, aurora.RedFg),
			result.Error.Error(),
		)

		if result.Debug != nil {
			b.logger.Debug().Fields(result.Debug.Meta).Msg(
				fmt.Sprintf("%s %s", "❗  Transaction Signature Error", result.Debug.Message),
			)
		}
	}
}

func (b *Blockchain) printScriptResult(result *types.ScriptResult) {
	if result.Succeeded() {
		b.logger.Info().
			Str("scriptID", result.ScriptID.String()).
			Uint64("computationUsed", result.ComputationUsed).
			Msg("⭐  Script executed")
	} else {
		b.logger.Warn().
			Str("scriptID", result.ScriptID.String()).
			Uint64("computationUsed", result.ComputationUsed).
			Msg("❗  Script reverted")
	}

	for _, log := range result.Logs {
		b.logger.Debug().Msgf(
			"%s %s",
			logPrefix("LOG", result.ScriptID, aurora.BlueFg),
			log,
		)
	}

	if !result.Succeeded() {
		b.logger.Warn().Msgf(
			"%s %s",
			logPrefix("ERR", result.ScriptID, aurora.RedFg),
			result.Error.Error(),
		)
	}
}

func logPrefix(prefix string, id sdk.Identifier, color aurora.Color) string {
	prefix = aurora.Colorize(prefix, color|aurora.BoldFm).String()
	shortID := fmt.Sprintf("[%s]", id.String()[:6])
	shortID = aurora.Colorize(shortID, aurora.FaintFm).String()
	return fmt.Sprintf("%s %s", prefix, shortID)
}

// ExecuteScript executes a read-only script against the world state and returns the result
func (b *Blockchain) ExecuteScript(script []byte, arguments [][]byte) (*types.ScriptResult, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.logger.Debug().Msgf("👤  ExecuteScriptAtLatestBlock called")

	latestBlock, _, err := b.GetLatestBlock()
	if err != nil {
		return nil, err
	}

	result, err := b.executeScriptAtBlock(script, arguments, latestBlock)
	if err != nil {
		return nil, err
	}

	b.printScriptResult(result)
	return result, err
}

func (b *Blockchain) ExecuteScriptAtBlockID(script []byte, arguments [][]byte, blockID flowgo.Identifier) (*types.ScriptResult, error) {
	b.logger.Debug().
		Str("blockID", blockID.String()).
		Msg("👤  ExecuteScriptAtBlockID called")

	requestedBlock, _, err := b.GetBlockByID(blockID)
	if err != nil {
		return nil, err
	}

	result, err := b.executeScriptAtBlock(script, arguments, requestedBlock)
	if err != nil {
		return nil, err
	}

	b.printScriptResult(result)
	return result, err
}

func (b *Blockchain) ExecuteScriptAtBlockHeight(script []byte, arguments [][]byte, blockHeight uint64) (*types.ScriptResult, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.logger.Debug().
		Uint64("blockHeight", blockHeight).
		Msg("👤  ExecuteScriptAtBlockHeight called")

	requestedBlock, err := b.getBlockByHeight(blockHeight)
	if err != nil {
		return nil, err
	}

	result, err := b.executeScriptAtBlock(script, arguments, requestedBlock)
	if err != nil {
		return nil, err
	}

	b.printScriptResult(result)
	return result, err
}

// SendTransaction submits a transaction to the network.
func (b *Blockchain) SendTransaction(flowTx *flowgo.TransactionBody) error {
	err := b.AddTransaction(*flowTx)
	if err != nil {
		switch t := err.(type) {
		case *DuplicateTransactionError:
			return NewInvalidArgumentError(err.Error())
		case *types.FlowError:
			switch t.FlowError.Code() {
			case fvmerrors.ErrCodeAccountAuthorizationError,
				fvmerrors.ErrCodeInvalidEnvelopeSignatureError,
				fvmerrors.ErrCodeInvalidPayloadSignatureError,
				fvmerrors.ErrCodeInvalidProposalSignatureError,
				fvmerrors.ErrCodeAccountPublicKeyNotFoundError,
				fvmerrors.ErrCodeInvalidProposalSeqNumberError,
				fvmerrors.ErrCodeInvalidAddressError:
				return NewInvalidArgumentError(err.Error())

			default:
				if fvmerrors.IsAccountNotFoundError(err) {
					return NewInvalidArgumentError(err.Error())
				}
				return NewInternalError(err.Error())

			}
		default:
			return NewInternalError(err.Error())
		}
	}

	b.logger.Debug().
		Str("txID", flowTx.ID().String()).
		Msg(`✉️   Transaction submitted`)

	if b.autoMine {
		_, _, err := b.executeAndCommitBlock()
		if err != nil {
			return err
		}
	}

	return nil
}

// AddTransaction validates a transaction and adds it to the current pending block.
func (b *Blockchain) AddTransaction(tx flowgo.TransactionBody) error {

	// If index > 0, pending block has begun execution (cannot add more transactions)
	if b.pendingBlock.ExecutionStarted() {
		return &PendingBlockMidExecutionError{BlockID: b.pendingBlock.ID()}
	}

	if b.pendingBlock.ContainsTransaction(tx.ID()) {
		return &DuplicateTransactionError{TxID: tx.ID()}
	}

	_, err := b.storage.TransactionByID(context.Background(), tx.ID())
	if err == nil {
		// Found the transaction, this is a duplicate
		return &DuplicateTransactionError{TxID: tx.ID()}
	} else if !errors.Is(err, storage.ErrNotFound) {
		// Error in the storage provider
		return fmt.Errorf("failed to check storage for transaction %w", err)
	}

	err = b.transactionValidator.Validate(&tx)
	if err != nil {
		return convertAccessError(err)
	}

	// add transaction to pending block
	b.pendingBlock.AddTransaction(tx)

	return nil
}

func (b *Blockchain) ExecuteBlock() ([]*types.TransactionResult, error) {
	results := make([]*types.TransactionResult, 0)

	// empty blocks do not require execution, treat as a no-op
	if b.pendingBlock.Empty() {
		return results, nil
	}

	header := b.pendingBlock.Block().Header
	blockContext := fvm.NewContextFromParent(
		b.vmCtx,
		fvm.WithBlockHeader(header),
	)

	// cannot execute a block that has already executed
	if b.pendingBlock.ExecutionComplete() {
		return results, &PendingBlockTransactionsExhaustedError{
			BlockID: b.pendingBlock.ID(),
		}
	}

	// continue executing transactions until execution is complete
	for !b.pendingBlock.ExecutionComplete() {
		result, err := b.executeNextTransaction(blockContext)
		if err != nil {
			return results, err
		}

		results = append(results, result)
	}

	return results, nil
}

// executeNextTransaction is a helper function for ExecuteBlock and ExecuteNextTransaction that
// executes the next transaction in the pending block.
func (b *Blockchain) executeNextTransaction(ctx fvm.Context) (*types.TransactionResult, error) {
	// check if there are remaining txs to be executed
	if b.pendingBlock.ExecutionComplete() {
		return nil, &PendingBlockTransactionsExhaustedError{
			BlockID: b.pendingBlock.ID(),
		}
	}

	// use the computer to execute the next transaction
	tp, err := b.pendingBlock.ExecuteNextTransaction(
		func(
			ledgerView state.View,
			txIndex uint32,
			txBody *flowgo.TransactionBody,
		) (*fvm.TransactionProcedure, error) {
			tx := fvm.Transaction(txBody, txIndex)

			err := b.vm.Run(ctx, tx, ledgerView)
			if err != nil {
				return nil, err
			}
			return tx, nil
		},
	)
	if err != nil {
		// fail fast if fatal error occurs
		return nil, err
	}

	tr, err := convert.VMTransactionResultToEmulator(tp)
	if err != nil {
		// fail fast if fatal error occurs
		return nil, err
	}

	// if transaction error exist try to further debug what was the problem
	if tr.Error != nil {
		tr.Debug = b.debugSignatureError(tr.Error, tp.Transaction)
	}

	return tr, nil
}

func (b *Blockchain) commitBlock() (*flowgo.Block, error) {
	// pending block cannot be committed before execution starts (unless empty)
	if !b.pendingBlock.ExecutionStarted() && !b.pendingBlock.Empty() {
		return nil, &PendingBlockCommitBeforeExecutionError{BlockID: b.pendingBlock.ID()}
	}

	// pending block cannot be committed before execution completes
	if b.pendingBlock.ExecutionStarted() && !b.pendingBlock.ExecutionComplete() {
		return nil, &PendingBlockMidExecutionError{BlockID: b.pendingBlock.ID()}
	}

	block := b.pendingBlock.Block()
	collections := b.pendingBlock.Collections()
	transactions := b.pendingBlock.Transactions()
	transactionResults, err := convertToSealedResults(b.pendingBlock.TransactionResults())
	if err != nil {
		return nil, err
	}
	ledgerDelta := b.pendingBlock.LedgerDelta()
	events := b.pendingBlock.Events()

	// commit the pending block to storage
	err = b.storage.CommitBlock(context.Background(), *block, collections, transactions, transactionResults, ledgerDelta, events)
	if err != nil {
		return nil, err
	}

	ledgerView := b.storage.LedgerViewByHeight(context.Background(), block.Header.Height)

	// reset pending block using current block and ledger state
	b.pendingBlock = newPendingBlock(block, ledgerView)

	return block, nil
}

// CommitBlock seals the current pending block and saves it to storage.
//
// This function clears the pending transaction pool and resets the pending block.
func (b *Blockchain) CommitBlock() (*flowgo.Block, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	block, err := b.commitBlock()
	if err != nil {
		return nil, err
	}

	b.logger.Debug().Fields(map[string]any{
		"blockHeight": block.Header.Height,
		"blockID":     block.Header.ID().String(),
	}).Msgf("📦  Block #%d committed", block.Header.Height)

	return block, nil
}

func (b *Blockchain) GetAccountStorage(address flowgo.Address) (*AccountStorage, error) {
	view := b.pendingBlock.ledgerView.NewChild()

	stateParameters := state.DefaultParameters().
		WithMaxKeySizeAllowed(b.vmCtx.MaxStateKeySize).
		WithMaxValueSizeAllowed(b.vmCtx.MaxStateValueSize)

	derivedBlockData := derived.NewEmptyDerivedBlockData()
	derivedTxnData, err := derivedBlockData.NewSnapshotReadDerivedTransactionData(
		derived.EndOfBlockExecutionTime,
		derived.EndOfBlockExecutionTime)
	if err != nil {
		return nil, err
	}

	env := environment.NewScriptEnvironment(
		context.Background(),
		tracing.NewMockTracerSpan(),
		b.vmCtx.EnvironmentParams,
		state.NewTransactionState(
			view,
			stateParameters,
		),
		derivedTxnData,
	)

	r := b.vmCtx.Borrow(env)
	defer b.vmCtx.Return(r)
	ctx := runtime.Context{
		Interface: env,
	}

	store, inter, err := r.Storage(ctx)
	if err != nil {
		return nil, err
	}

	account, err := b.vm.GetAccount(b.vmCtx, flowgo.BytesToAddress(address.Bytes()), view)
	if err != nil {
		return nil, err
	}

	extractStorage := func(path common.PathDomain) StorageItem {
		storageMap := store.GetStorageMap(
			common.MustBytesToAddress(address.Bytes()),
			path.Identifier(),
			false)
		if storageMap == nil {
			return nil
		}

		iterator := storageMap.Iterator(nil)
		values := make(StorageItem)
		k, v := iterator.Next()
		for v != nil {
			exportedValue, err := runtime.ExportValue(v, inter, interpreter.EmptyLocationRange)
			if err != nil {
				continue // just skip errored value
			}

			values[k] = exportedValue
			k, v = iterator.Next()
		}
		return values
	}

	return NewAccountStorage(
		account,
		address,
		extractStorage(common.PathDomainPrivate),
		extractStorage(common.PathDomainPublic),
		extractStorage(common.PathDomainStorage),
	)
}

// ExecuteAndCommitBlock is a utility that combines ExecuteBlock with CommitBlock.
func (b *Blockchain) ExecuteAndCommitBlock() (*flowgo.Block, []*types.TransactionResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.executeAndCommitBlock()
}

// ExecuteAndCommitBlock is a utility that combines ExecuteBlock with CommitBlock.
func (b *Blockchain) executeAndCommitBlock() (*flowgo.Block, []*types.TransactionResult, error) {

	results, err := b.ExecuteBlock()
	if err != nil {
		return nil, nil, err
	}

	block, err := b.commitBlock()
	if err != nil {
		return nil, results, err
	}

	return block, results, nil
}

// ResetPendingBlock clears the transactions in pending block.
func (b *Blockchain) ResetPendingBlock() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	latestBlock, err := b.storage.LatestBlock(context.Background())
	if err != nil {
		return &StorageError{err}
	}

	latestLedgerView := b.storage.LedgerViewByHeight(context.Background(), latestBlock.Header.Height)

	// reset pending block using latest committed block and ledger state
	b.pendingBlock = newPendingBlock(&latestBlock, latestLedgerView)

	return nil
}

// CreateAccount submits a transaction to create a new account with the given
// account keys and contracts. The transaction is paid by the service account.
func (b *Blockchain) CreateAccount(publicKeys []*sdk.AccountKey, contracts []templates.Contract) (sdk.Address, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	serviceKey := b.ServiceKey()
	serviceAddress := sdkconvert.FlowAddressToSDK(serviceKey.Address)

	latestBlock, _, err := b.GetLatestBlock()
	if err != nil {
		return sdk.Address{}, err
	}

	tx, err := templates.CreateAccount(publicKeys, contracts, serviceAddress)
	if err != nil {
		return sdk.Address{}, err
	}

	tx.SetGasLimit(flowgo.DefaultMaxTransactionGasLimit).
		SetReferenceBlockID(sdk.Identifier(latestBlock.ID())).
		SetProposalKey(serviceAddress, serviceKey.Index, serviceKey.SequenceNumber).
		SetPayer(serviceAddress)

	signer, err := serviceKey.Signer()
	if err != nil {
		return sdk.Address{}, err
	}

	err = tx.SignEnvelope(serviceAddress, serviceKey.Index, signer)
	if err != nil {
		return sdk.Address{}, err
	}

	err = b.AddTransaction(*sdkconvert.SDKTransactionToFlow(*tx))
	if err != nil {
		return sdk.Address{}, err
	}

	_, results, err := b.executeAndCommitBlock()
	if err != nil {
		return sdk.Address{}, err
	}

	lastResult := results[len(results)-1]

	_, err = b.commitBlock()
	if err != nil {
		return sdk.Address{}, err
	}

	if !lastResult.Succeeded() {
		return sdk.Address{}, lastResult.Error
	}

	var address sdk.Address

	for _, event := range lastResult.Events {
		if event.Type == sdk.EventAccountCreated {
			address = sdk.Address(event.Value.Fields[0].(cadence.Address))
			break
		}
	}

	if address == (sdk.Address{}) {
		return sdk.Address{}, fmt.Errorf("failed to find AccountCreated event")
	}

	return address, nil
}

func convertToSealedResults(
	results map[flowgo.Identifier]IndexedTransactionResult,
) (map[flowgo.Identifier]*types.StorableTransactionResult, error) {

	output := make(map[flowgo.Identifier]*types.StorableTransactionResult)

	for id, result := range results {
		temp, err := convert.ToStorableResult(result.Transaction)
		if err != nil {
			return nil, err
		}
		output[id] = &temp
	}

	return output, nil
}

// debugSignatureError tries to unwrap error to the root and test for invalid hashing algorithms
func (b *Blockchain) debugSignatureError(err error, tx *flowgo.TransactionBody) *types.TransactionResultDebug {
	if fvmerrors.HasErrorCode(err, fvmerrors.ErrCodeInvalidEnvelopeSignatureError) {
		for _, sig := range tx.EnvelopeSignatures {
			debug := b.testAlternativeHashAlgo(sig, tx.EnvelopeMessage())
			if debug != nil {
				return debug
			}
		}
	}
	if fvmerrors.HasErrorCode(err, fvmerrors.ErrCodeInvalidPayloadSignatureError) {
		for _, sig := range tx.PayloadSignatures {
			debug := b.testAlternativeHashAlgo(sig, tx.PayloadMessage())
			if debug != nil {
				return debug
			}
		}
	}

	return types.NewTransactionInvalidSignature(tx)
}

// testAlternativeHashAlgo tries to verify the signature with alternative hashing algorithm and if
// the signature is verified returns more verbose error
func (b *Blockchain) testAlternativeHashAlgo(sig flowgo.TransactionSignature, msg []byte) *types.TransactionResultDebug {
	acc, err := b.GetAccount(sig.Address)
	if err != nil {
		return nil
	}

	key := acc.Keys[sig.KeyIndex]

	for _, algo := range []hash.HashingAlgorithm{sdkcrypto.SHA2_256, sdkcrypto.SHA3_256} {
		if key.HashAlgo == algo {
			continue // skip valid hash algo
		}

		h, _ := fvmcrypto.NewPrefixedHashing(algo, flowgo.TransactionTagString)
		valid, _ := key.PublicKey.Verify(sig.Signature, msg, h)
		if valid {
			return types.NewTransactionInvalidHashAlgo(key, acc.Address, algo)
		}
	}

	return nil
}

// ExecuteNextTransaction executes the next indexed transaction in pending block.
func (b *Blockchain) ExecuteNextTransaction() (*types.TransactionResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	header := b.pendingBlock.Block().Header
	blockContext := b.newFVMContextFromHeader(header)
	return b.executeNextTransaction(blockContext)
}

func (b *Blockchain) GetTransactionResultByIndex(id flowgo.Identifier, index uint32) (*access.TransactionResult, error) {
	results, err := b.GetTransactionResultsByBlockID(id)
	if err != nil {
		return nil, err
	}
	if uint32(len(results)) <= index {
		return nil, status.Error(codes.NotFound, "TransactionResult not found")
	}
	return results[index], nil
}

func (b *Blockchain) GetTransactionsByBlockID(id flowgo.Identifier) (result []*flowgo.TransactionBody, err error) {
	block, _, err := b.GetBlockByID(id)
	if err != nil {
		switch err.(type) {
		case NotFoundError:
			return nil, status.Error(codes.NotFound, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	collectionIDs := block.Payload.Index().CollectionIDs

	for _, collectionID := range collectionIDs {
		collection, err := b.GetCollectionByID(collectionID)
		if err != nil {
			return nil, err
		}
		for _, transactionID := range collection.Transactions {
			transaction, err := b.GetTransaction(transactionID)
			if err != nil {
				return nil, err
			}
			result = append(result, transaction)
		}
	}
	return result, nil
}

func (b *Blockchain) GetTransactionResultsByBlockID(id flowgo.Identifier) (result []*access.TransactionResult, err error) {
	block, _, err := b.GetBlockByID(id)
	if err != nil {
		switch err.(type) {
		case NotFoundError:
			return nil, status.Error(codes.NotFound, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	collectionIDs := block.Payload.Index().CollectionIDs

	for _, collectionID := range collectionIDs {
		collection, err := b.GetCollectionByID(collectionID)
		if err != nil {
			return nil, err
		}
		for _, transactionID := range collection.Transactions {
			transactionResult, err := b.GetTransactionResult(transactionID)
			if err != nil {
				return nil, err
			}
			result = append(result, transactionResult)
		}
	}
	return result, nil
}

func (b *Blockchain) SetDebugger(debugger *interpreter.Debugger) {
	b.debugger = debugger
}

func (b *Blockchain) EndDebugging() {
	b.SetDebugger(nil)
}
