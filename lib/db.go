package lib

import (
	"bytes"
	"fmt"
	"os"

	badger "github.com/dgraph-io/badger/v4"
)

type dbWrapper struct {
	loc    string
	db     *badger.DB
	keys   []key
	values []value
	masks  int
}

type Opt func(w *dbWrapper) error

type key struct {
	field
}

type value struct {
	field
}

type field struct {
	name   string
	encode encoder
	decode decoder
}

// New creates a new dbWrapper instance with optional configuration.
// It initializes a temporary BadgerDB database (in memory if dir is '?') and applies any provided options.
// Returns the dbWrapper instance or an error if initialization fails.
func New(dir string, opts ...Opt) (*dbWrapper, error) {
	var (
		badgerOpts badger.Options
	)
	if dir == "?" {
		badgerOpts = badger.DefaultOptions("").WithInMemory(true)
	} else {
		tmpDir, err := os.MkdirTemp(dir, "badmerger-")
		if err != nil {
			return nil, fmt.Errorf("fail to create db %v", err)
		}
		badgerOpts = badger.DefaultOptions(tmpDir)
	}

	badgerOpts.Logger = nil
	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("fail to open db %v", err)
	}

	w := &dbWrapper{
		loc: badgerOpts.Dir,
		db:  db,
	}

	for _, opt := range opts {
		if err := opt(w); err != nil {
			return nil, fmt.Errorf("fail to handle option: %v", err)
		}
	}

	w.masks = (len(w.values) / 8) + 1

	return w, nil
}

// WithKey returns a configuration function that adds a key field to the dbWrapper.
// The key consists of a name and type (e.g., "id", "int32").
// This is used to define the structure of keys in the database.
func WithKey(name, kind string) Opt {
	return func(w *dbWrapper) error {
		if w.keys == nil {
			w.keys = make([]key, 0)
		}
		toBytes, fromBytes, err := chooseEncoder(kind)
		if err != nil {
			return err
		}
		w.keys = append(w.keys, key{field: field{name: name, encode: toBytes, decode: fromBytes}})
		return nil
	}
}

// WithValue returns a configuration function that adds a value field to the dbWrapper.
// The value consists of a name and type (e.g., "name", "string").
// This is used to define the structure of values in the database.
func WithValue(name, kind string) Opt {
	return func(w *dbWrapper) error {
		if w.values == nil {
			w.values = make([]value, 0)
		}
		toBytes, fromBytes, err := chooseEncoder(kind)
		if err != nil {
			return err
		}
		w.values = append(w.values, value{field: field{name: name, encode: toBytes, decode: fromBytes}})
		return nil
	}
}

type IterWrapper struct {
	*dbWrapper
	*merger
}

// NewIterator initializes a new iterWrapper
func (db *dbWrapper) NewIterator() *IterWrapper {
	return &IterWrapper{
		dbWrapper: db,
		merger: &merger{
			masks:     db.masks,
			allValues: db.values,
		},
	}
}

// WithPartialKey adds a key field to the partial keys list for iteration.
// name: The name of the key field to include in partial key extraction
// Returns the iterWrapper for method chaining, or nil if the key name is not found
func (itW *IterWrapper) WithPartialKey(name string) *IterWrapper {
	for _, k := range itW.keys {
		if k.name == name {
			itW.partialKeys = append(itW.partialKeys, k)
			return itW
		}
	}
	return nil
}

// WithAgg adds an aggregation operation to the iterator.
// name: The field name after aggregation
// op: The aggregation operation to perform
// Returns the iterWrapper for method chaining
func (itW *IterWrapper) WithAgg(name, op string) *IterWrapper {
	itW.aggs = append(itW.aggs, namedAggregation{name: name, aggregator: chooseAggregator(op)})
	return itW
}

// Iter executes the iteration over the BadgerDB keyspace, applying any configured
// aggregations and calling the provided callback for each result.
// fn: Callback function that receives each aggregated result map
// Returns error if any iteration or aggregation operation fails
func (itW *IterWrapper) Iter(fn func(res map[string]any) error) error {
	return itW.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		var lastKeyMap map[string]any
		lastKeyBytes := []byte{}
		valueMaps := []map[string]any{}

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			currKeyBytes, keyMap := itW.restoreKey(item.Key())
			if !bytes.Equal(lastKeyBytes, currKeyBytes) {
				if len(lastKeyBytes) > 0 {
					if err := fn(itW.merge(lastKeyMap, valueMaps)); err != nil {
						return err
					}
				}
				lastKeyBytes = lastKeyBytes[:0]
				lastKeyBytes = append(lastKeyBytes, currKeyBytes...)
				lastKeyMap = keyMap
				valueMaps = valueMaps[:0]
			}

			if len(itW.allValues) == 0 {
				continue
			}

			err := item.Value(func(valueBytes []byte) error {
				valueMaps = append(valueMaps, itW.restoreValue(valueBytes))
				return nil
			})

			if err != nil {
				return err
			}
		}

		if err := fn(itW.merge(lastKeyMap, valueMaps)); err != nil {
			return err
		}

		return nil
	})
}

// Destroy cleans up the database by removing all temporary files.
// This should be called when the database is no longer needed.
// Returns an error if cleanup fails.
func (db *dbWrapper) Destroy() error {
	if db.loc == "" {
		return nil
	}

	if err := os.RemoveAll(db.loc); err != nil {
		return fmt.Errorf("fail to destroy db %v", err)
	}
	return nil
}

type TxnWrapper struct {
	txn *badger.Txn
	dbW *dbWrapper
}

// Recv continuously receives records from the provided channel and writes them to the database.
// It creates a new write transaction and processes records until the channel is closed.
// Each record is added to the transaction using TxnWrapper.Add().
// The transaction is automatically committed when the channel closes (via defer).
func (db *dbWrapper) Recv(ch chan map[string]any) error {
	txn := db.db.NewTransaction(true)
	w := &TxnWrapper{txn: txn, dbW: db}
	defer w.Commit()

	for record := range ch {
		if err := w.Add(record); err != nil {
			return err
		}
	}
	return nil
}

// Commit finalizes the current transaction.
// This is called automatically by dbWrapper.Add but can be used manually if needed.
func (txn *TxnWrapper) Commit() {
	txn.txn.Commit()
}

// Add inserts a new record into the database within the current transaction.
// The record is a map of field names to values.
// Returns an error if the record cannot be added.
// Automatically handles transaction size limits by committing and starting a new transaction if needed.
func (txn *TxnWrapper) Add(record map[string]any) error {
	keyPayload := make([]byte, 0)
	for _, f := range txn.dbW.keys {
		fieldValue := record[f.name]
		fieldValueBin := f.encode(fieldValue)
		keyPayload = append(keyPayload, fieldValueBin...)
		delete(record, f.name)
	}

	var valuePayload []byte
	if len(txn.dbW.values) > 0 {
		valuePayload = make([]byte, txn.dbW.masks)
		for i, f := range txn.dbW.values {
			fieldValue, ok := record[f.name]
			if !ok || (fieldValue == nil) {
				valuePayload[i/8] |= (1 << (7 - (i % 8)))
				continue
			}
			fieldValueBin := f.encode(fieldValue)
			valuePayload = append(valuePayload, fieldValueBin...)
		}
	}

	if err := txn.txn.Set(keyPayload, valuePayload); err == badger.ErrTxnTooBig {
		_ = txn.txn.Commit()
		txn.txn = txn.dbW.db.NewTransaction(true)
		_ = txn.txn.Set(keyPayload, valuePayload)
	}

	return nil
}
