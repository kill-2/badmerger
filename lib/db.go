package lib

import (
	"fmt"
	"os"
)

type dbWrapper struct {
	loc    string
	db     storage
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

type storage interface {
	NewTransaction() transaction
	NewIterator() iterator
	Location() string
}

type transaction interface {
	Add(keyPayload, valuePayload []byte) error
	Commit() error
}

type iterator interface {
	Iter(*merger, func(res map[string]any) error) error
}

// New creates a new dbWrapper instance with optional configuration.
// It initializes a temporary BadgerDB database (in memory if dir is '?') and applies any provided options.
// Returns the dbWrapper instance or an error if initialization fails.
func New(dir string, opts ...Opt) (*dbWrapper, error) {
	db, err := NewBadger(dir, opts...)
	if err != nil {
		return nil, fmt.Errorf("fail to open db %v", err)
	}

	w := &dbWrapper{
		loc: db.Location(),
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
	return itW.db.NewIterator().Iter(itW.merger, fn)
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

// Recv continuously receives records from the provided channel and writes them to the database.
// It creates a new write transaction and processes records until the channel is closed.
// Each record is added to the transaction using TxnWrapper.Add().
// The transaction is automatically committed when the channel closes (via defer).
func (db *dbWrapper) Recv(ch chan map[string]any) error {
	txn := db.db.NewTransaction()
	defer txn.Commit()

	for record := range ch {
		keys, values := db.extractKeysAndValues(record)
		if err := txn.Add(keys, values); err != nil {
			return err
		}
	}
	return nil
}

func (dbW *dbWrapper) extractKeysAndValues(record map[string]any) ([]byte, []byte) {
	keyPayload := make([]byte, 0)
	for _, f := range dbW.keys {
		fieldValue := record[f.name]
		fieldValueBin := f.encode(fieldValue)
		keyPayload = append(keyPayload, fieldValueBin...)
		delete(record, f.name)
	}

	var valuePayload []byte
	if len(dbW.values) > 0 {
		valuePayload = make([]byte, dbW.masks)
		for i, f := range dbW.values {
			fieldValue, ok := record[f.name]
			if !ok || (fieldValue == nil) {
				valuePayload[i/8] |= (1 << (7 - (i % 8)))
				continue
			}
			fieldValueBin := f.encode(fieldValue)
			valuePayload = append(valuePayload, fieldValueBin...)
		}
	}

	return keyPayload, valuePayload
}
