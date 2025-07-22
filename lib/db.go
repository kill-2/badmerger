package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

var Registration = make(map[string]func(string) (Storage, error))

type dbWrapper struct {
	store  string
	dir    string
	db     Storage
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
	kind   string
	encode encoder
	decode decoder
}

type Storage interface {
	NewInserter() Inserter
	Iterate(*Merger, func(res map[string]any) error) error
	Close() error
}

type Inserter interface {
	Insert(keyPayload, valuePayload []byte) error
	Commit() error
}

func schemaFile(dir string) string {
	return filepath.Join(dir, "schema.json")
}

func recoverSchema(dir string) ([]Opt, error) {
	data, err := os.ReadFile(schemaFile(dir))
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}

	var schema fixedSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema: %w", err)
	}

	opts := []Opt{WithStorage(schema.Store), WithDir(dir)}
	for _, key := range schema.Keys {
		opts = append(opts, WithKey(key.Name, key.Kind))
	}
	for _, val := range schema.Values {
		opts = append(opts, WithValue(val.Name, val.Kind))
	}

	return opts, nil
}

// Open creates a new database wrapper instance with the provided options.
// It handles both new database creation and schema recovery from existing databases.
// When dir option is provided and contains a schema.json file, it will recover
// the schema configuration automatically.
func Open(opts ...Opt) (*dbWrapper, error) {
	w := &dbWrapper{}
	for _, opt := range opts {
		if err := opt(w); err != nil {
			return nil, fmt.Errorf("fail to handle option: %v", err)
		}
	}

	if w.dir != "" {
		if _, err := os.Stat(schemaFile(w.dir)); !os.IsNotExist(err) {
			recoveredOpts, err := recoverSchema(w.dir)
			if err != nil {
				return nil, fmt.Errorf("fail to recover options from %v: %v", w.dir, err)
			}
			opts = recoveredOpts
		}
	}

	return open(opts...)
}

func open(opts ...Opt) (*dbWrapper, error) {
	w := &dbWrapper{}
	for _, opt := range opts {
		if err := opt(w); err != nil {
			return nil, fmt.Errorf("fail to handle option: %v", err)
		}
	}

	if w.dir == "" {
		tmpDir, err := os.MkdirTemp("", "badmerger-")
		if err != nil {
			return nil, fmt.Errorf("fail to create db %v", err)
		}
		w.dir = tmpDir
	}

	storageBuilder, ok := Registration[w.store]
	if !ok {
		return nil, fmt.Errorf("no such storage: %v", w.store)
	}

	db, err := storageBuilder(w.dir)
	if err != nil {
		return nil, fmt.Errorf("fail to open db %v", err)
	}

	w.db = db

	w.masks = (len(w.values) / 8) + 1

	if err := w.lockSchema(); err != nil {
		return nil, fmt.Errorf("fail to lock schema: %v", err)
	}

	return w, nil
}

// WithStorage returns a configuration function that sets the storage name in dbWrapper.
// The storage name must match a registered storage implementation in the Registration map.
// This is typically used when creating a new database instance via New().
func WithStorage(name string) Opt {
	return func(w *dbWrapper) error {
		w.store = name
		return nil
	}
}

// WithDir returns a configuration function that sets the location in dbWrapper.
// This is typically used when creating a new database instance via New().
func WithDir(dir string) Opt {
	return func(w *dbWrapper) error {
		w.dir = dir
		return nil
	}
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
		w.keys = append(w.keys, key{field: field{name: name, kind: kind, encode: toBytes, decode: fromBytes}})
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
		w.values = append(w.values, value{field: field{name: name, kind: kind, encode: toBytes, decode: fromBytes}})
		return nil
	}
}

type fixedSchema struct {
	Store  string             `json:"store"`
	Keys   []fixedSchemaField `json:"keys"`
	Values []fixedSchemaField `json:"values"`
}

type fixedSchemaField struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

func (db *dbWrapper) lockSchema() error {
	schema := fixedSchema{
		Store:  db.store,
		Keys:   make([]fixedSchemaField, len(db.keys)),
		Values: make([]fixedSchemaField, len(db.values)),
	}

	for i, k := range db.keys {
		schema.Keys[i].Name = k.name
		schema.Keys[i].Kind = k.kind
	}

	for i, v := range db.values {
		schema.Values[i].Name = v.name
		schema.Values[i].Kind = v.kind
	}

	jsonData, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}

	filePath := schemaFile(db.dir)
	err = os.WriteFile(filePath, jsonData, 0644)
	if err != nil {
		return fmt.Errorf("failed to write schema file: %w", err)
	}

	return nil
}

type IterWrapper struct {
	*dbWrapper
	*Merger
}

// NewIterator initializes a new iterWrapper
func (db *dbWrapper) NewIterator() *IterWrapper {
	return &IterWrapper{
		dbWrapper: db,
		Merger: &Merger{
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
	return itW.db.Iterate(itW.Merger, fn)
}

// Destroy cleans up the database by removing all temporary files.
// This should be called when the database is no longer needed.
// Returns an error if cleanup fails.
func (db *dbWrapper) Destroy() error {
	if db.dir == "" {
		return nil
	}

	if err := os.RemoveAll(db.dir); err != nil {
		return fmt.Errorf("fail to destroy db %v", err)
	}
	return nil
}

func (db *dbWrapper) Close() error {
	return db.db.Close()
}

// Recv continuously receives records from the provided channel and writes them to the database.
// It creates a new write transaction and processes records until the channel is closed.
// Each record is added to the transaction using TxnWrapper.Add().
// The transaction is automatically committed when the channel closes (via defer).
func (db *dbWrapper) Recv(ch chan map[string]any) error {
	ins := db.db.NewInserter()
	defer ins.Commit()

	for record := range ch {
		keys, values := db.extractKeysAndValues(record)
		if err := ins.Insert(keys, values); err != nil {
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
