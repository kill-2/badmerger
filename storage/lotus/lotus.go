package lotus

import (
	"bytes"
	"fmt"

	"github.com/kill-2/badmerger/lib"
	"github.com/lotusdblabs/lotusdb/v2"
)

func init() {
	lib.Registration["lotus"] = NewLotus
}

type lotusDb struct {
	*lotusdb.DB
}

func NewLotus(dir string, opts ...lib.Opt) (lib.Storage, error) {

	lotusOpts := lotusdb.DefaultOptions
	lotusOpts.DirPath = dir

	db, err := lotusdb.Open(lotusOpts)
	if err != nil {
		return nil, fmt.Errorf("fail to open db %v", err)
	}
	return &lotusDb{DB: db}, nil
}

func (ld *lotusDb) NewInserter() lib.Inserter {
	return &lotusDbTxn{
		db:    ld,
		batch: ld.DB.NewBatch(lotusdb.DefaultBatchOptions),
	}
}

type lotusDbTxn struct {
	db    *lotusDb
	batch *lotusdb.Batch
}

func (lt *lotusDbTxn) Insert(keyPayload, valuePayload []byte) error {
	if err := lt.batch.Put(keyPayload, valuePayload); err != nil {
		_ = lt.Commit()
		lt.batch = lt.db.DB.NewBatch(lotusdb.DefaultBatchOptions)
		return lt.batch.Put(keyPayload, valuePayload)
	}
	return nil
}

func (lt *lotusDbTxn) Commit() error {
	return lt.batch.Commit()
}

func (db *lotusDb) Iterate(m *lib.Merger, fn func(res map[string]any) error) error {
	iter, _ := db.DB.NewIterator(lotusdb.IteratorOptions{})
	defer iter.Close()

	var lastKeyMap map[string]any
	lastKeyBytes := []byte{}
	valueMaps := []map[string]any{}

	for iter.Rewind(); iter.Valid(); iter.Next() {
		currKeyBytes, keyMap := m.RestoreKey(iter.Key())
		if !bytes.Equal(lastKeyBytes, currKeyBytes) {
			if len(lastKeyBytes) > 0 {
				if err := fn(m.Merge(lastKeyMap, valueMaps)); err != nil {
					return err
				}
			}
			lastKeyBytes = lastKeyBytes[:0]
			lastKeyBytes = append(lastKeyBytes, currKeyBytes...)
			lastKeyMap = keyMap
			valueMaps = valueMaps[:0]
		}

		if m.NoValue() {
			continue
		}

		valueMaps = append(valueMaps, m.RestoreValue(iter.Value()))
	}

	if err := fn(m.Merge(lastKeyMap, valueMaps)); err != nil {
		return err
	}

	return nil
}
