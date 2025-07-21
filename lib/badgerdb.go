package lib

import (
	"bytes"
	"fmt"
	"os"

	badger "github.com/dgraph-io/badger/v4"
)

type badgerDb struct {
	*badger.DB
}

func NewBadger(dir string, opts ...Opt) (*badgerDb, error) {
	var badgerOpts badger.Options
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
	return &badgerDb{DB: db}, nil
}

func (bg *badgerDb) Location() string {
	return bg.DB.Opts().Dir
}

func (bg *badgerDb) NewInserter() inserter {
	return &badgerDbTxn{
		db:  bg,
		txn: bg.DB.NewTransaction(true),
	}
}

type badgerDbTxn struct {
	db  *badgerDb
	txn *badger.Txn
}

func (bgt *badgerDbTxn) Insert(keyPayload, valuePayload []byte) error {
	if err := bgt.txn.Set(keyPayload, valuePayload); err == badger.ErrTxnTooBig {
		_ = bgt.Commit()
		bgt.txn = bgt.db.DB.NewTransaction(true)
		_ = bgt.txn.Set(keyPayload, valuePayload)
	}

	return nil
}

func (bgt *badgerDbTxn) Commit() error {
	return bgt.txn.Commit()
}

func (db *badgerDb) Iterate(m *merger, fn func(res map[string]any) error) error {
	return db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		var lastKeyMap map[string]any
		lastKeyBytes := []byte{}
		valueMaps := []map[string]any{}

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			currKeyBytes, keyMap := m.restoreKey(item.Key())
			if !bytes.Equal(lastKeyBytes, currKeyBytes) {
				if len(lastKeyBytes) > 0 {
					if err := fn(m.merge(lastKeyMap, valueMaps)); err != nil {
						return err
					}
				}
				lastKeyBytes = lastKeyBytes[:0]
				lastKeyBytes = append(lastKeyBytes, currKeyBytes...)
				lastKeyMap = keyMap
				valueMaps = valueMaps[:0]
			}

			if len(m.allValues) == 0 {
				continue
			}

			err := item.Value(func(valueBytes []byte) error {
				valueMaps = append(valueMaps, m.restoreValue(valueBytes))
				return nil
			})

			if err != nil {
				return err
			}
		}

		if err := fn(m.merge(lastKeyMap, valueMaps)); err != nil {
			return err
		}

		return nil
	})
}
