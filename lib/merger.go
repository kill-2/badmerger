package lib

type merger struct {
	masks       int
	partialKeys []key
	allValues   []value
	aggs        []namedAggregation
}

type namedAggregation struct {
	name string
	aggregator
}

// restoreKey decodes the keyBytes into a map of field names to their decoded values.
// It returns the original key bytes up to the offset that was processed and a map
// containing all the decoded key fields with their names as map keys.
func (m *merger) restoreKey(keyBytes []byte) ([]byte, map[string]any) {
	keyMap := make(map[string]any, len(m.partialKeys))
	keyOffset := 0
	for _, k := range m.partialKeys {
		var keyData any
		keyData, kStep := k.decode(keyBytes[keyOffset:])
		keyOffset += kStep
		keyMap[k.name] = keyData
	}

	currKeyBytes := keyBytes[:keyOffset]
	return currKeyBytes, keyMap
}

// restoreValue decodes the valueBytes into a map of field names to their decoded values.
// It handles masked fields (where bits in valueHead indicate if a field should be skipped)
// and returns a map containing all the decoded value fields with their names as map keys.
func (m *merger) restoreValue(valueBytes []byte) map[string]any {
	valueHead := valueBytes[:m.masks]
	valueBody := valueBytes[m.masks:]
	valueMap := make(map[string]any, len(m.allValues))
	offset := 0
	for i, f := range m.allValues {
		if (valueHead[i/8] & (1 << (7 - (i % 8)))) != 0 {
			continue
		}
		var valueData any
		valueData, step := f.decode(valueBody[offset:])
		valueMap[f.name] = valueData
		offset += step
	}
	return valueMap
}

// merge combines the key fields with aggregated values from multiple value maps.
// It applies each aggregation function in m.aggs to the valueValues and stores
// the results in the keyValue map using the aggregation names as keys.
// Returns the merged map containing both original key fields and aggregated values.
func (m *merger) merge(keyValue map[string]any, valueValues []map[string]any) map[string]any {
	for _, agg := range m.aggs {
		keyValue[agg.name] = agg.on(valueValues)
	}
	return keyValue
}
