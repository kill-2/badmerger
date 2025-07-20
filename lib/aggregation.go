package lib

type aggregator interface {
	on(collection []map[string]any) any
}

type first struct {
	name string
}

func (a first) on(collection []map[string]any) any {
	if len(collection) == 0 {
		return nil
	}
	return collection[0][a.name]
}

type firstNotNull struct {
	name string
}

func (a firstNotNull) on(collection []map[string]any) any {
	for _, v := range collection {
		if v0, ok := v[a.name]; ok && (v0 != nil) {
			return v0
		}
	}
	return nil
}
