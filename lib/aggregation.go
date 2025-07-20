package lib

import "strings"

type aggregator interface {
	on(collection []map[string]any) any
}

func chooseAggregator(op string) aggregator {
	var operator aggregator
	if strings.HasPrefix(op, "first(") {
		operator = first{name: strings.Replace(strings.Replace(op, "first(", "", -1), ")", "", -1)}

	} else if strings.HasPrefix(op, "first_not_null(") {
		operator = firstNotNull{name: strings.Replace(strings.Replace(op, "first_not_null(", "", -1), ")", "", -1)}
	}
	return operator
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
