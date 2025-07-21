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
	} else if strings.HasPrefix(op, "sum(") {
		operator = sum{name: strings.Replace(strings.Replace(op, "sum(", "", -1), ")", "", -1)}
	} else if strings.HasPrefix(op, "count(") {
		operator = count{name: strings.Replace(strings.Replace(op, "count(", "", -1), ")", "", -1)}
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

type sum struct {
	name string
}

func (a sum) on(collection []map[string]any) any {
	var total int64
	for _, item := range collection {
		if val, ok := item[a.name]; ok {
			switch v := val.(type) {
			case int8:
				total += int64(v)
			case int16:
				total += int64(v)
			case int32:
				total += int64(v)
			case int64:
				total += v
			case int:
				total += int64(v)
			default:
				continue
			}
		}
	}
	return total
}

type count struct {
	name string
}

func (a count) on(collection []map[string]any) any {
	var total int64
	for _, item := range collection {
		if _, ok := item[a.name]; ok {
			total += 1
		}
	}
	return total
}
