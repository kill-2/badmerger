package lib

import (
	"fmt"
	"strings"
)

type aggregator interface {
	on(collection []map[string]any) any
}

func chooseAggregator(op string) aggregator {
	var operator aggregator
	if strings.HasPrefix(op, "first(") {
		operator = first{name: strings.ReplaceAll(strings.ReplaceAll(op, "first(", ""), ")", "")}
	} else if strings.HasPrefix(op, "first_not_null(") {
		operator = firstNotNull{name: strings.ReplaceAll(strings.ReplaceAll(op, "first_not_null(", ""), ")", "")}
	} else if strings.HasPrefix(op, "sum(") {
		operator = sum{name: strings.ReplaceAll(strings.ReplaceAll(op, "sum(", ""), ")", "")}
	} else if strings.HasPrefix(op, "count(") {
		operator = count{name: strings.ReplaceAll(strings.ReplaceAll(op, "count(", ""), ")", "")}
	} else if strings.HasPrefix(op, "count_distinct(") {
		operator = countDistinct{name: strings.ReplaceAll(strings.ReplaceAll(op, "count_distinct(", ""), ")", "")}
	} else if strings.HasPrefix(op, "tally(") {
		operator = tally{name: strings.ReplaceAll(strings.ReplaceAll(op, "tally(", ""), ")", "")}
	} else if strings.HasPrefix(op, "min(") {
		operator = min{name: strings.ReplaceAll(strings.ReplaceAll(op, "min(", ""), ")", "")}
	} else if strings.HasPrefix(op, "max(") {
		operator = max{name: strings.ReplaceAll(strings.ReplaceAll(op, "max(", ""), ")", "")}
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

type min struct {
	name string
}

func (a min) on(collection []map[string]any) any {
	if len(collection) == 0 {
		return nil
	}
	var minVal int64
	first := true
	for _, item := range collection {
		if val, ok := item[a.name]; ok {
			switch v := val.(type) {
			case int8:
				if first || int64(v) < minVal {
					minVal = int64(v)
					first = false
				}
			case int16:
				if first || int64(v) < minVal {
					minVal = int64(v)
					first = false
				}
			case int32:
				if first || int64(v) < minVal {
					minVal = int64(v)
					first = false
				}
			case int64:
				if first || v < minVal {
					minVal = v
					first = false
				}
			case int:
				if first || int64(v) < minVal {
					minVal = int64(v)
					first = false
				}
			default:
				continue
			}
		}
	}
	if first {
		return nil
	}
	return minVal
}

type max struct {
	name string
}

func (a max) on(collection []map[string]any) any {
	if len(collection) == 0 {
		return nil
	}
	var maxVal int64
	first := true
	for _, item := range collection {
		if val, ok := item[a.name]; ok {
			switch v := val.(type) {
			case int8:
				if first || int64(v) > maxVal {
					maxVal = int64(v)
					first = false
				}
			case int16:
				if first || int64(v) > maxVal {
					maxVal = int64(v)
					first = false
				}
			case int32:
				if first || int64(v) > maxVal {
					maxVal = int64(v)
					first = false
				}
			case int64:
				if first || v > maxVal {
					maxVal = v
					first = false
				}
			case int:
				if first || int64(v) > maxVal {
					maxVal = int64(v)
					first = false
				}
			default:
				continue
			}
		}
	}
	if first {
		return nil
	}
	return maxVal
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

type countDistinct struct {
	name string
}

func (a countDistinct) on(collection []map[string]any) any {
	seen := make(map[any]struct{})
	for _, item := range collection {
		if val, ok := item[a.name]; ok && val != nil {
			seen[val] = struct{}{}
		}
	}
	return int64(len(seen))
}

type tally struct {
	name string
}

func (a tally) on(collection []map[string]any) any {
	seen := make(map[string]int64)
	for _, item := range collection {
		if val, ok := item[a.name]; ok && val != nil {
			valStr := fmt.Sprintf("%v", val)
			times, saw := seen[valStr]
			if !saw {
				times = 0
			}
			seen[valStr] = (times + 1)
		}
	}
	return seen
}
