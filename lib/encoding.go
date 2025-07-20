package lib

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

type encoder func(anyNum any) []byte
type decoder func(b []byte) (any, int)

func chooseEncoder(kind string) (encoder, decoder, error) {
	switch kind {
	case "int8":
		return toInt8Binary, fromInt8Binary, nil
	case "int16":
		return toInt16Binary, fromInt16Binary, nil
	case "int32":
		return toInt32Binary, fromInt32Binary, nil
	case "int64":
		return toInt64Binary, fromInt64Binary, nil
	case "string":
		return toStringBinary, fromStringBinary, nil
	case "json":
		return toJsonBinary, fromJsonBinary, nil
	}

	return nil, nil, fmt.Errorf("can not encode %s", kind)
}

func toInt8Binary(anyNum any) []byte {
	var num uint8
	switch v := anyNum.(type) {
	case float64:
		num = uint8(v)
	case float32:
		num = uint8(v)
	case int:
		num = uint8(v)
	case int64:
		num = uint8(v)
	case int32:
		num = uint8(v)
	case int16:
		num = uint8(v)
	case int8:
		num = uint8(v)
	default:
		num = uint8(0)
	}
	b := make([]byte, 1)
	b[0] = byte(num)
	return b
}

func fromInt8Binary(b []byte) (any, int) {
	return int8(b[0]), 1
}

func toInt16Binary(anyNum any) []byte {
	var num uint16
	switch v := anyNum.(type) {
	case float64:
		num = uint16(v)
	case float32:
		num = uint16(v)
	case int:
		num = uint16(v)
	case int64:
		num = uint16(v)
	case int32:
		num = uint16(v)
	case int16:
		num = uint16(v)
	case int8:
		num = uint16(v)
	default:
		num = uint16(0)
	}
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, num)
	return b
}

func fromInt16Binary(b []byte) (any, int) {
	return int16(binary.BigEndian.Uint16(b)), 2
}

func toInt32Binary(anyNum any) []byte {
	var num uint32
	switch v := anyNum.(type) {
	case float64:
		num = uint32(v)
	case float32:
		num = uint32(v)
	case int:
		num = uint32(v)
	case int64:
		num = uint32(v)
	case int32:
		num = uint32(v)
	case int16:
		num = uint32(v)
	case int8:
		num = uint32(v)
	default:
		num = uint32(0)
	}
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, num)
	return b
}

func fromInt32Binary(b []byte) (any, int) {
	return int32(binary.BigEndian.Uint32(b)), 4
}

func toInt64Binary(anyNum any) []byte {
	var num uint64
	switch v := anyNum.(type) {
	case float64:
		num = uint64(v)
	case float32:
		num = uint64(v)
	case int:
		num = uint64(v)
	case int64:
		num = uint64(v)
	case int32:
		num = uint64(v)
	case int16:
		num = uint64(v)
	case int8:
		num = uint64(v)
	default:
		num = uint64(0)
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, num)
	return b
}

func fromInt64Binary(b []byte) (any, int) {
	return int64(binary.BigEndian.Uint64(b)), 8
}

func toStringBinary(anyNum any) []byte {
	var str string
	switch v := anyNum.(type) {
	case string:
		str = v
	default:
		str = ""
	}
	body := []byte(str)
	header := toInt16Binary(len(body))
	return append(header, body...)
}

func fromStringBinary(b []byte) (any, int) {
	l, _ := fromInt16Binary(b[:2])
	limit := 2 + l.(int16)
	return string(b[2:limit]), int(limit)
}

func toJsonBinary(anyValue any) []byte {
	body, _ := json.Marshal(anyValue)
	header := toInt16Binary(len(body))
	return append(header, body...)
}

func fromJsonBinary(b []byte) (any, int) {
	l, _ := fromInt16Binary(b[:2])
	limit := 2 + l.(int16)
	var anyValue any
	json.Unmarshal(b[2:limit], &anyValue)
	return anyValue, int(limit)
}
