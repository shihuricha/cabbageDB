package sql

import "strconv"

type DataType byte

func (d DataType) String() string {
	switch d {
	case NullType:
		return "NULL"
	case IntType:
		return "INT"
	case StringType:
		return "VARCHAR(100)"
	case FloatType:
		return "FLOAF"
	case BoolType:
		return "BOOL"
	default:
		return ""
	}
}

type ValueData struct {
	Type  DataType
	Value any
}

func (v *ValueData) String() string {
	switch v.Type {
	case NullType:
		return "NULL"
	case FloatType:
		return strconv.FormatFloat(v.Value.(float64), 'f', 4, 64)
	case BoolType:
		if v.Value == true {
			return "TRUE"
		} else {
			return "FALSE"
		}
	case IntType:
		switch num := v.Value.(type) {
		case int:
			return strconv.Itoa(num)
		case int64:
			return strconv.Itoa(int(num))
		case uint64:
			return strconv.Itoa(int(num))
		}
		return ""
	case StringType:
		return v.Value.(string)
	}
	return ""
}

func (v *ValueData) Compare(other *ValueData) (int, bool) {
	if v.Type == other.Type && v.Type == NullType {
		return 0, true
	}
	if v.Type == NullType {
		return -1, true
	}
	if other.Type == NullType {
		return 1, true
	}
	if v.Type == other.Type && v.Type == BoolType {
		a := v.Value.(bool)
		b := other.Value.(bool)
		if a == b {
			return 0, true
		}
		if !a && b { // a 是 false，b 是 true → a < b
			return -1, true
		}
		return 1, true
	}
	if v.Type == other.Type && v.Type == StringType {
		a := v.Value.(string)
		b := other.Value.(string)
		if a == b {
			return 0, true
		}
		if a < b {
			return -1, true
		}
		return 1, true
	}
	if v.Type == other.Type && v.Type == IntType {
		a := v.Value.(int64)
		b := other.Value.(int64)
		if a == b {
			return 0, true
		}
		if a < b {
			return -1, true
		}
		return 1, true

	}
	if v.Type == other.Type && v.Type == FloatType {
		a := v.Value.(float64)
		b := other.Value.(float64)
		if a == b {
			return 0, true
		}
		if a < b {
			return -1, true
		}
		return 1, true
	}
	if (v.Type == FloatType && other.Type == IntType) || (v.Type == IntType && other.Type == FloatType) {
		a := v.Value.(float64)
		b := other.Value.(float64)
		if a == b {
			return 0, true
		}
		if a < b {
			return -1, true
		}
		return 1, true
	}
	return 0, false
}

const (
	NullType   DataType = 0x01
	BoolType   DataType = 0x02
	IntType    DataType = 0x03
	FloatType  DataType = 0x04
	StringType DataType = 0x05
)

func DecodeDataType(v interface{}) DataType {
	switch v.(type) {
	case int, int64, int8, int16, int32:
		return IntType
	case bool:
		return BoolType
	case string:
		return StringType
	case float64, float32:
		return FloatType
	case nil:
		return NullType
	}
	return StringType
}

func EqualValue(data1 *ValueData, data2 *ValueData) bool {
	if data1 == nil || data2 == nil {
		return false
	}
	if data1.Type != data2.Type {
		return false
	}
	if data1.String() != data2.String() {
		return false
	}
	return true
}
