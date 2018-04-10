package driver

import (
	"fmt"
	"strconv"
	. "github.com/woqutech/drt/tools"
)

type Result struct {
	Status       uint16
	InsertId     uint64
	AffectedRows uint64
	*ResultSet
}

type ResultSet struct {
	Fields     []*FieldPacket
	FieldNames map[string]int
	Values     [][]interface{}
	RowDatas   []RowData
}

type RowData []byte

func (r *ResultSet) GetString(row, column int) (string, error) {
	d, err := r.GetValue(row, column)
	if err != nil {
		return "", err
	}

	switch v := d.(type) {
	case string:
		return v, nil
	case []byte:
		return UnsafeGetString(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case nil:
		return "", nil
	default:
		return "", fmt.Errorf("data type is %T", v)
	}
}

func (r *ResultSet) GetInt64(row, column int) (int64, error) {
	v, err := r.GetUint64(row, column)
	if err != nil {
		return 0, err
	}

	return int64(v), nil
}

func (r *ResultSet) GetUint64(row, column int) (uint64, error) {
	d, err := r.GetValue(row, column)
	if err != nil {
		return 0, err
	}

	switch v := d.(type) {
	case uint64:
		return v, nil
	case int64:
		return uint64(v), nil
	case float64:
		return uint64(v), nil
	case string:
		return strconv.ParseUint(v, 10, 64)
	case []byte:
		return strconv.ParseUint(string(v), 10, 64)
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("data type is %T", v)
	}
}

func (r *ResultSet) GetValue(row, column int) (interface{}, error) {
	if row >= len(r.Values) || row < 0 {
		return nil, fmt.Errorf("invalid row index %d", row)
	}

	if column >= len(r.Fields) || column < 0 {
		return nil, fmt.Errorf("invalid column index %d", column)
	}

	return r.Values[row][column], nil
}

func (p RowData) parse(f []*FieldPacket, binary bool) ([]interface{}, error) {
	if binary {
		return p.parseBinary(f)
	} else {
		return p.parseText(f)
	}
}

// http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
func (p RowData) parseText(fields []*FieldPacket) ([]interface{}, error) {
	b := make([]interface{}, len(fields))

	var (
		err    error
		v      []byte
		isNull bool
	)

	i := 0
	n := 0

	for fieldIdx := range fields {
		v, isNull, n, err = ReadLengthEncodedString(p[i:])
		if err != nil {
			return nil, err
		}

		i = i + n

		if isNull {
			b[fieldIdx] = nil
		} else {
			isUnsigned := fields[fieldIdx].Flag&UNSIGNED_FLAG != 0

			switch fields[fieldIdx].Type {
			case MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT, MYSQL_TYPE_INT24,
				MYSQL_TYPE_LONGLONG, MYSQL_TYPE_YEAR:
				if isUnsigned {
					b[fieldIdx], err = strconv.ParseUint(string(v), 10, 64)
				} else {
					b[fieldIdx], err = strconv.ParseInt(string(v), 10, 64)
				}
				continue
			case MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE:
				b[fieldIdx], err = strconv.ParseFloat(string(v), 64)
				continue
			default:
				b[fieldIdx] = v
			}

			if err != nil {
				return nil, err
			}
		}
	}

	return b, nil
}

// http://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html
func (p RowData) parseBinary(f []*FieldPacket) ([]interface{}, error) {
	b := make([]interface{}, len(f))

	// packet indicator [1 byte]
	if p[0] != OK_HEADER {
		return nil, ErrMalformPkt
	}

	// NULL-bitmap,  [(column-count + 7 + 2) / 8 bytes]
	pos := 1 + ((len(f) + 7 + 2) >> 3)
	nullBitmap := p[1:pos]

	var isNull bool
	var n int
	var err error
	var v []byte
	for i := range b {
		// Field is NULL
		// (byte >> bit-pos) % 2 == 1
		if nullBitmap[(i+2)/8]&(1<<(uint(i+2)%8)) > 0 {
			b[i] = nil
			continue
		}

		isUnsigned := f[i].Flag&UNSIGNED_FLAG != 0
		// Convert to byte-coded string
		switch f[i].Type {
		case MYSQL_TYPE_NULL:
			b[i] = nil
			continue

			// Numeric Types
		case MYSQL_TYPE_TINY:
			if isUnsigned {
				b[i] = ReadBinaryUint8(p[pos : pos+1])
			} else {
				b[i] = ReadBinaryInt8(p[pos : pos+1])
			}
			pos++
			continue

		case MYSQL_TYPE_SHORT, MYSQL_TYPE_YEAR:
			if isUnsigned {
				b[i] = ReadBinaryUint16(p[pos : pos+2])
			} else {
				b[i] = ReadBinaryInt16(p[pos : pos+2])
			}
			pos += 2
			continue

		case MYSQL_TYPE_INT24:
			if isUnsigned {
				b[i] = ReadBinaryUint24(p[pos : pos+3])
			} else {
				b[i] = ReadBinaryInt24(p[pos : pos+3])
			}
			pos += 4
			continue

		case MYSQL_TYPE_LONG:
			if isUnsigned {
				b[i] = ReadBinaryUint32(p[pos : pos+4])
			} else {
				b[i] = ReadBinaryInt32(p[pos : pos+4])
			}
			pos += 4
			continue

		case MYSQL_TYPE_LONGLONG:
			if isUnsigned {
				b[i] = ReadBinaryUint64(p[pos : pos+8])
			} else {
				b[i] = ReadBinaryInt64(p[pos : pos+8])
			}
			pos += 8
			continue

		case MYSQL_TYPE_FLOAT:
			b[i] = ReadBinaryFloat32(p[pos : pos+4])
			pos += 4
			continue

		case MYSQL_TYPE_DOUBLE:
			b[i] = ReadBinaryFloat64(p[pos : pos+4])
			pos += 8
			continue

			// Length coded Binary Strings
		case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL, MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_BIT, MYSQL_TYPE_ENUM, MYSQL_TYPE_SET, MYSQL_TYPE_TINY_BLOB,
			MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_BLOB,
			MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING, MYSQL_TYPE_GEOMETRY:
			v, isNull, n, err = ReadLengthEncodedString(p[pos:])
			pos += n
			if err != nil {
				return nil, err
			}

			if !isNull {
				b[i] = v
				continue
			} else {
				b[i] = nil
				continue
			}
		case MYSQL_TYPE_DATE, MYSQL_TYPE_NEWDATE:
			var num uint64
			num, isNull, n = ReadLengthEncodedInteger(p[pos:])

			pos = pos + n

			if isNull {
				b[i] = nil
				continue
			}

			b[i], err = FormatBinaryDate(int(num), p[pos:])
			pos += int(num)

			if err != nil {
				return nil, err
			}

		case MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_DATETIME:
			var num uint64
			num, isNull, n = ReadLengthEncodedInteger(p[pos:])

			pos += n

			if isNull {
				b[i] = nil
				continue
			}

			b[i], err = FormatBinaryDateTime(int(num), p[pos:])
			pos += int(num)

			if err != nil {
				return nil, err
			}

		case MYSQL_TYPE_TIME:
			var num uint64
			num, isNull, n = ReadLengthEncodedInteger(p[pos:])

			pos += n

			if isNull {
				b[i] = nil
				continue
			}

			b[i], err = FormatBinaryTime(int(num), p[pos:])
			pos += int(num)

			if err != nil {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("stmt unknown field type %d %s", f[i].Type, f[i].Name)
		}
	}

	return b, nil
}
