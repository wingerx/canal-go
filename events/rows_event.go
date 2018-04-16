package events

import (
	"encoding/binary"
	"fmt"
	log "github.com/golang/glog"
	"github.com/juju/errors"
	. "github.com/woqutech/drt/tools"
	"strconv"
	"time"
)

type RowsEvent struct {
	Table         *TableMapEvent
	TableID       uint64
	Flags         uint16
	ColumnCount   uint64
	ColumnBitmap1 []byte          //len = (ColumnCount + 7) / 8
	ColumnBitmap2 []byte          //if UPDATE_ROWS_EVENT_V1 or v2, len = (ColumnCount + 7) / 8
	Rows          [][]interface{} //rows: invalid: int64, float64, bool, []byte, string
	ExtraData     []byte

	useDecimal bool
}

// Payload is structured as follows for MySQL v5.5:
//   19 bytes for common v4 event header
//   6 bytes (uint64) for table id
//   2 bytes (uint16) for flags
//   1 to 9 bytes (net_store_length variable encoded uint64), Z, for total
//     number of columns
//   ceil(Z / 8) bytes for bitmap indicating which columns are used (for update
//     events, this bitmap is used for the before image)
//   (v1/v2 update events specific) ceil(Z / 8) bytes for bitmap indicating which
//     columns are used in the after image
//   The remaining body contains the row data (row values are decoded based
//       on current table context):
//     v1/v2 write/delete events specific:
//       List of rows
//     v1/v2 update events specific:
//       List of pairs of (before image row, after image row)
//     Each row image is composed of:
//       bit field indicating whether each field in the row is NULL.
//       list of non-NULL encoded values.
func NewRowsEvent(format *FormatDescriptionEvent, tables map[uint64]*TableMapEvent, eventType EventType, data []byte) (Event, error) {

	var tableIDSize = 6
	if format.EventTypeHeaderLengths[TABLE_MAP_EVENT-1] == 6 {
		tableIDSize = 4
	}

	event := new(RowsEvent)
	offset := 0

	event.TableID = ReadLittleEndianFixedLengthInteger(data[offset:tableIDSize])
	offset += tableIDSize

	// Flags (2 bytes)
	event.Flags = binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	if eventType == UPDATE_ROWS_EVENT_V2 || eventType == WRITE_ROWS_EVENT_V2 || eventType == DELETE_ROWS_EVENT_V2 {
		dataLen := binary.LittleEndian.Uint16(data[offset:])
		offset += 2

		event.ExtraData = data[offset : offset+int(dataLen-2)]
		offset += int(dataLen - 2)
	}

	var n int
	event.ColumnCount, _, n = ReadLengthEncodedInteger(data[offset:])
	offset += n

	bitCount := bitmapByteSize(int(event.ColumnCount))
	event.ColumnBitmap1 = data[offset : offset+bitCount]
	offset += bitCount

	if eventType == UPDATE_ROWS_EVENT_V1 || eventType == UPDATE_ROWS_EVENT_V2 {
		event.ColumnBitmap2 = data[offset : offset+bitCount]
		offset += bitCount
	}

	var ok bool
	event.Table, ok = tables[event.TableID]
	if !ok {
		if len(tables) > 0 {
			return nil, ErrMissingTableId(errors.Errorf("not found table id %d", event.TableID))
		}
		return nil, ErrMissingTableMapEvent(errors.Errorf("invalid table id %d, no corresponding table map event", event.TableID))
	}
	// ... repeat rows until event-end
	defer func() {
		if r := recover(); r != nil {
			log.Fatalf("parse rows event panic %v, data %q, parsed rows %#v, table map %#v\n%s", r, data, event, event.Table, PStack())
		}
	}()

	// Repeatedly parse rows until end of event
	var err error
	for offset < len(data) {
		if n, err = event.parseRows(data[offset:], event.Table, event.ColumnBitmap1); err != nil {
			return nil, err
		}
		offset += n

		if eventType == UPDATE_ROWS_EVENT_V1 {
			if n, err = event.parseRows(data[offset:], event.Table, event.ColumnBitmap2); err != nil {
				return nil, err
			}
			offset += n
		}
	}

	return event, nil
}

func (re *RowsEvent) parseRows(data []byte, table *TableMapEvent, bitmap []byte) (int, error) {
	row := make([]interface{}, re.ColumnCount)
	offset := 0
	count := ByteCountFromBitCount(BitCount(bitmap))

	nullBitmap := data[offset : offset+count]
	offset += count
	nullBitIndex := 0

	var n int
	var err error
	for j := 0; j < int(re.ColumnCount); j++ {
		if GetBit(bitmap, j) == 0 {
			continue
		}

		isNull := (uint32(nullBitmap[nullBitIndex/8]) >> uint32(nullBitIndex%8)) & 0x01
		nullBitIndex++
		if isNull > 0 {
			row[j] = nil
			continue
		}

		row[j], n, err = parseValue(data[offset:], table.ColumnTypes[j], table.ColumnMetadata[j], re.useDecimal)

		if err != nil {
			return 0, nil
		}
		offset = offset + n
	}

	re.Rows = append(re.Rows, row)

	return offset, nil
}

// Ref: MySQL sql/log_event.cc > log_event_print_value
func parseValue(data []byte, typ byte, meta uint16, useDecimal bool) (v interface{}, n int, err error) {
	var length = 0

	if typ == MYSQL_TYPE_STRING {
		if meta >= 256 {
			b0 := uint8(meta >> 8)
			b1 := uint8(meta & 0xFF)

			if b0&0x30 != 0x30 {
				length = int(uint16(b1) | (uint16((b0&0x30)^0x30) << 4))
				typ = byte(b0 | 0x30)
			} else {
				length = int(meta & 0xFF)
				typ = b0
			}
		} else {
			length = int(meta)
		}
	}

	// The MySQL binary replication protocol doesn't tell us whether a field is a
	// signed or unsigned int; we have to process the value differently on the
	// receiving side, where we know more about the table schema.
	switch typ {
	case MYSQL_TYPE_NULL:
		return nil, 0, nil
	case MYSQL_TYPE_TINY:
		return ReadBinaryInt8(data), 1, nil
	case MYSQL_TYPE_SHORT:
		return ReadBinaryInt16(data), 2, nil
	case MYSQL_TYPE_INT24:
		return ReadBinaryInt24(data[0:3]), 3, nil
	case MYSQL_TYPE_LONG:
		return ReadBinaryInt32(data), 4, nil
	case MYSQL_TYPE_LONGLONG:
		return ReadBinaryInt64(data), 8, nil
	case MYSQL_TYPE_NEWDECIMAL:
		return parseDecimalType(data, meta, useDecimal)
	case MYSQL_TYPE_FLOAT:
		return ReadBinaryFloat32(data), 4, nil
	case MYSQL_TYPE_DOUBLE:
		return ReadBinaryFloat64(data), 8, nil
	case MYSQL_TYPE_BIT:
		return parseBitType(data, meta)
	case MYSQL_TYPE_TIMESTAMP:
		t := binary.LittleEndian.Uint32(data)
		return time.Unix(int64(t), 0), 4, nil
	case MYSQL_TYPE_TIMESTAMP2:
		return parseTimestamp2Type(data, meta)
	case MYSQL_TYPE_DATETIME:
		return parseDateTime(data)
	case MYSQL_TYPE_DATETIME2:
		return parseDatetime2(data, meta)
	case MYSQL_TYPE_TIME:
		n = 3
		i32 := uint32(ReadLittleEndianFixedLengthInteger(data[0:3]))
		if i32 == 0 {
			v = "00:00:00"
		} else {
			sign := ""
			if i32 < 0 {
				sign = "-"
			}
			v = fmt.Sprintf("%s%02d:%02d:%02d", sign, i32/10000, (i32%10000)/100, i32%100)
		}
		return v, n, nil
	case MYSQL_TYPE_TIME2:
		return parseTime2Type(data, meta)
	case MYSQL_TYPE_DATE:
		n = 3
		i32 := uint32(ReadLittleEndianFixedLengthInteger(data[0:3]))
		if i32 == 0 {
			v = "0000-00-00"
		} else {
			v = fmt.Sprintf("%04d-%02d-%02d", i32/(16*32), i32/32%16, i32%32)
		}
		return v, n, err
	case MYSQL_TYPE_YEAR:
		return parseYear(data)
	case MYSQL_TYPE_ENUM:
		l := meta & 0xFF
		switch l {
		case 1:
			v = int64(data[0])
			n = 1
		case 2:
			v = int64(binary.BigEndian.Uint16(data))
			n = 2
		default:
			err = fmt.Errorf("unknown ENUM packlen=%d", l)
		}
		return v, n, err
	case MYSQL_TYPE_SET:
		nbits := meta & 0xFF
		n = int(nbits+7) / 8
		v, err = parseBit(data, int(nbits), n)
		return v, n, err
	case MYSQL_TYPE_BLOB, MYSQL_TYPE_GEOMETRY:
		// MySQL saves Geometry as Blob in binlog
		// Seem that the binary format is SRID (4 bytes) + WKB, outer can use
		// MySQL GeoFromWKB or others to create the geometry data.
		// Refer https://dev.mysql.com/doc/refman/5.7/en/gis-wkb-functions.html
		// I also find some go libs to handle WKB if possible
		// see https://github.com/twpayne/go-geom or https://github.com/paulmach/go.geo
		return parseBlob(data, meta)
	case MYSQL_TYPE_VARCHAR, MYSQL_TYPE_VAR_STRING:
		return parseString(data, int(meta))
	case MYSQL_TYPE_STRING:
		return parseString(data, length)
	case MYSQL_TYPE_JSON:
		// Refer: https://github.com/shyiko/mysql-binlog-connector-java/blob/master/src/main/java/com/github/shyiko/mysql/binlog/event/deserialization/AbstractRowsEventDataDeserializer.java#L404
		length = int(ReadLittleEndianFixedLengthInteger(data[0:meta]))
		n = length + int(meta)
		v, err = DecodeJsonBinary(data[meta:n], useDecimal)
		return v, n, err
	default:
		return v, n, fmt.Errorf("unsupported type %d in binlog", typ)
	}
}

func parseYear(b []byte) (string, int, error) {
	v := b[0]
	var str string

	if v == 0 {
		str = "0000"
	} else {
		str = strconv.Itoa(int(b[0]) + 1900)
	}

	return str, 1, nil
}

func parseBitType(b []byte, meta uint16) (int64, int, error) {
	numBits := int(((meta >> 8) * 8) + (meta & 0xFF))
	numBytes := ByteCountFromBitCount(numBits)
	v, err := parseBit(b, numBits, numBytes)
	return v, numBytes, err
}

func parseBit(b []byte, numBits int, length int) (int64, error) {
	var (
		value int64
		err   error
	)
	if numBits > 1 {
		switch length {
		case 1:
			value = int64(b[0])
		case 2:
			value = int64(binary.BigEndian.Uint16(b))
		case 4:
			value = int64(binary.BigEndian.Uint32(b))
		case 3, 5, 6, 7:
			value = int64(ReadBigEndianFixedLengthInteger(b[0:length]))
		case 8:
			value = int64(binary.BigEndian.Uint64(b))
		default:
			err = fmt.Errorf("invalid bit length %d", length)
		}
	} else {
		if length != 1 {
			err = fmt.Errorf("invalid bit length %d", length)
		} else {
			value = int64(b[0])
		}
	}
	return value, err
}

// Ref: https://github.com/jeremycole/mysql_binlog
func parseDecimalType(data []byte, meta uint16, useDecimal bool) (interface{}, int, error) {
	precision := int(meta >> 8)
	decimals := int(meta & 0xFF)
	return ParseDecimalType(data, precision, decimals, useDecimal)
}

func parseDateTime(b []byte) (time.Time, int, error) {
	val := binary.LittleEndian.Uint64(b)
	d := val / 1000000
	t := val % 1000000
	v := time.Date(
		int(d/10000),              // year
		time.Month((d%10000)/100), // month
		int(d%100),                // day
		int(t/10000),              // hour
		int((t%10000)/100),        // minute
		int(t%100),                // second
		0,                         // nanosecond
		time.UTC)
	return v, 8, nil
}

func parseString(data []byte, length int) (v string, n int, err error) {
	if length < 256 {
		length = int(data[0])
		n = int(length) + 1
		v = UnsafeGetString(data[1:n])
	} else {
		length = int(binary.LittleEndian.Uint16(data[0:]))
		n = length + 2
		v = UnsafeGetString(data[2:n])
	}
	return v, n, nil
}

func parseTimestamp2Type(data []byte, meta uint16) (string, int, error) {
	numBytes := int(4 + (meta+1)/2)
	sec := int64(binary.BigEndian.Uint32(data[0:4]))
	usec := int64(0)

	switch meta {
	case 1, 2:
		usec = int64(data[4]) * 10000
	case 3, 4:
		usec = int64(binary.BigEndian.Uint16(data[4:])) * 100
	case 5, 6:
		usec = int64(ReadBigEndianFixedLengthInteger(data[4:7]))
	}

	if sec == 0 {
		return "0000-00-00 00:00:00", numBytes, nil
	}

	t := time.Unix(sec, usec*1000)
	return t.Format(TimeFormat), numBytes, nil
}

func parseDatetime2(data []byte, meta uint16) (string, int, error) {
	//get datetime binary length
	n := int(5 + (meta+1)/2)

	intPart := int64(ReadBigEndianFixedLengthInteger(data[0:5])) - DATETIMEF_INT_OFS
	var frac int64 = 0

	switch meta {
	case 1, 2:
		frac = int64(data[5]) * 10000
	case 3, 4:
		frac = int64(binary.BigEndian.Uint16(data[5:7])) * 100
	case 5, 6:
		frac = int64(ReadBigEndianFixedLengthInteger(data[5:8]))
	}

	if intPart == 0 {
		return "0000-00-00 00:00:00", n, nil
	}

	tmp := intPart<<24 + frac

	if tmp < 0 {
		tmp = -tmp
	}

	// Ignore second part (precision)
	// var secPart int64 = tmp % (1 << 24)

	ymdhms := tmp >> 24

	ymd := ymdhms >> 17
	ym := ymd >> 5
	hms := ymdhms % (1 << 17)

	day := int(ymd % (1 << 5))
	month := int(ym % 13)
	year := int(ym / 13)

	second := int(hms % (1 << 6))
	minute := int((hms >> 6) % (1 << 6))
	hour := int(hms >> 12)

	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second), n, nil
}

func parseTime2Type(data []byte, meta uint16) (string, int, error) {
	numBytes := int(3 + (meta+1)/2)

	tmp := int64(0)
	intPart := int64(0)
	frac := int64(0)

	switch meta {
	case 1:
	case 2:
		intPart = int64(ReadBigEndianFixedLengthInteger(data[0:3])) - TIMEF_INT_OFS
		frac = int64(data[3])
		/*
			   Negative values are stored with reverse fractional part order,
			   for binary sort compatibility.

				 Disk value  intpart frac   Time value   Memory value
				 800000.00    0      0      00:00:00.00  0000000000.000000
				 7FFFFF.FF   -1      255   -00:00:00.01  FFFFFFFFFF.FFD8F0
				 7FFFFF.9D   -1      99    -00:00:00.99  FFFFFFFFFF.F0E4D0
				 7FFFFF.00   -1      0     -00:00:01.00  FFFFFFFFFF.000000
				 7FFFFE.FF   -1      255   -00:00:01.01  FFFFFFFFFE.FFD8F0
				 7FFFFE.F6   -2      246   -00:00:01.10  FFFFFFFFFE.FE7960

				 Formula to convert fractional part from disk format
				 (now stored in "frac" variable) to absolute value: "0x100 - frac".
				 To reconstruct in-memory value, we shift
				 to the next integer value and then substruct fractional part.
		*/
		if intPart < 0 && frac > 0 {
			intPart = intPart + 1 // Shift to the next integer value
			frac = frac - 0x100   /* -(0x100 - frac) */
		}
		tmp = intPart<<24 + frac*10000
	case 3:
	case 4:
		intPart = int64(ReadBigEndianFixedLengthInteger(data[0:3])) - TIMEF_INT_OFS
		frac = int64(binary.BigEndian.Uint16(data[3:5]))
		if intPart < 0 && frac > 0 {
			// Fix reverse fractional part order: "0x10000 - frac"
			intPart = intPart + 1 // Shift to the next integer value
			frac = frac - 0x10000
		}
		tmp = intPart<<24 + frac*100
	case 5:
	case 6:
		tmp = int64(ReadBigEndianFixedLengthInteger(data[0:6])) - TIMEF_OFS
	default:
		intPart = int64(ReadBigEndianFixedLengthInteger(data[0:3])) - TIMEF_INT_OFS
		tmp = intPart << 24
	}

	if intPart == 0 {
		return "00:00:00", numBytes, nil
	}

	hms := int64(0)
	sign := ""
	if tmp < 0 {
		tmp = -tmp
		sign = "-"
	}

	// Ignore second part (precision)
	hms = tmp >> 24

	hour := (hms >> 12) % (1 << 10) // 10 bits starting at 12th
	minute := (hms >> 6) % (1 << 6) // 6 bits starting at 6th
	second := hms % (1 << 6)        // 6 bits starting at 0th
	secPart := tmp % (1 << 24)

	if secPart != 0 {
		return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hour, minute, second, secPart), numBytes, nil
	}

	return fmt.Sprintf("%s%02d:%02d:%02d", sign, hour, minute, second), numBytes, nil
}

func parseBlob(data []byte, meta uint16) (v []byte, n int, err error) {
	var length int
	switch meta {
	case 1:
		length = int(data[0])
		v = data[1 : 1+length]
		n = length + 1
	case 2:
		length = int(binary.LittleEndian.Uint16(data))
		v = data[2 : 2+length]
		n = length + 2
	case 3:
		length = int(ReadLittleEndianFixedLengthInteger(data[0:3]))
		v = data[3 : 3+length]
		n = length + 3
	case 4:
		length = int(binary.LittleEndian.Uint32(data))
		v = data[4 : 4+length]
		n = length + 4
	default:
		err = fmt.Errorf("invalid blob packlen = %d", meta)
	}
	return v, n, err
}

const TIMEF_OFS int64 = 0x800000000000
const TIMEF_INT_OFS int64 = 0x800000
