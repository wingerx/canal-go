package events

import (
	"encoding/binary"
	"github.com/juju/errors"
	. "github.com/woqutech/drt/tools"
	"io"
	"strings"
)

// A format description event is the first event for binlog-version 4;
// it describes how the following events are structured.
type FormatDescriptionEvent struct {
	BinlogVersion          uint16
	ServerVersion          []byte
	CreationTimestamp      uint32
	EventHeaderLength      uint8
	EventTypeHeaderLengths []byte

	// 0 is off, 1 is for CRC32, 255 is undefined
	ChecksumAlgorithm byte
}

// Expected format (L = total length of event data):
//   # bytes   field
//   2         format version
//   50        server version string, 0-padded but not necessarily 0-terminated
//   4         timestamp (same as timestamp header field)
//   1         header length
//   p         (one byte per packet type) event type header lengths
//             Rest was infered from reading source code:
//   1         checksum algorithm
//   4         checksum
func NewFormatDescriptionEvent(data []byte) (Event, error) {

	event := new(FormatDescriptionEvent)
	offset := 0

	// Version of this binlog format (2 bytes)
	event.BinlogVersion = binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Version of the MySQL server that created the binlog (string[50])
	event.ServerVersion = make([]byte, 50)
	copy(event.ServerVersion, data[offset:offset+50])
	offset += 50

	// Seconds since Unix epoch when the binlog was created (4 bytes)
	event.CreationTimestamp = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Length of the binlog event header of following events; should always match
	// const EventHeaderSize (1 byte)
	event.EventHeaderLength = data[offset]
	offset++
	if event.EventHeaderLength != byte(EventHeaderSize) {
		return nil, errors.Errorf("header length too short, required [%d] but get [%d]", EventHeaderSize, event.EventHeaderLength)
	}

	serverVersion := string(event.ServerVersion)
	checksumProduct := checksumVersionProductMySQL
	if strings.Contains(strings.ToLower(serverVersion), "mariadb") {
		checksumProduct = checksumVersionProductMariaDB
	}

	if getVersionProduct(serverVersion) >= checksumProduct {
		// the last 5 bytes is 1 byte check sum alg type and 4 byte checksum if exists
		event.ChecksumAlgorithm = data[len(data)-5]
		event.EventTypeHeaderLengths = data[offset : len(data)-5]
	} else {
		event.ChecksumAlgorithm = BINLOG_CHECKSUM_ALG_UNDEF
		event.EventTypeHeaderLengths = data[offset:]
	}
	return event, nil
}

func NewFormatDescriptionEventV4() *FormatDescriptionEvent {
	event := new(FormatDescriptionEvent)
	event.BinlogVersion = 4
	event.ServerVersion = []byte("5.0")
	event.EventHeaderLength = uint8(int(PREVIOUS_GTIDS_LOG_EVENT) - 1)
	event.EventTypeHeaderLengths = make([]byte, int(PREVIOUS_GTIDS_LOG_EVENT))
	event.EventTypeHeaderLengths[START_EVENT_V3-1] = START_V3_HEADER_LEN
	event.EventTypeHeaderLengths[QUERY_EVENT-1] = QUERY_HEADER_LEN
	event.EventTypeHeaderLengths[STOP_EVENT-1] = STOP_HEADER_LEN
	event.EventTypeHeaderLengths[ROTATE_EVENT-1] = ROTATE_HEADER_LEN
	event.EventTypeHeaderLengths[INTVAR_EVENT-1] = INTVAR_HEADER_LEN
	event.EventTypeHeaderLengths[LOAD_EVENT-1] = LOAD_HEADER_LEN
	event.EventTypeHeaderLengths[SLAVE_EVENT-1] = SLAVE_HEADER_LEN
	event.EventTypeHeaderLengths[CREATE_FILE_EVENT-1] = CREATE_FILE_HEADER_LEN
	event.EventTypeHeaderLengths[APPEND_BLOCK_EVENT-1] = APPEND_BLOCK_HEADER_LEN
	event.EventTypeHeaderLengths[EXEC_LOAD_EVENT-1] = EXEC_LOAD_HEADER_LEN
	event.EventTypeHeaderLengths[DELETE_FILE_EVENT-1] = DELETE_FILE_HEADER_LEN
	event.EventTypeHeaderLengths[NEW_LOAD_EVENT-1] = NEW_LOAD_HEADER_LEN
	event.EventTypeHeaderLengths[RAND_EVENT-1] = RAND_HEADER_LEN
	event.EventTypeHeaderLengths[USER_VAR_EVENT-1] = USER_VAR_HEADER_LEN
	event.EventTypeHeaderLengths[FORMAT_DESCRIPTION_EVENT-1] = FORMAT_DESCRIPTION_HEADER_LEN
	event.EventTypeHeaderLengths[XID_EVENT-1] = XID_HEADER_LEN
	event.EventTypeHeaderLengths[BEGIN_LOAD_QUERY_EVENT-1] = BEGIN_LOAD_QUERY_HEADER_LEN
	event.EventTypeHeaderLengths[EXECUTE_LOAD_QUERY_EVENT-1] = EXECUTE_LOAD_QUERY_HEADER_LEN
	event.EventTypeHeaderLengths[TABLE_MAP_EVENT-1] = TABLE_MAP_HEADER_LEN
	event.EventTypeHeaderLengths[WRITE_ROWS_EVENT_V1-1] = ROWS_HEADER_LEN_V1
	event.EventTypeHeaderLengths[UPDATE_ROWS_EVENT_V1-1] = ROWS_HEADER_LEN_V1
	event.EventTypeHeaderLengths[DELETE_ROWS_EVENT_V1-1] = ROWS_HEADER_LEN_V1
	event.EventTypeHeaderLengths[HEARTBEAT_EVENT-1] = 0
	event.EventTypeHeaderLengths[IGNORABLE_EVENT-1] = IGNORABLE_HEADER_LEN
	event.EventTypeHeaderLengths[ROWS_QUERY_EVENT-1] = IGNORABLE_HEADER_LEN
	event.EventTypeHeaderLengths[WRITE_ROWS_EVENT_V2-1] = ROWS_HEADER_LEN_V2
	event.EventTypeHeaderLengths[UPDATE_ROWS_EVENT_V2-1] = ROWS_HEADER_LEN_V2
	event.EventTypeHeaderLengths[DELETE_ROWS_EVENT_V2-1] = ROWS_HEADER_LEN_V2
	event.EventTypeHeaderLengths[GTID_LOG_EVENT-1] = POST_HEADER_LENGTH
	event.EventTypeHeaderLengths[ANONYMOUS_GTID_LOG_EVENT-1] = POST_HEADER_LENGTH
	event.EventTypeHeaderLengths[PREVIOUS_GTIDS_LOG_EVENT-1] = IGNORABLE_HEADER_LEN

	event.ChecksumAlgorithm = BINLOG_CHECKSUM_ALG_CRC32
	return event
}

// "Transaction ID for 2PC, written whenever a COMMIT is expected."
type XidEvent struct {
	Xid uint64
}

func NewXidEvent(data []byte) (Event, error) {
	event := new(XidEvent)

	// XID (8 bytes)
	event.Xid = binary.LittleEndian.Uint64(data)

	return event, nil
}

// The query event is used to send text queries the correct binlog.
type QueryEvent struct {
	SlaveID       uint32
	ExecutionTime uint32
	ErrorCode     uint16
	StatusVars    []byte
	DatabaseName  []byte
	Query         []byte
	Charset       *Charset
}

// Charset is the per-statement charset info from a QUERY_EVENT binlog entry.
type Charset struct {
	// @@session.character_set_client
	Client int32
	// @@session.collation_connection
	Conn int32
	// @@session.collation_server
	Server int32
}

// Expected format (L = total length of event data):
//   # bytes   field
//   4         thread_id
//   4         execution time
//   1         length of db_name, not including NULL terminator (X)
//   2         error code
//   2         length of status vars block (Y)
//   Y         status vars block
//   X+1       db_name + NULL terminator
//   L-X-1-Y   SQL statement (no NULL terminator)
func NewQueryLogEvent(data []byte) (Event, error) {
	event := new(QueryEvent)
	offset := 0

	// Slave ID (4 bytes)
	event.SlaveID = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Execution time (4 bytes)
	event.ExecutionTime = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Database name length (1 byte)
	dbNameLength := uint8(data[offset])
	offset++

	// Error code (2 bytes)
	event.ErrorCode = binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Status-vars length (2 bytes)
	statusVarsLength := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Status-vars (string[$len])
	event.StatusVars = data[offset : offset+int(statusVarsLength)]
	offset += int(statusVarsLength)

	// DatabaseName (string[$len])
	event.DatabaseName = data[offset : offset+int(dbNameLength)]
	offset += int(dbNameLength)

	// Skip [00] byte
	offset++

	// Query (string[EOF])
	event.Query = data[offset:]

	// Scan the status vars for ones we care about. This requires us to know the
	// size of every var that comes before the ones we're interested in.
	vars := event.StatusVars
varsLoop:
	for pos := 0; pos < len(vars); {
		code := vars[pos]
		pos++

		// All codes are optional, but if present they must occur in numerically
		// increasing order (except for 6 which occurs in the place of 2) to allow
		// for backward compatibility.
		switch code {
		case QFlags2Code, QAutoIncrement:
			pos += 4
		case QSQLModeCode:
			pos += 8
		case QCatalog: // Used in MySQL 5.0.0 - 5.0.3
			if pos+1 > len(vars) {
				return event, errors.Errorf("Q_CATALOG status var overflows buffer (%v + 1 > %v)", pos, len(vars))
			}
			pos += 1 + int(vars[pos]) + 1
		case QCatalogNZCode: // Used in MySQL > 5.0.3 to replace QCatalog
			if pos+1 > len(vars) {
				return event, errors.Errorf("Q_CATALOG_NZ_CODE status var overflows buffer (%v + 1 > %v)", pos, len(vars))
			}
			pos += 1 + int(vars[pos])
		case QCharsetCode:
			if pos+6 > len(vars) {
				return event, errors.Errorf("Q_CHARSET_CODE status var overflows buffer (%v + 6 > %v)", pos, len(vars))
			}
			event.Charset = &Charset{
				Client: int32(binary.LittleEndian.Uint16(vars[pos : pos+2])),
				Conn:   int32(binary.LittleEndian.Uint16(vars[pos+2 : pos+4])),
				Server: int32(binary.LittleEndian.Uint16(vars[pos+4 : pos+6])),
			}
			pos += 6
		default:
			// If we see something higher than what we're interested in, we can stop.
			break varsLoop
		}
	}
	return event, nil
}

// "truncate a file and set block-data"
type BeginLoadQueryEvent struct {
	FileID    uint32
	BlockData []byte
}

func NewBeginLoadQueryEvent(data []byte) (Event, error) {
	event := new(BeginLoadQueryEvent)

	offset := 0

	// File ID (4)
	event.FileID = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Block data (string[EOF])
	event.BlockData = data[offset:]

	return event, nil
}

type ExecuteLoadQueryEvent struct {
	SlaveProxyID     uint32
	ExecutionTime    uint32
	SchemaLength     uint8
	ErrorCode        uint16
	StatusVars       uint16
	FileID           uint32
	StartPos         uint32
	EndPos           uint32
	DupHandlingFlags uint8
}

func NewExecuteLoadQueryEvent(data []byte) (Event, error) {
	event := new(ExecuteLoadQueryEvent)
	offset := 0

	// Slave proxy ID (4 bytes)
	event.SlaveProxyID = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Execution time (4 bytes)
	event.ExecutionTime = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Schema length (1 byte)
	event.SchemaLength = uint8(data[offset])
	offset++

	// Error code (2 bytes)
	event.ErrorCode = binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Status-vars length (2 byte)
	event.StatusVars = binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	// File ID (4 bytes)
	event.FileID = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Start position (4 nytes)
	event.StartPos = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// End position (4 bytes)
	event.EndPos = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Dup handling flags (1 byte)
	event.DupHandlingFlags = uint8(data[offset])

	return event, nil
}

// A rotate event tells us what binlog to request next.
type RotateEvent struct {
	NextPosition uint64 // position inside next binlog file
	NextFileName []byte // name of next binlog file
}

// Payload is structured as follows for MySQL v5.5:
//   8 bytes (uint64) for offset position
//   the remainder for the new log name (not zero terminated)
func NewRotateEvent(data []byte) (Event, error) {
	event := new(RotateEvent)
	event.NextPosition = binary.LittleEndian.Uint64(data[0:8])
	event.NextFileName = data[8:]

	return event, nil
}

type TableMapEvent struct {
	TableID        uint64
	Flags          uint16
	DatabaseName   []byte
	TableName      []byte
	ColumnCount    uint64
	ColumnTypes    []byte
	ColumnMetadata []uint16
	NullBitVector  []byte
}

// Expected format (L = total length of event data):
//  # bytes   field
//  4/6       table id
//  2         flags
//  1         schema name length sl
//  sl        schema name
//  1         [00]
//  1         table name length tl
//  tl        table name
//  1         [00]
//  <var>     column count cc (var-len encoded)
//  cc        column-def, one byte per column
//  <var>     column-meta-def (var-len encoded string)
//  n         NULL-bitmask, length: (cc + 7) / 8
func NewTableMapEvent(format *FormatDescriptionEvent, data []byte) (Event, error) {
	var tableIDSize = 6
	if format.EventTypeHeaderLengths[TABLE_MAP_EVENT-1] == 6 {
		tableIDSize = 4
	}

	event := new(TableMapEvent)
	offset := 0

	// Numeric table ID (now 6 bytes, previously 4)
	event.TableID = ReadLittleEndianFixedLengthInteger(data[offset:tableIDSize])
	offset += tableIDSize

	// Flags (2 bytes)
	event.Flags = binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Length of the database name (1 byte)
	dbNameLength := int(data[offset])
	offset++

	// Database name (string[dbNameLength])
	dbName := data[offset : offset+dbNameLength+1]
	offset = offset + dbNameLength + 1
	event.DatabaseName = dbName[:dbNameLength] // drop the null-termination char

	// Length of the table name
	tblNameLength := int(data[offset])
	offset++

	// Table name (string[tableLength])
	tblName := data[offset : offset+tblNameLength+1]
	offset += tblNameLength + 1
	event.TableName = tblName[:tblNameLength] // drop the null-termination char

	// Number of columns in the table map (lenenc-int)
	var n int
	event.ColumnCount, _, n = ReadLengthEncodedInteger(data[offset:])
	offset += n

	// Array of column definitions: one byte per field type
	event.ColumnTypes = data[offset : offset+int(event.ColumnCount)]
	offset += int(event.ColumnCount)

	// Array of metadata per column (lenenc-str):
	// length is the overall length of the metadata array in bytes
	// length of each metadata field is dependent on the column's field type
	var err error
	var metadata []byte
	if metadata, _, n, err = ReadLengthEncodedString(data[offset:]); err != nil {
		return nil, err
	}
	if err = event.parseMetadata(metadata); err != nil {
		return nil, err
	}
	offset += n

	// A bitmask containing a bit set for each column that can be null.
	if len(data[offset:]) != bitmapByteSize(int(event.ColumnCount)) {
		return nil, io.EOF
	}
	event.NullBitVector = data[offset:]

	return event, nil
}

//	0 byte
//	MYSQL_TYPE_DECIMAL
//	MYSQL_TYPE_TINY
//	MYSQL_TYPE_SHORT
//	MYSQL_TYPE_LONG
//	MYSQL_TYPE_NULL
//	MYSQL_TYPE_TIMESTAMP
//	MYSQL_TYPE_LONGLONG
//	MYSQL_TYPE_INT24
//	MYSQL_TYPE_DATE
//	MYSQL_TYPE_TIME
//	MYSQL_TYPE_DATETIME
//	MYSQL_TYPE_YEAR
//
//	1 byte
//	MYSQL_TYPE_FLOAT
//	MYSQL_TYPE_DOUBLE
//	MYSQL_TYPE_BLOB
//	MYSQL_TYPE_GEOMETRY
//
//	//maybe
//	MYSQL_TYPE_TIME2
//	MYSQL_TYPE_DATETIME2
//	MYSQL_TYPE_TIMESTAMP2
//
//	2 byte
//	MYSQL_TYPE_VARCHAR
//	MYSQL_TYPE_BIT
//	MYSQL_TYPE_NEWDECIMAL
//	MYSQL_TYPE_VAR_STRING
//	MYSQL_TYPE_STRING
//
//	This enumeration value is only used internally and cannot exist in a binlog.
//	MYSQL_TYPE_NEWDATE
//	MYSQL_TYPE_ENUM
//	MYSQL_TYPE_SET
//	MYSQL_TYPE_TINY_BLOB
//	MYSQL_TYPE_MEDIUM_BLOB
//	MYSQL_TYPE_LONG_BLOB
func (e *TableMapEvent) parseMetadata(data []byte) error {
	e.ColumnMetadata = make([]uint16, e.ColumnCount)
	offset := 0

	for col, t := range e.ColumnTypes {
		switch t {
		// due to Bug37426 layout of the string metadata is a bit tightly packed: https://bugs.mysql.com/bug.php?id=37426
		case MYSQL_TYPE_STRING:
			x := uint16(data[offset]) << 8 // type
			x = x + uint16(data[offset+1]) // length
			e.ColumnMetadata[col] = x
			offset += 2
		case MYSQL_TYPE_VAR_STRING,
			MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_BIT:
			e.ColumnMetadata[col] = binary.LittleEndian.Uint16(data[offset:])
			offset += 2
		case MYSQL_TYPE_NEWDECIMAL:
			x := uint16(data[offset]) << 8 // precision
			x = x + uint16(data[offset+1]) // decimals
			e.ColumnMetadata[col] = x
			offset += 2
		case MYSQL_TYPE_BLOB,
			MYSQL_TYPE_DOUBLE,
			MYSQL_TYPE_FLOAT,
			MYSQL_TYPE_GEOMETRY:
			e.ColumnMetadata[col] = uint16(data[offset])
			offset++
		case MYSQL_TYPE_TIME2,
			MYSQL_TYPE_DATETIME2,
			MYSQL_TYPE_TIMESTAMP2:
			e.ColumnMetadata[col] = uint16(data[offset])
			offset++
		case MYSQL_TYPE_NEWDATE,
			MYSQL_TYPE_ENUM,
			MYSQL_TYPE_SET,
			MYSQL_TYPE_TINY_BLOB,
			MYSQL_TYPE_MEDIUM_BLOB,
			MYSQL_TYPE_LONG_BLOB:
			return errors.New("unsupported type")
		default:
			e.ColumnMetadata[col] = 0
		}
	}

	return nil
}

// Note: MySQL docs claim this is (n+8)/7, but the below is actually correct
func bitmapByteSize(columnCount int) int {
	return int(columnCount+7) / 8
}

type RowsQueryEvent struct {
	RowsQuery string
}

func NewRowsQueryEvent(data []byte) (Event, error) {
	event := new(RowsQueryEvent)
	event.RowsQuery = string(data[:])
	return event, nil
}

type UserVarLogEvent struct {
	NameLength uint32
	Name       []byte
	IsNull     uint8

	//if not is null
	Type        uint8
	Charset     uint32
	ValueLength uint32
	Value       []byte

	Query string
}

// Expected format (L = total length of event data):
//   # bytes   field
//   4         variable name
//   sl        variable value
//	 1	   	   SQL NULL value
//	 1	   	   user variable type
//	 4	   	   The number of the character set for the user variable
//	 4	   	   The size of the user variable value
//	 vl	   	   value

func NewUserVarLogEvent(data []byte) (Event, error) {
	event := new(UserVarLogEvent)
	offset := 0
	// 4 bytes. the size of the user variable name
	event.NameLength = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// user variable name
	event.Name = data[offset : offset+int(event.NameLength)]
	offset += int(event.NameLength)
	// 1 byte. Non-zero if the variable value is the SQL NULL value, 0 otherwise.
	//  If this byte is 0, the following parts exist in the event.
	event.IsNull = data[offset]
	offset++
	if event.IsNull == 0 {
		// 1 byte user variable type
		event.Type = data[offset]
		offset++

		// 4 byte user character set
		event.Charset = binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		// 4 byte user variable value
		event.ValueLength = binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		event.Value = data[offset:event.ValueLength]
	}
	return event, nil
}

type IntVarEvent struct {
	Type  byte
	Value uint64
}

// Expected format (L = total length of event data):
//   # bytes   field
//   1         variable ID
//   8         variable value
func NewIntVarEvent(data []byte) (Event, error) {
	event := new(IntVarEvent)
	event.Type = data[0]
	if event.Type != IntVarLastInsertID && event.Type != IntVarInsertID {
		return nil, errors.Errorf("invalid IntVar ID: %v", data[0])
	}

	event.Value = binary.LittleEndian.Uint64(data[1 : 1+8])
	return event, nil
}

type HeartbeatLogEvent struct {
	LogIdent []byte
}

func NewHeartbeatLogEvent(data []byte) (Event, error) {
	return &HeartbeatLogEvent{data}, nil
}
