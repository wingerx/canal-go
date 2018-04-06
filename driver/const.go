package driver

import (
	"github.com/juju/errors"
	"log"
	"os"
)

const (
	NULL_TERMINATED_STRING_DELIMITER       = 0x00
	BinlogVersion                    byte  = 4
	EventHeaderSize                        = 19
	TimeFormat                             = "2006-01-02 15:04:05"
	MinProtocolVersion               byte  = 10
	DATETIMEF_INT_OFS                int64 = 0x8000000000
	NATIVE_CLIENT_NAME                     = "mysql_native_password"
	COM_SEMI_MARK                = 0xef
	SERVER_MORE_RESULTS_EXISTS = 0x0008
)

// Status codes
const (
	OK_HEADER            byte = 0x00
	LOCAL_IN_FILE_HEADER byte = 0xfb
	EOF_HEADER           byte = 0xfe
	ERR_HEADER           byte = 0xff
)

// Types
const (
	MYSQL_TYPE_DECIMAL    byte = iota
	MYSQL_TYPE_TINY
	MYSQL_TYPE_SHORT
	MYSQL_TYPE_LONG
	MYSQL_TYPE_FLOAT
	MYSQL_TYPE_DOUBLE
	MYSQL_TYPE_NULL
	MYSQL_TYPE_TIMESTAMP
	MYSQL_TYPE_LONGLONG
	MYSQL_TYPE_INT24
	MYSQL_TYPE_DATE
	MYSQL_TYPE_TIME
	MYSQL_TYPE_DATETIME
	MYSQL_TYPE_YEAR
	MYSQL_TYPE_NEWDATE
	MYSQL_TYPE_VARCHAR
	MYSQL_TYPE_BIT
	MYSQL_TYPE_TIMESTAMP2  // 5.6+
	MYSQL_TYPE_DATETIME2
	MYSQL_TYPE_TIME2
)

const (
	MYSQL_TYPE_NEWDECIMAL  byte = iota + 0xf6
	MYSQL_TYPE_ENUM
	MYSQL_TYPE_SET
	MYSQL_TYPE_TINY_BLOB
	MYSQL_TYPE_MEDIUM_BLOB
	MYSQL_TYPE_LONG_BLOB
	MYSQL_TYPE_BLOB
	MYSQL_TYPE_VAR_STRING
	MYSQL_TYPE_STRING
	MYSQL_TYPE_GEOMETRY
)

// Commands
const (
	COM_SLEEP               byte = iota
	COM_QUIT
	COM_INIT_DB
	COM_QUERY
	COM_FIELD_LIST
	COM_CREATE_DB
	COM_DROP_DB
	COM_REFRESH
	COM_SHUTDOWN
	COM_STATISTICS
	COM_PROCESS_INFO
	COM_CONNECT
	COM_PROCESS_KILL
	COM_DEBUG
	COM_PING
	COM_TIME
	COM_DELAYED_INSERT
	COM_CHANGE_USER
	COM_BINLOG_DUMP
	COM_TABLE_DUMP
	COM_CONNECT_OUT
	COM_REGISTER_SLAVE
	COM_STMT_PREPARE
	COM_STMT_EXECUTE
	COM_STMT_SEND_LONG_DATA
	COM_STMT_CLOSE
	COM_STMT_RESET
	COM_SET_OPTION
	COM_STMT_FETCH
	COM_DAEMON
	COM_BINLOG_DUMP_GTID
	COM_RESET_CONNECTION
)

// Listening options
const (
	BINLOG_DUMP_NEVER_STOP  uint16 = 0x0
	BINLOG_DUMP_NON_BLOCK   uint16 = 0x1
	BINLOG_THROUGH_POSITION uint16 = 0x2
	BINLOG_THROUGH_GTID     uint16 = 0x4
)

// Binlog events
type EventType byte

const (
	UNKNOWN_EVENT            EventType = iota // 0x00
	START_EVENT_V3
	QUERY_EVENT
	STOP_EVENT
	ROTATE_EVENT
	INTVAR_EVENT
	LOAD_EVENT
	SLAVE_EVENT
	CREATE_FILE_EVENT
	APPEND_BLOCK_EVENT
	EXEC_LOAD_EVENT
	DELETE_FILE_EVENT
	NEW_LOAD_EVENT
	RAND_EVENT
	USER_VAR_EVENT
	FORMAT_DESCRIPTION_EVENT
	XID_EVENT
	BEGIN_LOAD_QUERY_EVENT
	EXECUTE_LOAD_QUERY_EVENT
	TABLE_MAP_EVENT
	PRE_GA_WRITE_ROWS_EVENT   // pre-GA: from 5.1.0 to 5.1.15
	PRE_GA_UPDATE_ROWS_EVENT
	PRE_GA_DELETE_ROWS_EVENT
	WRITE_ROWS_EVENT_V1       // V1: from 5.1.15 to 5.6.x
	UPDATE_ROWS_EVENT_V1
	DELETE_ROWS_EVENT_V1
	INCIDENT_EVENT
	HEARTBEAT_EVENT
	IGNORABLE_EVENT
	ROWS_QUERY_EVENT
	WRITE_ROWS_EVENT_V2       // V2: From 5.6.x
	UPDATE_ROWS_EVENT_V2
	DELETE_ROWS_EVENT_V2
	GTID_LOG_EVENT
	ANONYMOUS_GTID_LOG_EVENT
	PREVIOUS_GTIDS_LOG_EVENT  // 0x23
)

func (e EventType) String() string {
	switch e {
	case UNKNOWN_EVENT:
		return "UnknownEvent"
	case START_EVENT_V3:
		return "StartEventV3"
	case QUERY_EVENT:
		return "QueryEvent"
	case STOP_EVENT:
		return "StopEvent"
	case ROTATE_EVENT:
		return "RotateEvent"
	case INTVAR_EVENT:
		return "IntVarEvent"
	case LOAD_EVENT:
		return "LoadEvent"
	case SLAVE_EVENT:
		return "SlaveEvent"
	case CREATE_FILE_EVENT:
		return "CreateFileEvent"
	case APPEND_BLOCK_EVENT:
		return "AppendBlockEvent"
	case EXEC_LOAD_EVENT:
		return "ExecLoadEvent"
	case DELETE_FILE_EVENT:
		return "DeleteFileEvent"
	case NEW_LOAD_EVENT:
		return "NewLoadEvent"
	case RAND_EVENT:
		return "RandEvent"
	case USER_VAR_EVENT:
		return "UserVarEvent"
	case FORMAT_DESCRIPTION_EVENT:
		return "FormatDescriptionEvent"
	case XID_EVENT:
		return "XidEvent"
	case BEGIN_LOAD_QUERY_EVENT:
		return "BeginLoadQueryEvent"
	case EXECUTE_LOAD_QUERY_EVENT:
		return "ExecuteLoadQueryEvent"
	case TABLE_MAP_EVENT:
		return "TableMapEvent"
	case PRE_GA_WRITE_ROWS_EVENT:
		return "PreGAWriteRowsEvent"
	case PRE_GA_UPDATE_ROWS_EVENT:
		return "PreGAUpdateRowsEvent"
	case PRE_GA_DELETE_ROWS_EVENT:
		return "PreGADeleteRowsEvent"
	case WRITE_ROWS_EVENT_V1:
		return "WriteRowsEventV1"
	case UPDATE_ROWS_EVENT_V1:
		return "UpdateRowsEventV1"
	case DELETE_ROWS_EVENT_V1:
		return "DeleteRowsEventV1"
	case INCIDENT_EVENT:
		return "IncidentEvent"
	case HEARTBEAT_EVENT:
		return "HeartbeatEvent"
	case IGNORABLE_EVENT:
		return "IgnorableEvent"
	case ROWS_QUERY_EVENT:
		return "RowsQueryEvent"
	case WRITE_ROWS_EVENT_V2:
		return "WriteRowsEventV2"
	case UPDATE_ROWS_EVENT_V2:
		return "UpdateRowsEventV2"
	case DELETE_ROWS_EVENT_V2:
		return "DeleteRowsEventV2"
	case GTID_LOG_EVENT:
		return "GTIDLogEvent"
	case ANONYMOUS_GTID_LOG_EVENT:
		return "AnonymousGTIDLogEvent"
	case PREVIOUS_GTIDS_LOG_EVENT:
		return "PreviousGTIDsLogEvent"
	default:
		return "UnknownEvent"
	}
}

// Log events
const (
	LOG_EVENT_BINLOG_IN_USE_F            uint16 = 0x0001
	LOG_EVENT_FORCED_ROTATE_F            uint16 = 0x0002
	LOG_EVENT_THREAD_SPECIFIC_F          uint16 = 0x0004
	LOG_EVENT_SUPPRESS_USE_F             uint16 = 0x0008
	LOG_EVENT_UPDATE_TABLE_MAP_VERSION_F uint16 = 0x0010
	LOG_EVENT_ARTIFICIAL_F               uint16 = 0x0020
	LOG_EVENT_RELAY_LOG_F                uint16 = 0x0040
	LOG_EVENT_IGNORABLE_F                uint16 = 0x0080
	LOG_EVENT_NO_FILTER_F                uint16 = 0x0100
	LOG_EVENT_MTS_ISOLATE_F              uint16 = 0x0200
)

const (
	CLIENT_LONG_PASSWORD                  uint32 = 1 << iota
	CLIENT_FOUND_ROWS
	CLIENT_LONG_FLAG
	CLIENT_CONNECT_WITH_DB
	CLIENT_NO_SCHEMA
	CLIENT_COMPRESS
	CLIENT_ODBC
	CLIENT_LOCAL_FILES
	CLIENT_IGNORE_SPACE
	CLIENT_PROTOCOL_41
	CLIENT_INTERACTIVE
	CLIENT_SSL
	CLIENT_IGNORE_SIGPIPE
	CLIENT_TRANSACTIONS
	CLIENT_RESERVED
	CLIENT_SECURE_CONNECTION
	CLIENT_MULTI_STATEMENTS
	CLIENT_MULTI_RESULTS
	CLIENT_PS_MULTI_RESULTS
	CLIENT_PLUGIN_AUTH
	CLIENT_CONNECT_ATTRS
	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
)

// Flags
const (
	NOT_NULL_FLAG       = 1
	PRI_KEY_FLAG        = 2
	UNIQUE_KEY_FLAG     = 4
	BLOB_FLAG           = 16
	UNSIGNED_FLAG       = 32
	ZEROFILL_FLAG       = 64
	BINARY_FLAG         = 128
	ENUM_FLAG           = 256
	AUTO_INCREMENT_FLAG = 512
	TIMESTAMP_FLAG      = 1024
	SET_FLAG            = 2048
	NUM_FLAG            = 32768
	PART_KEY_FLAG       = 16384
	GROUP_FLAG          = 32768
	UNIQUE_FLAG         = 65536
)

// A few interesting character set values.
// See http://dev.mysql.com/doc/internals/en/character-set.html#packet-Protocol::CharacterSet
const (
	// CharacterSetUtf8 is for UTF8. We use this by default.
	CharacterSetUtf8 = 33

	// CharacterSetBinary is for binary. Use by integer fields for instance.
	CharacterSetBinary = 63
)

// CharacterSetMap maps the charset name (used in ConnParams) to the
// integer value.  Interesting ones have their own constant above.
var CharacterSetMap = map[string]uint8{
	"big5":     1,
	"dec8":     3,
	"cp850":    4,
	"hp8":      6,
	"koi8r":    7,
	"latin1":   8,
	"latin2":   9,
	"swe7":     10,
	"ascii":    11,
	"ujis":     12,
	"sjis":     13,
	"hebrew":   16,
	"tis620":   18,
	"euckr":    19,
	"koi8u":    22,
	"gb2312":   24,
	"greek":    25,
	"cp1250":   26,
	"gbk":      28,
	"latin5":   30,
	"armscii8": 32,
	"utf8":     CharacterSetUtf8,
	"ucs2":     35,
	"cp866":    36,
	"keybcs2":  37,
	"macce":    38,
	"macroman": 39,
	"cp852":    40,
	"latin7":   41,
	"utf8mb4":  45,
	"cp1251":   51,
	"utf16":    54,
	"utf16le":  56,
	"cp1256":   57,
	"cp1257":   59,
	"utf32":    60,
	"binary":   CharacterSetBinary,
	"geostd8":  92,
	"cp932":    95,
	"eucjpms":  97,
}

var (
	ErrInvalidConn = errors.New("Invalid Connection")
	ErrMalformPkt  = errors.New("Malformed Packet")
	ErrBadConn     = errors.New("driver: bad connection")
)

// Logger is used to log critical error messages.
type Logger interface {
	Print(v ...interface{})
}

var errLog Logger = log.New(os.Stderr, "[ERR] ", log.Ldate|log.Ltime|log.Lshortfile)
