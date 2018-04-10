package events

import (
	"strings"
	"strconv"
	"unicode"
)

const (
	//MinBinlogVersion     byte  = 4
	EventHeaderSize         = 19
	DATETIMEF_INT_OFS int64 = 0x8000000000
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

// end of event marker
const ENUM_END_EVENT = byte(164)

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

const (
	QUERY_HEADER_MINIMAL_LEN            = 4 + 4 + 1 + 2
	QUERY_HEADER_LEN                    = QUERY_HEADER_MINIMAL_LEN + 2
	LOG_EVENT_TYPES                     = ENUM_END_EVENT - 1
	ST_SERVER_VER_LEN                   = 50
	STOP_HEADER_LEN                     = 0
	LOAD_HEADER_LEN                     = 4 + 4 + 4 + 1 + 1 + 4
	SLAVE_HEADER_LEN                    = 0
	START_V3_HEADER_LEN                 = 2 + ST_SERVER_VER_LEN + 4
	ROTATE_HEADER_LEN                   = 8
	INTVAR_HEADER_LEN                   = 0
	CREATE_FILE_HEADER_LEN              = 4
	APPEND_BLOCK_HEADER_LEN             = 4
	EXEC_LOAD_HEADER_LEN                = 4
	DELETE_FILE_HEADER_LEN              = 4
	NEW_LOAD_HEADER_LEN                 = LOAD_HEADER_LEN
	RAND_HEADER_LEN                     = 0
	USER_VAR_HEADER_LEN                 = 0
	FORMAT_DESCRIPTION_HEADER_LEN       = START_V3_HEADER_LEN + 1 + LOG_EVENT_TYPES
	XID_HEADER_LEN                      = 0
	BEGIN_LOAD_QUERY_HEADER_LEN         = APPEND_BLOCK_HEADER_LEN
	ROWS_HEADER_LEN_V1                  = 8
	TABLE_MAP_HEADER_LEN                = 8
	EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN = 4 + 4 + 4 + 1
	EXECUTE_LOAD_QUERY_HEADER_LEN       = QUERY_HEADER_LEN + EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN
	INCIDENT_HEADER_LEN                 = 2
	HEARTBEAT_HEADER_LEN                = 0
	IGNORABLE_HEADER_LEN                = 0
	ROWS_HEADER_LEN_V2                  = 10
	ANNOTATE_ROWS_HEADER_LEN            = 0
	BINLOG_CHECKPOINT_HEADER_LEN        = 4
	GTID_HEADER_LEN                     = 19
	GTID_LIST_HEADER_LEN                = 4
	POST_HEADER_LENGTH                  = 11
	BINLOG_CHECKSUM_ALG_DESC_LEN        = 1
)

// Flags
//const (
//	NOT_NULL_FLAG       = 1
//	PRI_KEY_FLAG        = 2
//	UNIQUE_KEY_FLAG     = 4
//	BLOB_FLAG           = 16
//	UNSIGNED_FLAG       = 32
//	ZEROFILL_FLAG       = 64
//	BINARY_FLAG         = 128
//	ENUM_FLAG           = 256
//	AUTO_INCREMENT_FLAG = 512
//	TIMESTAMP_FLAG      = 1024
//	SET_FLAG            = 2048
//	NUM_FLAG            = 32768
//	PART_KEY_FLAG       = 16384
//	GROUP_FLAG          = 32768
//	UNIQUE_FLAG         = 65536
//)

const RowsEventStmtEndFlag = 0x01

const (
	// QFlags2Code is Q_FLAGS2_CODE
	QFlags2Code = 0

	// QSQLModeCode is Q_SQL_MODE_CODE
	QSQLModeCode = 1

	// QCatalog is Q_CATALOG
	QCatalog = 2

	// QAutoIncrement is Q_AUTO_INCREMENT
	QAutoIncrement = 3

	// QCharsetCode is Q_CHARSET_CODE
	QCharsetCode = 4

	// QTimeZoneCode is Q_TIME_ZONE_CODE
	QTimeZoneCode = 5

	// QCatalogNZCode is Q_CATALOG_NZ_CODE
	QCatalogNZCode = 6
)

// Constants for the type of an INTVAR_EVENT.
const (
	// IntVarInvalidInt is INVALID_INT_EVENT
	IntVarInvalidInt = 0

	// IntVarLastInsertID is LAST_INSERT_ID_EVENT
	IntVarLastInsertID = 1

	// IntVarInsertID is INSERT_ID_EVENT
	IntVarInsertID = 2
)

const (
	BINLOG_CHECKSUM_ALG_OFF byte = 0 // Events are without checksum though its generator
	// is checksum-capable New Master (NM).
	BINLOG_CHECKSUM_ALG_CRC32 byte = 1 // CRC32 of zlib algorithm.
	//  BINLOG_CHECKSUM_ALG_ENUM_END,  // the cut line: valid alg range is [1, 0x7f].
	BINLOG_CHECKSUM_ALG_UNDEF byte = 255 // special value to tag undetermined yet checksum
	// or events from checksum-unaware servers
)

var (
	checksumVersionSplitMySQL   = []int{5, 6, 1}
	checksumVersionProductMySQL = (checksumVersionSplitMySQL[0]*256+checksumVersionSplitMySQL[1])*256 + checksumVersionSplitMySQL[2]

	checksumVersionSplitMariaDB   = []int{5, 3, 0}
	checksumVersionProductMariaDB = (checksumVersionSplitMariaDB[0]*256+checksumVersionSplitMariaDB[1])*256 + checksumVersionSplitMariaDB[2]
)

// server version format X.Y.Zabc, a is not . or number
func splitServerVersion(serverVersion string) []int {
	splits := strings.Split(serverVersion, ".")
	if len(splits) < 3 {
		return []int{0, 0, 0}
	}

	x, _ := strconv.Atoi(splits[0])
	y, _ := strconv.Atoi(splits[1])

	index := 0
	for i, c := range splits[2] {
		if !unicode.IsNumber(c) {
			index = i
			break
		}
	}

	z, _ := strconv.Atoi(splits[2][0:index])

	return []int{x, y, z}
}

func getVersionProduct(serverVersion string) int {
	versionSplit := splitServerVersion(serverVersion)

	return (versionSplit[0]*256+versionSplit[1])*256 + versionSplit[2]
}

type ErrMissingTableMapEvent error

type ErrMissingTableId error
