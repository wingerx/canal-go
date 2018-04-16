package driver

import (
	"github.com/juju/errors"
	"log"
	"os"
	"strconv"
	"strings"

	. "github.com/woqutech/drt/tools"
)

const (
	NULL_TERMINATED_STRING_DELIMITER      = 0x00
	MinProtocolVersion               byte = 10
	NATIVE_CLIENT_NAME                    = "mysql_native_password"
	COM_SEMI_MARK                         = 0xef
	SERVER_MORE_RESULTS_EXISTS            = 0x0008
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

var errLog Logger = log.New(os.Stderr, "[Driver] ", log.Ldate|log.Ltime|log.Lshortfile)

// parses the provided character set.
// Returns default charset(utf8) if it can't.
func parseCharset(cs string) uint8 {
	// Check if it's empty, return utf8. This is a reasonable default.
	if cs == "" {
		return CharacterSetUtf8
	}

	// Check if it's in our map.
	charset, ok := CharacterSetMap[strings.ToLower(cs)]
	if ok {
		return charset
	}

	// As a fallback, try to readFieldPacket a number. So we support more values.
	if i, err := strconv.ParseInt(cs, 10, 8); err == nil {
		return uint8(i)
	}
	errLog.Print(errors.Errorf("failed to interpret character set '%v'. Try using an integer value if needed", cs))
	// No luck. return utf8
	return CharacterSetUtf8
}
