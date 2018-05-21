package parse

import (
	"github.com/wingerx/drt/protoc"
	"github.com/toolkits/net"
	"hash/crc32"
	"strings"
	"strconv"
	"github.com/pkg/errors"
)

type MySQLEventParser struct {
	*MySQLConnection

	MasterAuth     *AuthenticInfo
	SlaveAuth      *AuthenticInfo
	MasterPosition *protoc.EntryPosition
	SlavePosition  *protoc.EntryPosition

	tableMetaCache *TableMetaCache
	eventConvert   *EventConvert
	BinLogFormat   binlogFormatImage
	BinLogImage    binlogFormatImage

	destination string
	slaveId     int
}

func NewMySQLEventParser(destination string, slaveId int) *MySQLEventParser {
	return &MySQLEventParser{
		destination: destination,
		slaveId:     slaveId,
	}
}

func (mp *MySQLEventParser) PrepareDump() error {
	metaConn := mp.Fork()
	if err := metaConn.Connect(); err != nil {
		return err
	}
	// check support binlog format
	if err := metaConn.checkBinlogFormat(mp.BinLogFormat); err != nil {
		return err
	}
	// check support binlog img
	if err := metaConn.checkBinlogImage(mp.BinLogImage); err != nil {
		return err
	}
	// init tableMetaCache
	var err error
	mp.tableMetaCache, err = NewTableMetaCache(metaConn, mp.destination)
	if err != nil {
		return err
	}
	mp.eventConvert = NewEventCovert(mp.tableMetaCache)

	return nil
}

func (mp *MySQLEventParser) BuildMySQLConnection(auth *AuthenticInfo) (*MySQLConnection, error) {
	if mp.slaveId <= 0 {
		uid, err := mp.generateUniqueServerId()
		if err != nil {
			return nil, err
		}
		mp.slaveId = int(uid)
	}
	return NewMySQLConnection(auth, uint32(mp.slaveId)), nil
}

func (mp *MySQLEventParser) FindStartPosition() *protoc.EntryPosition {
	return nil
}

func findStartPosition(conn *MySQLConnection) *protoc.EntryPosition {
	return nil
}
func (mp *MySQLEventParser) generateUniqueServerId() (uint32, error) {
	if ips, _ := net.IntranetIP(); len(ips) > 0 {
		ip := ips[0]
		addr := strings.Split(ip, ".")
		salt := crc32.ChecksumIEEE([]byte(mp.destination))
		ad1, _ := strconv.Atoi(addr[1])
		ad2, _ := strconv.Atoi(addr[2])
		ad3, _ := strconv.Atoi(addr[3])
		return ((0x7f & salt) << 24) + (uint32(0xff&ad1) << 16) + (uint32(0xff&ad2) << 8) + uint32(0xff&ad3), nil
	}
	return 0, errors.New("unknown host ip")
}
