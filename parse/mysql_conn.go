package parse

import (
	"context"
	"fmt"
	"github.com/golang/glog"
	"github.com/juju/errors"
	. "github.com/woqutech/drt/driver"
	. "github.com/woqutech/drt/events"
	"time"
)

type AuthenticInfo struct {
	host        string
	port        int
	username    string
	password    string
	dbName      string
	charset     string
	connTimeout int
	ReadTimeout int
}

type MySQLConnection struct {
	*MySQLConnector
	auth *AuthenticInfo

	slaveId uint32
	cancel  context.CancelFunc
	ctx     context.Context
	//wg      sync.WaitGroup
}

type SinkFunction func(event *LogEvent) bool

func NewMySQLConnection(auth *AuthenticInfo, slaveId uint32) *MySQLConnection {
	// new driver
	address := fmt.Sprintf("%v:%d", auth.host, auth.port)
	conn := NewMySQLConnector(address, auth.username, auth.password, auth.dbName)
	conn.SetCharSet(auth.charset)
	conn.SetConnTimeout(auth.connTimeout)
	mc := new(MySQLConnection)
	mc.MySQLConnector = conn
	mc.auth = auth
	mc.slaveId = slaveId
	mc.ctx, mc.cancel = context.WithCancel(context.Background())

	return mc
}

func (mc *MySQLConnection) Connect() error {
	return mc.MySQLConnector.Connect(mc.ctx)
}

func (mc *MySQLConnection) dump(binlogName string, position uint32, sinkFunc SinkFunction) error {
	mc.updateSettings()
	// ignore err
	mc.sendRegisterSlave()
	// send binlog dump cmd
	mc.sendBinlogDumpCommand(binlogName, position)
	// dump & parse event
	ld := NewLogDecoder(UNKNOWN_EVENT, PREVIOUS_GTIDS_LOG_EVENT)
	lf := NewLogFetcher(mc, ld)
	lf.ctx, lf.cancel = mc.ctx, mc.cancel

	streamer := lf.Fetch()
	for {
		event, err := streamer.GetEvent(lf.ctx)

		if err != nil {
			return errors.Trace(err)
		}
		if !sinkFunc(event) {
			break
		}
	}
	return nil
}

// SendBinlogDump writes a ComBinlogDump command.
// See http://dev.mysql.com/doc/internals/en/com-binlog-dump.html for syntax
// Returns a SQLError.
func (mc *MySQLConnection) sendBinlogDumpCommand(binlogName string, position uint32) error {
	glog.Infof("binlog dump with position [%s:%d]", binlogName, position)
	return mc.WriteBinlogDumpPacket(binlogName, mc.slaveId, position)
}

func (mc *MySQLConnection) sendRegisterSlave() error {
	glog.Infof("register slave with id [%d]", mc.slaveId)
	return mc.WriteRegisterSlavePacket(mc.slaveId)
}
func (mc *MySQLConnection) updateSettings() {
	// Tell the server that we understand the format of events
	// that will be used if binlog_checksum is enabled on the server.
	if _, err := mc.Update("set wait_timeout=9999999"); err != nil {
		errLog.Print(errors.Errorf("failed to set wait_timeout=9999999: %v", err))
	}
	if _, err := mc.Update("set net_write_timeout=1800"); err != nil {
		errLog.Print(errors.Errorf("failed to set net_write_timeout=1800: %v", err))
	}
	if _, err := mc.Update("set net_read_timeout=1800"); err != nil {
		errLog.Print(errors.Errorf("failed to set net_read_timeout=1800: %v", err))
	}
	if _, err := mc.Update("set names 'binary'"); err != nil {
		errLog.Print(errors.Errorf("failed to set names 'binary': %v", err))
	}
	if _, err := mc.Update("set @master_binlog_checksum=@@global.binlog_checksum"); err != nil {
		errLog.Print(errors.Errorf("failed to set @master_binlog_checksum=@@global.binlog_checksum: %v", err))
	}
	if _, err := mc.Update("set @slave_uuid=uuid()"); err != nil {
		errLog.Print(errors.Errorf("failed to set @slave_uuid=uuid(): %v", err))
	}
	// MASTER_HEARTBEAT_PERIOD sets the interval in seconds between replication heartbeats.
	// Whenever the master's binary log is updated with an event, the waiting period for the next heartbeat is reset.
	// interval is a decimal value having the range 0 to 4294967 seconds and a resolution in milliseconds;
	// the smallest nonzero value is 0.001. Heartbeats are sent by the master
	// only if there are no unsent events in the binary log file for a period longer than interval.
	// https://dev.mysql.com/doc/refman/5.7/en/change-master-to.html
	if _, err := mc.Update(fmt.Sprintf("set @master_heartbeat_period=%d", time.Second.Nanoseconds()*MASTER_HEARTBEAT_PERIOD_SECONDS)); err != nil {
		errLog.Print(errors.Errorf("failed to set @master_heartbeat_period: %v", err))
	}
}

// https://dev.mysql.com/doc/internals/en/binlog-formats.html
func (mc *MySQLConnection) checkBinlogFormat() error {
	if bgFmt, err := mc.loadBinlogFmtImage("show variables like 'binlog_format'"); err != nil {
		return err
	} else if !binlogFormat.contains(bgFmt) {
		return errors.Errorf("unexpected binlog format result:%v", bgFmt)
	}
	return nil
}

// https://dev.mysql.com/doc/internals/en/binlog-row-image.html
func (mc *MySQLConnection) checkBinlogImage() error {
	if bgImg, err := mc.loadBinlogFmtImage("show variables like 'binlog_row_image'"); err != nil {
		return err
	} else if !binlogImage.contains(bgImg) {
		return errors.Errorf("unexpected binlog row image result:%v", bgImg)
	}
	return nil
}

func (mc *MySQLConnection) loadBinlogFmtImage(query string) (string, error) {
	if ret, err := mc.Query(query); err != nil {
		return "", err
	} else {
		return ret.GetString(0, 1)
	}
}

func fork(auth *AuthenticInfo, slaveId uint32) *MySQLConnection {
	return NewMySQLConnection(auth, slaveId)
}

func (mc *MySQLConnection) Disconnect() error {
	glog.Infof("try to disconnect %v", fmt.Sprintf("%v:%d", mc.auth.host, mc.auth.port))
	mc.cancel()
	err := mc.MySQLConnector.Close()
	if mc.ConnectionId() > 0 {
		glog.Infof("kill dump connectionId: %d", mc.ConnectionId())
		kc := fork(mc.auth, mc.slaveId)
		err := kc.Connect()
		if err != nil {
			glog.Errorf("fork connection failed.%v", err)
		}
		kc.Update(fmt.Sprintf("KILL CONNECTION %d", mc.ConnectionId()))
		kc.Close()
	}
	return err
}

func (mc *MySQLConnection) Reconnect() error {
	mc.Disconnect()
	return mc.Connect()
}
