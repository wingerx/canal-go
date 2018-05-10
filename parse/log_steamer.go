package parse

import (
	"fmt"
	"github.com/golang/glog"
	. "github.com/wingerx/drt/tools"
	"sync"
	"time"
)

type LogSteamer struct {
	conn    *MySQLConnection
	decoder *LogDecoder

	m       sync.RWMutex
	wg      sync.WaitGroup
	running bool
}

func NewLogSteamer(conn *MySQLConnection, ld *LogDecoder) *LogSteamer {
	ls := new(LogSteamer)
	ls.conn = conn
	ls.decoder = ld
	ls.running = true
	return ls
}

func (ls *LogSteamer) Fetch() *Streamer {
	glog.Infof("start stream binlog with slave id %d", ls.conn.slaveId)

	s := NewStreamer()

	ls.wg.Add(1)
	go ls.fetch(s)
	return s
}

func (ls *LogSteamer) fetch(streamer *Streamer) {
	defer func() {
		if e := recover(); e != nil {
			streamer.closeWithError(fmt.Errorf("Err: %v\n Stack: %s", e, PStack()))
		}
		ls.wg.Done()
	}()

	for {
		//set read timeout
		//if ls.conn.auth.ReadTimeout > 0 {
		//	ls.conn.SetReadDeadline(time.Now().Add(time.Second * 10))
		//}
		data, err := ls.conn.ReadPacket()
		if err != nil {
			errLog.Print(err)
			streamer.closeWithError(err)
			return
		}
		//if ls.conn.auth.ReadTimeout > 0 {
		//	ls.conn.SetReadDeadline(time.Time{})
		//}
		switch data[0] {
		case OK_HEADER:
			// skip ok
			event, err := ls.decoder.Decode(data[1:])
			if err != nil {
				streamer.closeWithError(err)
				return
			}
			streamer.ch <- event
		case ERR_HEADER:
			err = ls.conn.HandleERRPacket(data)
			streamer.closeWithError(err)
			return
		case EOF_HEADER:
			glog.Info("receive EOF packet, retry ReadPacket")
			continue
		default:
			glog.Errorf("invalid stream header %c", data[0])
			continue
		}
	}

}

// Close the LogSteamer.
func (ls *LogSteamer) Close() error {
	ls.m.Lock()
	defer ls.m.Unlock()
	ls.running = false

	return ls.close()
}

func (ls *LogSteamer) close() error {
	glog.Info("log fetcher start to closing")

	if ls.conn != nil {
		ls.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	}

	ls.wg.Wait()

	if ls.conn != nil {
		return ls.conn.Close()
	}
	return nil
}
