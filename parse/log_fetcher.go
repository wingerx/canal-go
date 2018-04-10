package parse

import "fmt"
import (
	. "github.com/woqutech/drt/tools"
	"time"
	"context"
	"github.com/juju/errors"
	"sync"
	log "github.com/golang/glog"
)

type LogFetcher struct {
	conn    *MySQLConnection
	decoder *LogDecoder

	m      sync.RWMutex
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	running bool
}

func NewLogFetcher(conn *MySQLConnection, ld *LogDecoder) *LogFetcher {
	log.Infof("create BinlogFetch with connect auth %v", conn.auth)

	lf := new(LogFetcher)
	lf.conn = conn
	lf.decoder = ld
	lf.running = true
	return lf
}

func (lf *LogFetcher) Fetch() *EventStreamer {
	streamer := NewStreamer()

	lf.wg.Add(1)
	go lf.fetch(streamer)
	return streamer
}

func (lf *LogFetcher) fetch(streamer *EventStreamer) {
	defer func() {
		if e := recover(); e != nil {
			streamer.closeWithError(fmt.Errorf("Err: %v\n Stack: %s", e, PStack()))
		}
		lf.wg.Done()
	}()

	for {
		data, err := lf.conn.ReadPacket()
		if err != nil {
			errLog.Print(err)
			streamer.closeWithError(err)
			return
		}

		//set read timeout
		if lf.conn.auth.ReadTimeout > 0 {
			lf.conn.SetReadDeadline(time.Now().Add(time.Duration(lf.conn.auth.ReadTimeout)))
		}

		switch data[0] {
		case OK_HEADER:
			// skip ok
			event, err := lf.decoder.Decode(data[1:])
			if err != nil {
				streamer.closeWithError(err)
				return
			}
			select {
			case streamer.ch <- event:
			case <-lf.ctx.Done():
				streamer.closeWithError(errors.New("log fetch has been closed"))
			}
		case ERR_HEADER:
			err = lf.conn.HandleERRPacket(data)
			streamer.closeWithError(err)
			return
		case EOF_HEADER:
			log.Info("receive EOF packet, retry ReadPacket")
			continue
		default:
			log.Errorf("invalid stream header %c", data[0])
			continue
		}
	}
}

// Close closes the LogFetcher.
func (lf *LogFetcher) Close() {
	lf.m.Lock()
	defer lf.m.Unlock()

	lf.close()
}

func (lf *LogFetcher) close() {
	log.Info("log fetcher start to closing")
	if lf.isClosed() {
		return
	}

	lf.running = false
	lf.cancel()

	if lf.conn != nil {
		lf.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	}

	lf.wg.Wait()

	if lf.conn != nil {
		lf.conn.Close()
	}
}

func (lf *LogFetcher) isClosed() bool {
	select {
	case <-lf.ctx.Done():
		return true
	default:
		return false
	}
}
