package parse

import (
	"github.com/golang/glog"
	. "github.com/woqutech/drt/tools"
	. "github.com/woqutech/drt/events"
	"github.com/pkg/errors"
	"time"
)

type LogFetcher struct {
	conn    *MySQLConnection
	decoder *LogDecoder
}

func NewLogFetcher(conn *MySQLConnection, ld *LogDecoder) *LogFetcher {
	lf := new(LogFetcher)
	lf.conn = conn
	lf.decoder = ld
	return lf
}

func (lf *LogFetcher) Fetch() (*LogEvent, error) {
	data, err := lf.conn.ReadPacket()
	if err != nil {
		errLog.Print(err)
		return nil, err
	}
	switch data[0] {
	case OK_HEADER:
		// skip ok
		event, err := lf.decoder.Decode(data[1:])
		if err != nil {
			return nil, err
		}
		return event, nil
	case ERR_HEADER:
		err = lf.conn.HandleERRPacket(data)
		return nil, err
	case EOF_HEADER:
		glog.Info("receive EOF packet, retry ReadPacket")
		return nil, nil
	default:
		// should not happen
		glog.Errorf("invalid event header %c", data[0])
		return nil, errors.New("invalid event header")
	}
}

// Close the LogSteamer.
func (lf *LogFetcher) Close() error {
	return lf.close()
}

func (lf *LogFetcher) close() error {
	glog.Info("log fetcher start to closing")

	if lf.conn != nil {
		lf.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	}

	if lf.conn != nil {
		return lf.conn.Close()
	}
	return nil
}
