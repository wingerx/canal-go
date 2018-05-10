package parse

import (
	"github.com/golang/glog"
	"github.com/juju/errors"
	. "github.com/wingerx/drt/events"
)

type Streamer struct {
	ch  chan *LogEvent
	ech chan error
	err error
}

func NewStreamer() *Streamer {
	es := new(Streamer)

	es.ch = make(chan *LogEvent, 1024)
	es.ech = make(chan error, 4)

	return es
}

func (s *Streamer) GetEvent() (*LogEvent, error) {
	if s.err != nil {
		return nil, s.err
	}

	select {
	case c := <-s.ch:
		return c, nil
	case s.err = <-s.ech:
		return nil, s.err
	}
}

func (s *Streamer) close() {
	s.closeWithError(errors.New("last sync failed"))
}

func (s *Streamer) closeWithError(err error) {
	glog.Errorf("close Dump stream by %v", err)
	if err == nil {
		err = errors.New("sync was closed")
	}
	select {
	case s.ech <- err:
	default:
	}
}
