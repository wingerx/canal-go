package parse

import (
	. "github.com/woqutech/drt/events"
	"github.com/juju/errors"
	"context"
)

type EventStreamer struct {
	ch  chan *LogEvent
	ech chan error
	err error
}

func NewStreamer() *EventStreamer {
	es := new(EventStreamer)

	es.ch = make(chan *LogEvent, 1024)
	es.ech = make(chan error, 4)

	return es
}

// GetEvent gets the binlog event one by one, it will block until Syncer receives any events from MySQL
// or meets a sync error. You can pass a context (like Cancel or Timeout) to break the block.
func (es *EventStreamer) GetEvent(ctx context.Context) (*LogEvent, error) {
	if es.err != nil {
		return nil, es.err
	}

	select {
	case c := <-es.ch:
		return c, nil
	case es.err = <-es.ech:
		return nil, es.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (es *EventStreamer) close() {
	es.closeWithError(errors.New("last sync failed"))
}

func (es *EventStreamer) closeWithError(err error) {
	if err == nil {
		err = errors.New("sync was closed")
	}
	select {
	case es.ech <- err:
	default:
	}
}
