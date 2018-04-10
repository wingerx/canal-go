package events

// IgnoreLogEvent use to record what we don't care event
type IgnoreLogEvent struct {
	*EventHeader
}

func NewIgnoreLogEvent(data []byte) (*EventHeader, error) {
	return NewEventHeader(data)
}

