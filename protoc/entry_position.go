package protoc

import (
	"encoding/json"
	"fmt"
)

type EntryPosition struct {
	Included    bool
	LogfileName string
	LogPosition int64
	Timestamp   int64
	ServerId    int64
}

func (ep *EntryPosition) String() string {
	js, _ := json.Marshal(ep)
	return fmt.Sprintf("%s", js)
}
