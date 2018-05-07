package protoc

type EntryPosition struct {
	Included    bool
	LogfileName string
	LogPosition int64
	Timestamp   int64
	ServerId    int64
}
