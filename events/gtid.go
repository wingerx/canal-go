package events

type GTIDSet interface {
	String() string

	Equal(o GTIDSet) bool

	Contain(o GTIDSet) bool

	Update(GTIDStr string) error
}

func ParseGTIDSet(s string) (GTIDSet, error) {
	return ParseMysqlGTIDSet(s)
}
