package driver

func (mc *MySQLConnector) Query(query string) (*Result, error) {
	return mc.executor(query)
}

func (mc *MySQLConnector) QueryMulti(query string) ([]*Result, error) {
	return mc.writeMultiComQueriesPacket(query)
}

func (mc *MySQLConnector) Update(update string) (*Result, error) {
	return mc.executor(update)
}

func (mc *MySQLConnector) executor(command string) (*Result, error) {
	if mc.IsConnected() {
		return mc.writeComQueryPacket(command)
	}
	return nil, ErrInvalidConn
}
