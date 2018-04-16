package driver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/juju/errors"
	. "github.com/woqutech/drt/tools"
	"strconv"
	"strings"
)

// |Type			  |Name						   	  |Description
// |int<1>			  |header					   	  |[00] or [fe] the OK packet header
// |int<lenenc>		  |affected_rows			   	  |affected rows
// |int<lenenc>		  |last_insert_id			   	  |last insert-id
// |if capabilities & CLIENT_PROTOCOL_41 {         	  |
// |  int<2>		  |status_flags				   	  |Status Flags
// |  int<2>		  |warnings	number of warnings 	  |
// |} elseif capabilities & CLIENT_TRANSACTIONS {  	  |
// |  int<2>		  |status_flags	Status Flags   	  |
// |}                 |                            	  |
// |if capabilities & CLIENT_SESSION_TRACK {       	  |
// |  string<lenenc>  |	info					   	  |human readable status information
// |  if status_flags & SERVER_SESSION_STATE_CHANGED {|
// |    string<lenenc>|	session_state_changes	   	  |session state info
// |  }               |                            	  |
// |} else {          |                            	  |
// |  string<EOF>	  |info						   	  |human readable status information
// https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
func (mc *MySQLConnector) handleOKPacket(data []byte) (*Result, error) {
	var n int
	offset := 0
	if data[offset] != OK_HEADER {
		return nil, ErrMalformPkt
	}
	// skip 0x00 [1 byte]
	offset++

	result := new(Result)
	// Affected rows [Length Coded Binary]
	result.AffectedRows, _, n = ReadLengthEncodedInteger(data[offset:])
	offset += n

	// Insert id [Length Coded Binary]
	result.InsertId, _, n = ReadLengthEncodedInteger(data[offset:])
	offset += n

	if mc.capabilities&CLIENT_PROTOCOL_41 > 0 {
		result.Status = binary.LittleEndian.Uint16(data[offset:])
		mc.status = result.Status
		offset += 2

	} else if mc.capabilities&CLIENT_TRANSACTIONS > 0 {
		result.Status = binary.LittleEndian.Uint16(data[offset:])
		mc.status = result.Status
		offset += 2
	}

	// Skip info
	return result, nil
}

// |Type		|Name				|Description                 |
// |int<1>		|header				|0xFF ERR packet header      |
// |int<2>		|error_code			|error-code                  |
// |if capabilities & CLIENT_PROTOCOL_41 {                       |
// |string[1]	|sql_state_marker	|# marker of the SQL state   |
// |string[5]	|sql_state			|SQL state                   |
// |}                                                            |
// |string<EOF>|error_message		|human readable error message|
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html
func (mc *MySQLConnector) HandleERRPacket(data []byte) error {
	return mc.handleERRPacket(data)
}

func (mc *MySQLConnector) handleERRPacket(data []byte) error {
	offset := 0
	if data[offset] != ERR_HEADER {
		return ErrMalformPkt
	}
	// 1. 0xff [1 byte]
	offset++
	// 2. read error code
	errorCode := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2
	var sqlState []byte
	// SQL State [optional: # + 5 bytes string]
	if data[offset] == 0x23 {
		// 3. skip sql state marker('#')
		offset++
		// 4. read sqlState
		sqlState = data[offset : offset+5]
		//sqlstate := string(data[4 : 4+5])
		offset += 5
	}
	// 5. read message
	errorMessage := string(data[offset:])

	return fmt.Errorf("ErrorPacket [errorCode=%d, message='%v', sqlState=%s]", errorCode, errorMessage, string(sqlState))
}

// |Type  |Name                                       |
// |4     |binlog position to start at (little endian)|
// |2     |binlog flags (currently not used; always 0)|
// |4     |server_id of the slave (little endian)     |
// |n     |binlog file name (optional)                |
func (mc *MySQLConnector) WriteBinlogDumpPacket(binlogName string, slaveId, position uint32) error {
	mc.resetSequence()

	data := make([]byte, 4+1+4+2+4+len(binlogName))
	offset := 4

	data[offset] = COM_BINLOG_DUMP
	offset++

	binary.LittleEndian.PutUint32(data[offset:], position)
	offset = offset + 4

	binary.LittleEndian.PutUint16(data[offset:], BINLOG_DUMP_NEVER_STOP)
	offset = offset + 2

	binary.LittleEndian.PutUint32(data[offset:], slaveId)
	offset = offset + 4

	copy(data[offset:], binlogName)
	return mc.writePacket(data[:])

}

func (mc *MySQLConnector) WriteRegisterSlavePacket(slaveId uint32) error {
	mc.resetSequence()
	hostname, _ := mc.hostname()
	port, _ := strconv.Atoi(strings.Split(mc.address, ":")[1])

	data := make([]byte, 4+1+4+1+len(hostname)+1+len(mc.username)+1+len(mc.password)+2+4+4)
	offset := 4

	data[offset] = COM_REGISTER_SLAVE
	offset++

	binary.LittleEndian.PutUint32(data[offset:], slaveId)
	offset += 4

	data[offset] = uint8(len(hostname))
	offset++

	n := copy(data[offset:], hostname)
	offset += n

	data[offset] = uint8(len(mc.username))
	offset++

	n = copy(data[offset:], mc.username)
	offset += n

	data[offset] = uint8(len(mc.password))
	offset++

	n = copy(data[offset:], mc.password)
	offset += n

	binary.LittleEndian.PutUint16(data[offset:], uint16(port))
	offset = offset + 2

	// Replication rank (not used)
	binary.LittleEndian.PutUint32(data[offset:], 0)
	offset = offset + 4

	// master id use 0 here
	binary.LittleEndian.PutUint32(data[offset:], 0)
	return nil
}

// |Type  					   |Name                       |
// |4                          |client_flags               |
// |4                          |max_packet_size            |
// |1                          |charset_number             |
// |23                         |(filler) always 0x00...    |
// |n (Null-Terminated String) |user                       |
// |n (Length Coded Binary)    |scramble_buff (1 + x bytes)|
// |n (Null-Terminated String) |database_name (optional)    |
func (mc *MySQLConnector) writeAuthPacket() error {
	// Adjust client capability flags based on server support

	// CLIENT_LONG_PASSWORD CLIENT_LONG_FLAG CLIENT_PROTOCOL_41
	// CLIENT_INTERACTIVE CLIENT_TRANSACTIONS CLIENT_SECURE_CONNECTION
	// CLIENT_MULTI_STATEMENTS (1|4|512|8192|32768|65536)

	capabilities := CLIENT_LONG_PASSWORD | CLIENT_LONG_FLAG | CLIENT_PROTOCOL_41 |
		CLIENT_TRANSACTIONS | CLIENT_SECURE_CONNECTION | CLIENT_MULTI_STATEMENTS

	capabilities &= mc.capabilities

	// Calculate hash
	scrambleBuff := Scramble41(mc.scramble, []byte(mc.password))
	// Length: capability (4) + max-packet size (4) + charset (1) + reserved all[0]
	// (23) + username + 1(0x00) + password + 1(0x00) + 21("mysql_native_password") + 1(0x00)
	packetLength := 4 + 4 + 1 + 23 + (len(mc.username) + 1) + (len(scrambleBuff) + 1) + 21 + 1

	// To specify a db name
	if n := len(mc.dbname); n > 0 {
		capabilities |= CLIENT_CONNECT_WITH_DB
		packetLength += n + 1
	}
	mc.capabilities = capabilities

	offset := 0
	data := make([]byte, packetLength+4)
	offset += 4

	// Client capability flags [32 bit]
	binary.LittleEndian.PutUint32(data[offset:offset+4], capabilities)
	offset += 4

	// Max packet size [32 bit] (none)
	binary.LittleEndian.PutUint32(data[offset:offset+4], 0)
	offset += 4

	// Client charset [1 byte];
	data[offset] = byte(parseCharset(mc.charset))
	offset++

	// Filler [23 bytes] (always 0x00)
	offset += 23

	// User [null terminated string]
	if len(mc.username) > 0 {
		offset += copy(data[offset:], mc.username)
	}
	data[offset] = 0x00
	offset++

	// Auth [length encoded integer]
	data[offset] = byte(len(scrambleBuff))
	offset += 1 + copy(data[offset+1:], scrambleBuff)

	// databaseName [null terminated string]
	if len(mc.dbname) > 0 {
		offset += copy(data[offset:], mc.dbname)
		data[offset] = 0x00
		offset++
	}
	// Assume native client during response
	offset += copy(data[offset:], NATIVE_CLIENT_NAME)
	data[offset] = 0x00

	// Send Auth packet
	return mc.writePacket(data)
}

func (mc *MySQLConnector) writeComQueryPacket(query string) (*Result, error) {
	err := mc.writeCommandPacket(query, COM_QUERY)
	if err != nil {
		mc.close()
		return nil, err
	}
	return mc.readResultSetHeaderPacket(false)
}

func (mc *MySQLConnector) writeMultiComQueriesPacket(query string) ([]*Result, error) {
	err := mc.writeCommandPacket(query, COM_QUERY)
	if err != nil {
		mc.close()
		return nil, err
	}
	var results []*Result
	var hasMoreResult = true
	for {
		result, err := mc.readResultSetHeaderPacket(false)

		if err != nil {
			errLog.Print(err)
			mc.close()
			break
		}

		hasMoreResult = (mc.status & SERVER_MORE_RESULTS_EXISTS) != 0

		results = append(results, result)

		// no more data
		if !hasMoreResult {
			break
		}
	}

	return results, nil
}

func (mc *MySQLConnector) writeCommandPacket(cmd string, cmdType byte) error {
	// Reset Packet Sequence
	mc.resetSequence()

	data := make([]byte, 4+len(cmd)+1)

	data[4] = cmdType
	copy(data[5:], cmd)

	return mc.writePacket(data)
}

// |Type  |Name                                       |
// |1     |semi mark                                  |
// |8     |binlog position to start at (little endian)|
// |n     |binlog file name                           |
type SemiAckComPacket struct {
	Position   uint64
	BinlogName string
}

func (mc *MySQLConnector) WriteSemiAckComPacket(p *SemiAckComPacket) error {
	data := make([]byte, 4+1+8+len(p.BinlogName))
	offset := 4

	data[offset] = COM_SEMI_MARK
	offset++

	binary.LittleEndian.PutUint64(data[offset:], uint64(p.Position))
	offset += 8

	copy(data[offset:], p.BinlogName)
	mc.resetSequence()
	return mc.writePacket(data[:])
}

// |Type  					  |Name					      |
// |1                         |protocol_version           |
// |n (Null-Terminated String)|server_version             |
// |4                         |thread_id                  |
// |8                         |scramble_buff              |
// |1                         |(filler) always 0x00       |
// |2                         |server_capabilities        |
// |1                         |server_language            |
// |2                         |server_status              |
// |13                        |(filler) always 0x00 ...   |
// |13                        |rest of scramble_buff (4.1)|
type HandshakeInitPacket struct {
	connectionId uint32
	scramble     []byte
	capabilities uint32
	status       uint16
}

func (mc *MySQLConnector) handleHandshakeInitPacket(data []byte) {
	p := new(HandshakeInitPacket)

	offset := 0
	// 1. skip protocol_version
	offset++
	// 2. skip server_version
	offset += (bytes.IndexByte(data[offset:], NULL_TERMINATED_STRING_DELIMITER)) + 1
	// 3. read thread_id
	p.connectionId = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4
	// 4. read scramble_buff
	p.scramble = data[offset : offset+8]
	offset += 8
	// 5. 1 byte (filler) always 0x00
	offset += 1
	// 6. read server_capabilities
	p.capabilities = uint32(binary.LittleEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if len(data) > offset {
		// 7. skip server_language
		offset++
		// 8. read server_status
		p.status = binary.LittleEndian.Uint16(data[offset : offset+2])
		offset += 2
		// 9. Capability flags (upper 2 bytes)
		p.capabilities = uint32(binary.LittleEndian.Uint16(data[offset:offset+2]))<<16 | p.capabilities
		offset += 2
		// 10. skip auth data
		offset++
		// 11. skip reserved
		offset += 10
		// 12. append rest of scramble
		p.scramble = append(p.scramble, data[offset:offset+12]...)
	}
	mc.HandshakeInitPacket = p
}

// handshake
func (mc *MySQLConnector) readHandshakeInitPacket() error {
	data, err := mc.readPacket()
	if err != nil {
		return err
	}

	if data[0] == ERR_HEADER {
		return errors.New("initial handshake error")
	}
	if data[0] < MinProtocolVersion {
		return errors.Errorf(
			"unsupported MySQL Protocol Version %d. Protocol Version %d or higher is required",
			data[0],
			MinProtocolVersion,
		)
	}

	mc.handleHandshakeInitPacket(data[:])

	return nil
}

// Returns error if Packet is not an 'Result OK'-Packet
func (mc *MySQLConnector) readResultOK() (*Result, error) {
	data, err := mc.readPacket()
	if err != nil {
		return nil, err
	}
	switch data[0] {
	case OK_HEADER:
		return mc.handleOKPacket(data[:])
	case EOF_HEADER:
		if len(data) > 1 {
			plugin := string(data[1:bytes.IndexByte(data, 0x00)])
			return nil, errors.Errorf("authentication plugin is not supported:%v ", plugin)
		} else {
			return nil, errors.Errorf("unsupported mysql old password")
		}
	default:
		return nil, mc.handleERRPacket(data[:])
	}

	return nil, errors.New("invalid ok packet")
}

/******************************************************************************
*                              Result Packets                                 *
******************************************************************************/

// Result Set Header Packet
// http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
func (mc *MySQLConnector) readResultSetHeaderPacket(binary bool) (*Result, error) {
	data, err := mc.readPacket()
	if err != nil {
		return nil, err
	}
	switch data[0] {
	case OK_HEADER:
		return mc.handleOKPacket(data[:])
	case ERR_HEADER:
		return nil, mc.handleERRPacket(data[:])
	case LOCAL_IN_FILE_HEADER:
		return nil, errors.New("malformed packet error, not supported file in error.")
	}

	return mc.readResultset(data[:], binary)
}

func (mc *MySQLConnector) readResultset(data []byte, binary bool) (*Result, error) {

	result := &Result{
		Status:       0,
		InsertId:     0,
		AffectedRows: 0,
		ResultSet:    &ResultSet{},
	}
	columnCount, _, n := ReadLengthEncodedInteger(data[:])

	if n-len(data) != 0 {
		return nil, ErrMalformPkt
	}

	result.Fields = make([]*FieldPacket, columnCount)
	result.FieldNames = make(map[string]int, columnCount)

	if err := mc.readResultColumns(result); err != nil {
		return nil, err
	}

	if err := mc.readResultRows(result, binary); err != nil {
		return nil, err
	}

	return result, nil
}

// Read Packets as Field Packets until EOF-Packet or an Error appears
// http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition41
func (mc *MySQLConnector) readResultColumns(result *Result) (err error) {
	var index = 0
	var data []byte

	for {
		data, err = mc.readPacket()
		if err != nil {
			return
		}

		if isEOFPacket(data) {
			if mc.capabilities&CLIENT_PROTOCOL_41 > 0 {
				// read EOF statusFlag for judge if have more data
				result.Status = binary.LittleEndian.Uint16(data[3:])
				mc.status = result.Status
			}

			if index != len(result.Fields) {
				err = fmt.Errorf("ColumnsCount mismatch n:%d len:%d", index, len(result.Fields))
			}

			return
		}

		result.Fields[index], err = FieldData(data).readFieldPacket()
		if err != nil {
			return
		}

		result.FieldNames[UnsafeGetString(result.Fields[index].Name)] = index

		index = index + 1
	}
}

// Read Packets as Field Packets until EOF-Packet or an Error appears
// http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
func (mc *MySQLConnector) readResultRows(result *Result, isBinary bool) (err error) {
	var data []byte

	for {
		data, err = mc.readPacket()

		if err != nil {
			return
		}

		if isEOFPacket(data) {
			if mc.capabilities&CLIENT_PROTOCOL_41 > 0 {
				result.Status = binary.LittleEndian.Uint16(data[3:])
				mc.status = result.Status
			}
			break
		}

		result.RowDatas = append(result.RowDatas, data)
	}

	result.Values = make([][]interface{}, len(result.RowDatas))

	for i := range result.Values {
		result.Values[i], err = result.RowDatas[i].parse(result.Fields, isBinary)

		if err != nil {
			return err
		}
	}

	return nil
}

type FieldData []byte

//EventBody                      Name
// -----                      ----
// n (Length Coded String)    catalog
// n (Length Coded String)    db
// n (Length Coded String)    table
// n (Length Coded String)    org_table
// n (Length Coded String)    name
// n (Length Coded String)    org_name
// 1                          (filler)
// 2                          charsetnr
// 4                          length
// 1                          type
// 2                          flags
// 1                          decimals
// 2                          (filler), always 0x00
// n (Length Coded Binary)    default
type FieldPacket struct {
	Data               FieldData
	Schema             []byte
	Table              []byte
	OrgTable           []byte
	Name               []byte
	OrgName            []byte
	Charset            uint16
	ColumnLength       uint32
	Type               uint8
	Flag               uint16
	Decimal            uint8
	DefaultValueLength uint64
	DefaultValue       []byte
}

func (p FieldData) readFieldPacket() (f *FieldPacket, err error) {
	f = new(FieldPacket)

	f.Data = p

	var n int
	offset := 0

	// Skip catalog (always def)
	n, err = SkipLengthEncodedString(p)
	if err != nil {
		return
	}
	offset += n

	// Schema
	f.Schema, _, n, err = ReadLengthEncodedString(p[offset:])
	if err != nil {
		return
	}
	offset += n

	// Table
	f.Table, _, n, err = ReadLengthEncodedString(p[offset:])
	if err != nil {
		return
	}
	offset += n

	// Org table
	f.OrgTable, _, n, err = ReadLengthEncodedString(p[offset:])
	if err != nil {
		return
	}
	offset += n

	// Name
	f.Name, _, n, err = ReadLengthEncodedString(p[offset:])
	if err != nil {
		return
	}
	offset += n

	// Org name
	f.OrgName, _, n, err = ReadLengthEncodedString(p[offset:])
	if err != nil {
		return
	}
	offset += n

	// Skip Filler [uint8]
	offset++

	// Charset
	f.Charset = binary.LittleEndian.Uint16(p[offset : offset+2])
	offset += 2

	// Column length
	f.ColumnLength = binary.LittleEndian.Uint32(p[offset : offset+4])
	offset += 4

	// Type
	f.Type = p[offset]
	offset++

	// Flag
	f.Flag = binary.LittleEndian.Uint16(p[offset : offset+2])
	offset += 2

	// Decimals
	f.Decimal = p[offset]
	offset++

	// skip Filter [0x00][0x00]
	offset += 2

	f.DefaultValue = nil

	// If more data, command was field list
	if offset < len(p) {

		// Length of default value (lenenc-int)
		f.DefaultValueLength, _, n = ReadLengthEncodedInteger(p[offset:])
		offset += n

		if len(p) < (offset + int(f.DefaultValueLength)) {
			err = ErrMalformPkt
			return
		}

		// Default value (string[len])
		f.DefaultValue = p[offset:(offset + int(f.DefaultValueLength))]
	}

	return
}
