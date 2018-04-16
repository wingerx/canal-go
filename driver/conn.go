package driver

import (
	"bufio"
	"context"
	"github.com/golang/glog"
	"github.com/juju/errors"
	. "github.com/woqutech/drt/tools"
	"io"
	"net"
	"os"
	"time"
)

const (
	DEFAULT_RECEIVE_BUFFER_SIZE = 16 * 1024 //16k
	DEFAULT_CONNECT_TIMEOUT     = 10 * time.Second
	DEFAULT_CHARSET             = "utf8" //utf8
	MaxPacketSize               = 1<<24 - 1
)

// connectChan is used by Connect.
type connectChan struct {
	conn *MySQLConnector
	err  error
}

type Conn struct {
	net.Conn
	reader   *bufio.Reader
	sequence uint8 // packet sequence number
}

func newConn(conn net.Conn) *Conn {
	c := new(Conn)

	c.reader = bufio.NewReaderSize(conn, DEFAULT_RECEIVE_BUFFER_SIZE)
	c.Conn = conn

	return c
}

type MySQLConnector struct {
	*Conn

	address     string
	username    string
	password    string
	charset     string
	dbname      string
	connTimeout time.Duration
	connected   AtomicBool

	//handshake packet
	*HandshakeInitPacket
}

func NewMySQLConnector(address, username, password, dbName string) *MySQLConnector {
	return &MySQLConnector{
		charset:     DEFAULT_CHARSET,
		address:     address,
		username:    username,
		password:    password,
		dbname:      dbName,
		connected:   NewAtomicBool(false),
		connTimeout: DEFAULT_CONNECT_TIMEOUT,
	}
}

// Start a background connection routine.  It first
// establishes a network connection, returns it on the channel,
// then starts the negotiation, and returns the result on the channel.
// It can send on the channel, before closing it:
// - a connectChan with an error and nothing else (when dial fails).
// - a connectChan with a *Conn and no error, then another one
//   with possibly an error.
func (mc *MySQLConnector) Connect(ctx context.Context) error {
	glog.Infof("start to connect [%v %s]", mc.address, mc.dbname)
	status := make(chan connectChan)
	go func() {
		defer close(status)

		// connect address
		conn, err := net.DialTimeout("tcp", mc.address, mc.connTimeout)
		if err != nil {
			status <- connectChan{
				err: errors.Errorf("net.Dial(%v) failed: %v", mc.address, err),
			}
			return
		}

		// Send the connection back, so the other side can close it.
		mc.Conn = newConn(conn)
		status <- connectChan{
			conn: mc,
		}

		// During the handshake, and if the context is
		// canceled, the connection will be closed. That will
		// make any read or write just return with an error
		// right away.
		status <- connectChan{
			err: negotiate(mc),
		}
	}()

	// Wait on the context and the status, for the connection to happen.
	select {
	case <-ctx.Done():
		// The background routine may send us a few things,
		// wait for them and terminate them properly in the
		// background.
		go func() {
			dial := <-status
			if dial.err != nil {
				// return nothing if dial failed
				return
			}
			// if dial worked, close the connection, wait for the end.
			// We wait as not to leave a channel with an unread value.
			dial.conn.Close()
			<-status
		}()
		return ctx.Err()
	case cr := <-status:
		// Dial failed, no connection was established.
		if cr.err != nil {
			return cr.err
		}
		// Dial working, wait handshake
		glog.Infof("dial [%v] success, wait for handshake", mc.address)
	}

	// Wait for the end of the handshake
	select {
	case <-ctx.Done():
		// We are interrupted. Close the connection, wait for
		// the handshake to finish in the background.
		mc.Close()
		go func() {
			// Since we closed the connection, this one should be fast.
			// We wait as not to leave a channel with an unread value.
			<-status
		}()
		return ctx.Err()
	case cr := <-status:
		if cr.err != nil {
			mc.Close()
			return cr.err
		}
	}
	glog.Infof("connect [%v] success, connectionId: [%d]", mc.address, mc.connectionId)
	return nil
}

// establish the connection
//func (mc *MySQLConnector) Connect() error {
//	// connect address
//	conn, err := net.DialTimeout("tcp", mc.address, mc.connTimeout)
//
//	if err != nil {
//		return err
//	}
//	mc.Conn = newConn(conn)
//	return negotiate(mc)
//}

func negotiate(mc *MySQLConnector) error {
	// read handshake packet
	if err := mc.readHandshakeInitPacket(); err != nil {
		mc.close()
		return err
	}
	glog.Info("handshake success, sent out auth packet")
	// write auth packet
	if err := mc.writeAuthPacket(); err != nil {
		mc.close()
		return err
	}
	// read ok packet
	if _, err := mc.readResultOK(); err != nil {
		mc.close()
		errLog.Print(err)
		return err
	}
	// set connection status
	mc.connected.Set(true)

	return nil
}

// reads a raw packet from the MySQL connection. A raw packet is
// (1) a binary log event sent from a master to a slave <- what we care about
// (2) a single statement sent to the MySQL server, or
// (3) a single row sent to the client.
// This method returns a generic error, not a SQLError.
func (c *Conn) ReadPacket() ([]byte, error) {
	return c.readPacket()
}

func (c *Conn) readPacket() ([]byte, error) {
	// Optimize for a single packet case.
	data, err := c.readOnePacket()
	if err != nil {
		return nil, err
	}

	// This is a single packet.
	if len(data) < MaxPacketSize {
		return data, nil
	}

	// There is more than one packet, read them all.
	for {
		next, err := c.readOnePacket()
		if err != nil {
			errLog.Print(err)
			return nil, err
		}

		if len(next) == 0 {
			// Again, the packet after a packet of exactly size MaxPacketSize.
			break
		}

		data = append(data, next...)
		if len(next) < MaxPacketSize {
			break
		}
	}

	return data, nil
}

// read a single packet into a newly allocated buffer.
func (c *Conn) readOnePacket() ([]byte, error) {
	var header [4]byte

	if _, err := io.ReadFull(c.reader, header[:]); err != nil {
		return nil, errors.Errorf("io.ReadFull(header size) failed: %v", err)
	}

	sequence := uint8(header[3])
	if sequence != c.sequence {
		return nil, errors.Errorf("invalid sequence, expected %v got %v", c.sequence, sequence)
	}

	c.sequence++

	payloadLength := ReadBinaryUint24(header[0:3])
	if payloadLength == 0 {
		// This can be caused by the packet after a packet of
		// exactly size MaxPacketSize.
		return nil, nil
	}

	data := make([]byte, payloadLength)
	if _, err := io.ReadFull(c.reader, data); err != nil {
		return nil, errors.Errorf("io.ReadFull(packet body of length %v) failed: %v", payloadLength, err)
	}
	return data, nil
}

// write the passed packet with a header to the network.
func (c *Conn) writePacket(data []byte) error {
	payloadLength := len(data) - 4

	for payloadLength >= MaxPacketSize {
		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff
		data[3] = c.sequence

		if n, err := c.write(data[:4+MaxPacketSize]); err != nil {
			return err
		} else if n != (4 + MaxPacketSize) {
			return ErrBadConn
		} else {
			c.sequence++
			payloadLength = payloadLength - MaxPacketSize
			data = data[MaxPacketSize:]
		}
	}

	data[0] = byte(payloadLength)
	data[1] = byte(payloadLength >> 8)
	data[2] = byte(payloadLength >> 16)
	data[3] = c.sequence

	if n, err := c.write(data); err != nil {
		return err
	} else if n != len(data) {
		return ErrBadConn
	} else {
		c.sequence++
		return nil
	}
}

func (c *Conn) write(data []byte) (int, error) {
	n, err := c.Write(data)
	if err != nil {
		errLog.Print(errors.Errorf("write to connection has error occur:%v", err))
		return 0, err
	}
	return n, nil
}

// resets the packet sequence number.
func (c *Conn) resetSequence() {
	c.sequence = 0
}
func (mc *MySQLConnector) IsConnected() bool {
	return mc.connected.Get()
}
func (mc *MySQLConnector) Close() error {
	mc.connected.Set(false)
	return mc.close()
}
func (c *Conn) close() error {
	c.resetSequence()

	if c.Conn != nil {
		return c.Conn.Close()
	}
	return nil
}

func (mc *MySQLConnector) SetConnTimeout(timeout int) {
	mc.connTimeout = time.Second * time.Duration(timeout)
}

func (mc *MySQLConnector) SetCharSet(charset string) {
	mc.charset = charset
}

func (mc *MySQLConnector) hostname() (string, error) {
	return os.Hostname()
}
func isEOFPacket(data []byte) bool {
	return data[0] == EOF_HEADER && len(data) <= 5
}

func (mc *MySQLConnector) ConnectionId() uint32 {
	return mc.connectionId
}
