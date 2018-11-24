package client

import (
	"bytes"
	"encoding/binary"
	"net"
	"sync"
	"time"
)

type testConn struct {
	opaque chan uint32
	buf    []byte
	key    []byte
}

func (c *testConn) Read(b []byte) (n int, err error) {
	if c.buf == nil {
		buf := make([]byte, 28)
		buf[0] = MagicResponse
		buf[1] = OpcodeGet
		binary.BigEndian.PutUint32(buf[8:12], 4+2)
		binary.BigEndian.PutUint32(buf[12:16], <-c.opaque)
		c.buf = append(buf, []byte("ok")...)
	} else {
		binary.BigEndian.PutUint32(c.buf[12:16], <-c.opaque)
	}

	return copy(b, c.buf), nil
}

func (c *testConn) Write(b []byte) (n int, err error) {
	c.opaque <- binary.BigEndian.Uint32(b[12:16])
	if c.key == nil {
		keySize := binary.BigEndian.Uint16(b[2:4])
		key := make([]byte, keySize)
		copy(key, b[24:24+keySize])
		c.key = key
	}

	return len(b), nil
}

func (*testConn) Close() error {
	panic("implement me")
}

func (*testConn) LocalAddr() net.Addr {
	panic("implement me")
}

func (*testConn) RemoteAddr() net.Addr {
	panic("implement me")
}

func (*testConn) SetDeadline(t time.Time) error {
	panic("implement me")
}

func (*testConn) SetReadDeadline(t time.Time) error {
	panic("implement me")
}

func (*testConn) SetWriteDeadline(t time.Time) error {
	panic("implement me")
}

func (c *testConn) DropPacket() {
	for {
		<-c.opaque
	}
}

func NewTestClient() *Client {
	return &Client{
		conn:         &testConn{opaque: make(chan uint32)},
		mu:           &sync.Mutex{},
		sequence:     0,
		asyncRequest: make(map[uint32]chan *Item),
		writeBufferPool: &sync.Pool{
			New: func() interface{} {
				return &bytes.Buffer{}
			},
		},
	}
}
