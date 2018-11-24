package client

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
)

const (
	MagicRequest  = 0x80
	MagicResponse = 0x81

	OpcodeGet     = 0x00
	OpcodeSet     = 0x01
	OpcodeAdd     = 0x02
	OpcodeReplace = 0x03
	OpcodeDel     = 0x04
	OpcodeIncr    = 0x05
	OpcodeDecr    = 0x06

	StatusNoError                       = 0x0000
	StatusKeyNotFound                   = 0x0001
	StatusKeyExists                     = 0x0002
	StatusValueTooLarge                 = 0x0003
	StatusInvalidArguments              = 0x0004
	StatusItemNotStored                 = 0x0005
	StatusNonNumericValue               = 0x0006
	StatusVBucketBelongsToAnotherServer = 0x0007
	StatusAuthenticationError           = 0x0008
	StatusAuthenticationContinue        = 0x0009
	StatusUnknownCommand                = 0x0081
	StatusOutOfMemory                   = 0x0082
	StatusNotSupported                  = 0x0083
	StatusInternalError                 = 0x0084
	StatusBusy                          = 0x0085
	StatusTemporaryFailure              = 0x0086
)

var (
	ErrKeyNotFound      = errors.New("client: key not found")
	ErrKeyAlreadyExists = errors.New("client: key already exists")
	ErrValueTooLarge    = errors.New("client: value too large")
)

type Item struct {
	Key   []byte
	Value []byte
	Extra []byte
	CAS   uint64
	Err   error
	Raw   []byte
}

type Client struct {
	conn            net.Conn
	sequence        uint32
	asyncRequest    map[uint32]chan *Item
	mu              *sync.Mutex
	writeBufferPool *sync.Pool
}

func NewClient(host string, port int) (*Client, error) {
	conn, err := net.Dial("tcp", host+":"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}

	client := &Client{
		conn:         conn,
		sequence:     0,
		asyncRequest: make(map[uint32]chan *Item),
		mu:           &sync.Mutex{},
		writeBufferPool: &sync.Pool{
			New: func() interface{} {
				return &bytes.Buffer{}
			},
		},
	}
	go client.readConn()

	return client, nil
}

func (client *Client) GetAsync(key []byte) (<-chan *Item, error) {
	buf := make([]byte, 24)
	buf[0] = MagicRequest
	buf[1] = OpcodeGet
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(key)))
	binary.BigEndian.PutUint32(buf[4:8], 0)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(key)))
	sequence := client.nextOpaque()
	binary.BigEndian.PutUint32(buf[12:16], sequence)
	binary.BigEndian.PutUint64(buf[16:24], 0)

	return client.callAsync(sequence, buf, key)
}

func (client *Client) Get(key []byte) (*Item, error) {
	c, err := client.GetAsync(key)
	if err != nil {
		return nil, err
	}

	v := <-c
	if v.Err != nil {
		return nil, v.Err
	}
	return v, nil
}

func (client *Client) SetAsync(key, value []byte, cas uint64, flag []byte, expiration int) (<-chan *Item, error) {
	return client.setAsync(OpcodeSet, key, value, cas, flag, expiration)
}

func (client *Client) Set(key, value []byte, cas uint64, flag []byte, expiration int) error {
	c, err := client.SetAsync(key, value, cas, flag, expiration)
	if err != nil {
		return err
	}

	v := <-c
	return v.Err
}

func (client *Client) AddAsync(key, value, flag []byte, expiration int) (<-chan *Item, error) {
	return client.setAsync(OpcodeAdd, key, value, 0, flag, expiration)
}

func (client *Client) Add(key, value, flag []byte, expiration int) error {
	c, err := client.AddAsync(key, value, flag, expiration)
	if err != nil {
		return err
	}

	v := <-c
	return v.Err
}

func (client *Client) ReplaceAsync(key, value []byte, cas uint64, flag []byte, expiration int) (<-chan *Item, error) {
	return client.setAsync(OpcodeReplace, key, value, cas, flag, expiration)
}

func (client *Client) Replace(key, value []byte, cas uint64, flag []byte, expiration int) error {
	c, err := client.ReplaceAsync(key, value, cas, flag, expiration)
	if err != nil {
		return err
	}

	v := <-c
	return v.Err
}

func (client *Client) DelAsync(key []byte) (<-chan *Item, error) {
	buf := make([]byte, 24)
	buf[0] = MagicRequest
	buf[1] = OpcodeDel
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(key)))
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(key)))
	sequence := client.nextOpaque()
	binary.BigEndian.PutUint32(buf[12:16], sequence)

	return client.callAsync(sequence, buf, key)
}

func (client *Client) Del(key []byte) error {
	c, err := client.DelAsync(key)
	if err != nil {
		return err
	}

	v := <-c
	return v.Err
}

func (client *Client) IncrAsync(key []byte, delta, initial int64, expiration int) (<-chan *Item, error) {
	return client.incrAndDecrAsync(OpcodeIncr, key, delta, initial, expiration)
}

func (client *Client) Incr(key []byte, delta, initial int64, expiration int) (uint64, error) {
	c, err := client.IncrAsync(key, delta, initial, expiration)
	if err != nil {
		return 0, err
	}

	v := <-c
	return binary.BigEndian.Uint64(v.Value), nil
}

func (client *Client) DecrAsync(key []byte, delta, initial int64, expiration int) (<-chan *Item, error) {
	return client.incrAndDecrAsync(OpcodeDecr, key, delta, initial, expiration)
}

func (client *Client) Decr(key []byte, delta, initial int64, expiration int) (uint64, error) {
	c, err := client.DecrAsync(key, delta, initial, expiration)
	if err != nil {
		return 0, err
	}

	v := <-c
	return binary.BigEndian.Uint64(v.Value), nil
}

func (client *Client) incrAndDecrAsync(opcode byte, key []byte, delta, initial int64, expiration int) (<-chan *Item, error) {
	buf := make([]byte, 24)
	buf[0] = MagicRequest
	buf[1] = opcode
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(key)))
	buf[4] = byte(20)
	buf[5] = byte(0)
	binary.BigEndian.PutUint16(buf[6:8], 0)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(key)+20))
	sequence := client.nextOpaque()
	binary.BigEndian.PutUint32(buf[12:16], sequence)
	binary.BigEndian.PutUint64(buf[16:24], 0)
	extra := make([]byte, 20)
	binary.BigEndian.PutUint64(extra[0:8], uint64(delta))
	binary.BigEndian.PutUint64(extra[8:16], uint64(initial))
	binary.BigEndian.PutUint32(extra[16:20], uint32(expiration))

	return client.callAsync(sequence, buf, extra, key)
}

func (client *Client) setAsync(opcode byte, key, value []byte, cas uint64, flag []byte, expiration int) (<-chan *Item, error) {
	buf := make([]byte, 24)
	buf[0] = MagicRequest
	buf[1] = opcode
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(key)))
	buf[4] = byte(len(flag) + 4)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(key)+len(flag)+len(value)+4))
	sequence := client.nextOpaque()
	binary.BigEndian.PutUint32(buf[12:16], sequence)
	if cas != 0 {
		binary.BigEndian.PutUint64(buf[16:24], cas)
	}
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(expiration))

	return client.callAsync(sequence, buf, flag, b, key, value)
}

func (client *Client) callAsync(sequence uint32, buffers ...[]byte) (<-chan *Item, error) {
	b := client.writeBufferPool.Get().(*bytes.Buffer)
	defer func() {
		b.Reset()
		client.writeBufferPool.Put(b)
	}()
	for _, v := range buffers {
		b.Write(v)
	}

	result := make(chan *Item, 1)
	client.mu.Lock()
	client.asyncRequest[sequence] = result
	client.mu.Unlock()

	if n, err := client.conn.Write(b.Bytes()); err != nil || b.Len() != n {
		client.mu.Lock()
		delete(client.asyncRequest, sequence)
		client.mu.Unlock()
		return nil, err
	}

	return result, nil
}

func (client *Client) nextOpaque() uint32 {
	return atomic.AddUint32(&client.sequence, 1)
}

func (client *Client) readConn() {
	messageBuf := &bytes.Buffer{}
	buf := make([]byte, 1500)
	for {
		n, err := client.conn.Read(buf)
		if err == io.EOF {
			break
		}
		messageBuf.Write(buf[:n])
		if n == 1500 {
			continue
		}
		if messageBuf.Len() < 24 {
			messageBuf.Reset()
			continue
		}

		b := make([]byte, messageBuf.Len())
		copy(b, messageBuf.Bytes())
		client.dispatch(b)
		messageBuf.Reset()
	}
}

func (client *Client) dispatch(buf []byte) {
	if buf[0] != MagicResponse {
		return
	}
	if len(buf) < 24 {
		return
	}

	keySize := int(binary.BigEndian.Uint16(buf[2:4]))
	extraSize := int(buf[4])
	status := binary.BigEndian.Uint16(buf[6:8])
	bodySize := int(binary.BigEndian.Uint32(buf[8:12]))
	opaque := binary.BigEndian.Uint32(buf[12:16])
	cas := binary.BigEndian.Uint64(buf[16:24])
	extra := buf[24 : 24+extraSize]
	var key []byte
	if keySize > 0 {
		key = buf[24+extraSize : 24+extraSize+keySize]
	}
	var body []byte
	if bodySize > 0 {
		body = buf[24+keySize+extraSize : 24+keySize+extraSize+bodySize]
	}

	var err error
	switch status {
	case StatusKeyNotFound:
		err = ErrKeyNotFound
	case StatusKeyExists:
		err = ErrKeyAlreadyExists
	case StatusValueTooLarge:
		err = ErrValueTooLarge
	}

	client.mu.Lock()
	if c, ok := client.asyncRequest[opaque]; ok {
		c <- &Item{Key: key, Value: body, Extra: extra, CAS: cas, Err: err, Raw: buf}
		delete(client.asyncRequest, opaque)
	}
	client.mu.Unlock()
}
