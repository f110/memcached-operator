package router

import (
	"encoding/binary"
	"io"
	"net"

	"github.com/f110/memcached-operator/client"
	"github.com/f110/memcached-operator/logger"
)

type Router struct {
	Addr    string
	Cluster *Cluster
}

func NewRouter(addr string, servers []*Memcached) *Router {
	return &Router{
		Addr:    addr,
		Cluster: NewCluster(servers),
	}
}

func (s *Router) ListenAndServe() error {
	l, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}
		go s.serve(conn)
	}
}

func (s *Router) serve(conn net.Conn) {
	buf := make([]byte, 1500)
	msg := make([]byte, 0)
	for {
		n, err := conn.Read(buf)
		if err == io.EOF {
			return
		}
		if err != nil {
			return
		}
		if n == 1500 {
			msg = append(msg, buf...)
			continue
		}

		s.parseRequest(conn, msg)
		msg = msg[:0]
	}
}

func (s *Router) parseRequest(conn net.Conn, req []byte) {
	if req[0] != client.MagicRequest {
		return
	}

	opcode := req[1]
	keyLength := int(binary.BigEndian.Uint16(req[2:4]))
	extraLength := int(req[4])
	totalBody := int(binary.BigEndian.Uint32(req[8:12]))
	opaque := binary.BigEndian.Uint32(req[12:16])
	cas := binary.BigEndian.Uint64(req[16:24])
	valueLength := totalBody - extraLength - keyLength
	req = req[24:]
	var extra []byte
	if extraLength > 0 {
		extra = make([]byte, extraLength)
		copy(extra, req[:extraLength])
		req = req[extraLength:]
	}
	var key []byte
	if keyLength > 0 {
		key = make([]byte, keyLength)
		copy(key, req[:keyLength])
		req = req[keyLength:]
	}
	var value []byte
	if valueLength > 0 {
		value := make([]byte, valueLength)
		copy(value, req[:valueLength])
		req = req[:valueLength]
	}

	s.handle(conn, opcode, key, value, cas, extra, opaque)
}

func (s *Router) handle(conn net.Conn, opcode byte, key, value []byte, cas uint64, extra []byte, opaque uint32) {
	switch opcode {
	case client.OpcodeGet:
		v, err := s.Cluster.Get(key)
		if err != nil {
			logger.Log.Info(err)
		}
		if v == nil {
			return
		}
		res := <-v
		_, err = conn.Write(res.Raw)
		if err != nil {
			logger.Log.Info(err)
		}
	case client.OpcodeSet:
		expiration := int(binary.BigEndian.Uint32(extra[4:8]))
		v, err := s.Cluster.Set(key, value, cas, extra[:4], expiration)
		if err != nil {
			logger.Log.Info(err)
		}
		if v == nil {
			return
		}
		res := <-v
		_, err = conn.Write(res.Raw)
		if err != nil {
			logger.Log.Info(err)
		}
	}
}
