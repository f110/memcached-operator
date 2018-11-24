package client

import (
	"testing"
)

func BenchmarkClient_GetAsync(b *testing.B) {
	b.ReportAllocs()

	c := NewTestClient()
	conn := c.conn.(*testConn)
	go conn.DropPacket()
	key := []byte("benchmark")
	for i := 0; i < b.N; i++ {
		if _, err := c.GetAsync(key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClient_Get(b *testing.B) {
	b.ReportAllocs()

	c := NewTestClient()
	go c.readConn()
	key := []byte("benchmark")
	for i := 0; i < b.N; i++ {
		if _, err := c.Get(key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClient_IncrAsync(b *testing.B) {
	b.ReportAllocs()

	c := NewTestClient()
	conn := c.conn.(*testConn)
	go conn.DropPacket()
	key := []byte("benchmark")
	for i := 0; i < b.N; i++ {
		if _, err := c.IncrAsync(key, 1, 0, 60); err != nil {
			b.Fatal(err)
		}
	}
}
