package router

import "github.com/f110/memcached-operator/client"

type Cluster struct {
	Ring *Ring
}

func NewCluster(servers []*Memcached) *Cluster {
	ring, err := NewRing(servers)
	if err != nil {
		return nil
	}

	return &Cluster{Ring: ring}
}

func (c *Cluster) Get(key []byte) (<-chan *client.Item, error) {
	return c.Ring.Pick(key).Get(key)
}

func (c *Cluster) Set(key, value []byte, cas uint64, flag []byte, expiration int) (<-chan *client.Item, error) {
	return c.Ring.Pick(key).Set(key, value, cas, flag, expiration)
}

func (c *Cluster) Add(key, value, flag []byte, expiration int) (<-chan *client.Item, error) {
	return c.Ring.Pick(key).Add(key, value, flag, expiration)
}

func (c *Cluster) Replace(key, value []byte, cas uint64, flag []byte, expiration int) (<-chan *client.Item, error) {
	return c.Ring.Pick(key).Replace(key, value, cas, flag, expiration)
}

func (c *Cluster) Del(key []byte) (<-chan *client.Item, error) {
	return c.Ring.Pick(key).Del(key)
}

func (c *Cluster) Incr(key []byte, delta, initial int64, expiration int) (<-chan *client.Item, error) {
	return c.Ring.Pick(key).Incr(key, delta, initial, expiration)
}

func (c *Cluster) Decr(key []byte, delta, initial int64, expiration int) (<-chan *client.Item, error) {
	return c.Ring.Pick(key).Decr(key, delta, initial, expiration)
}
