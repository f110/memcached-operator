package router

import (
	"github.com/f110/memcached-operator/client"
)

const (
	StatusNormal Status = iota
	StatusAdding
	StatusDeleting
)

type Status int

const (
	PhaseDeleteOnly Phase = iota
	PhaseWriteOnly
	PhaseReadWrite
)

type Phase int

const (
	ModeDeleteOnly Mode = iota
	ModeWriteOnly
	ModeReadWrite
)

type Mode int

type Memcached struct {
	Name   string
	Host   string
	Port   int
	Status Status
	Phase  Phase
	Mode   Mode

	Client *client.Client

	hash uint32
	next *Memcached
}

func NewMemcached(name, host string, port int, status Status, phase Phase) *Memcached {
	c, err := client.NewClient(host, port)
	if err != nil {
		return nil
	}

	return &Memcached{
		Name:   name,
		Host:   host,
		Port:   port,
		Status: status,
		Phase:  phase,
		Client: c,
	}
}

func (m *Memcached) Get(key []byte) (<-chan *client.Item, error) {
	if m.Mode != ModeReadWrite {
		if m.next.Mode == ModeReadWrite {
			return m.next.Client.GetAsync(key)
		} else {
			panic("unreachable")
		}
	}

	return m.Client.GetAsync(key)
}

func (m *Memcached) Set(key, value []byte, cas uint64, flag []byte, expiration int) (<-chan *client.Item, error) {
	primary, secondary := m.PrimarySecondary()

	result, err := primary.Client.SetAsync(key, value, cas, flag, expiration)
	if err != nil {
		return nil, err
	}

	if secondary != nil && secondary.Mode == ModeWriteOnly {
		v := <-result
		if v.Err != nil {
			return nil, err
		}
		c := make(chan *client.Item, 1)
		c <- v
		_, err = secondary.Client.SetAsync(key, v.Value, 0, nil, expiration)
		return c, err
	}

	return result, nil
}

func (m *Memcached) Add(key, value, flag []byte, expiration int) (<-chan *client.Item, error) {
	primary, secondary := m.PrimarySecondary()

	result, err := primary.Client.AddAsync(key, value, flag, expiration)
	if err != nil {
		return nil, err
	}

	if secondary != nil && secondary.Mode == ModeWriteOnly {
		v := <-result
		if v.Err != nil {
			return nil, err
		}
		c := make(chan *client.Item, 1)
		c <- v
		_, err = secondary.Client.SetAsync(key, v.Value, 0, nil, expiration)
		return c, err
	}

	return result, err
}

func (m *Memcached) Replace(key, value []byte, cas uint64, flag []byte, expiration int) (<-chan *client.Item, error) {
	primary, secondary := m.PrimarySecondary()

	result, err := primary.Client.ReplaceAsync(key, value, cas, flag, expiration)
	if err != nil {
		return nil, err
	}

	if secondary != nil && secondary.Mode == ModeWriteOnly {
		v := <-result
		if v.Err != nil {
			return nil, err
		}
		c := make(chan *client.Item, 1)
		c <- v
		_, err = secondary.Client.SetAsync(key, v.Value, 0, nil, expiration)
		return c, err
	}

	return result, err
}

func (m *Memcached) Del(key []byte) (<-chan *client.Item, error) {
	primary, secondary := m.PrimarySecondary()

	res, err := primary.Client.DelAsync(key)
	if err != nil {
		return nil, err
	}

	if secondary != nil {
		_, err = secondary.Client.DelAsync(key)
		if err != nil {
			return res, err
		}
	}

	return res, err
}

func (m *Memcached) Incr(key []byte, delta, initial int64, expiration int) (<-chan *client.Item, error) {
	primary, secondary := m.PrimarySecondary()

	res, err := primary.Client.IncrAsync(key, delta, initial, expiration)
	if err != nil {
		return nil, err
	}

	if secondary != nil && secondary.Mode == ModeWriteOnly {
		v := <-res
		if v.Err != nil {
			return nil, v.Err
		}
		c := make(chan *client.Item, 1)
		c <- v
		_, err = secondary.Client.SetAsync(key, v.Value, 0, nil, expiration)
		return c, err
	}

	return res, nil
}

func (m *Memcached) Decr(key []byte, delta, initial int64, expiration int) (<-chan *client.Item, error) {
	primary, secondary := m.PrimarySecondary()

	res, err := primary.Client.DecrAsync(key, delta, initial, expiration)
	if err != nil {
		return nil, err
	}

	if secondary != nil && secondary.Mode == ModeWriteOnly {
		v := <-res
		if v.Err != nil {
			return nil, v.Err
		}
		c := make(chan *client.Item, 1)
		c <- v
		_, err = secondary.Client.SetAsync(key, v.Value, 0, nil, expiration)
		return c, err
	}

	return res, nil
}

func (m *Memcached) Dup() *Memcached {
	c := &Memcached{}
	*c = *m
	return c
}

func (m *Memcached) PrimarySecondary() (primary, secondary *Memcached) {
	if m.Mode == ModeReadWrite {
		primary = m
		secondary = m.next
	} else {
		primary = m.next
		secondary = m
	}

	return
}
