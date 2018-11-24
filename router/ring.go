package router

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
)

var (
	ErrConflictName = errors.New("router: conflict name")
)

type node struct {
	Hash   uint32
	Server *Memcached
}

type Ring struct {
	Servers []*Memcached
	Table   []*node
}

func NewRing(servers []*Memcached) (*Ring, error) {
	c := make(map[string]struct{})
	t := make([]*node, 0, len(servers)*100)
	for _, v := range servers {
		if _, ok := c[v.Name]; ok {
			return nil, ErrConflictName
		} else {
			c[v.Name] = struct{}{}
		}

		for i := 0; i < 100; i++ {
			h := fmt.Sprintf("%s-%d", v.Name, i)
			hash := crc32.ChecksumIEEE([]byte(h))
			t = append(t, &node{Hash: hash, Server: v})
		}
	}

	sort.Slice(t, func(i, j int) bool {
		return t[i].Hash < t[j].Hash
	})

	for i, v := range t {
		switch v.Server.Status {
		case StatusAdding:
			switch v.Server.Phase {
			case PhaseDeleteOnly:
				v.Server.Mode = ModeDeleteOnly
				v.Server.next = t[i+1].Server
			case PhaseWriteOnly:
				v.Server.Mode = ModeWriteOnly
				v.Server.next = t[i+1].Server
			case PhaseReadWrite:
				v.Server.Mode = ModeReadWrite
				v.Server.next = t[i+1].Server.Dup()
				v.Server.next.Mode = ModeWriteOnly
			}
		case StatusDeleting:
			switch v.Server.Phase {
			case PhaseDeleteOnly:
				v.Server.Mode = ModeReadWrite
				v.Server.next = t[i+1].Server.Dup()
				v.Server.next.Mode = ModeDeleteOnly
			case PhaseWriteOnly:
				v.Server.Mode = ModeReadWrite
				v.Server.next = t[i+1].Server.Dup()
				v.Server.next.Mode = ModeWriteOnly
			case PhaseReadWrite:
				v.Server.Mode = ModeWriteOnly
				v.Server.next = t[i+1].Server.Dup()
				v.Server.next.Mode = ModeReadWrite
			}
		default:
			v.Server.Mode = ModeReadWrite
		}
	}

	return &Ring{Servers: servers, Table: t}, nil
}

func (r *Ring) Pick(key []byte) *Memcached {
	h := crc32.ChecksumIEEE(key)
	start := 0
	end := len(r.Table) - 1
	for end-start > 0 {
		i := start + (end-start)/2
		if h < r.Table[i].Hash {
			end = i
		} else {
			start = i + 1
		}
	}
	if start == len(r.Table)-1 && r.Table[start].Hash <= h {
		start = 0
	}

	return r.Table[start].Server
}
