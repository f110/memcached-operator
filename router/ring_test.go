package router

import "testing"

func TestRing_Pick(t *testing.T) {
	cases := []struct {
		Table            []*node
		PickedServerName string
	}{
		{
			Table: []*node{
				{Hash: 1, Server: &Memcached{Name: "host1"}},
				{Hash: 2, Server: &Memcached{Name: "host2"}},
				{Hash: 3632233997, Server: &Memcached{Name: "host3"}},
			},
			PickedServerName: "host3",
		},
		{
			Table: []*node{
				{Hash: 1, Server: &Memcached{Name: "host1"}},
				{Hash: 2, Server: &Memcached{Name: "host2"}},
				{Hash: 3632233996, Server: &Memcached{Name: "host3"}},
			},
			PickedServerName: "host1",
		},
		{
			Table: []*node{
				{Hash: 1, Server: &Memcached{Name: "host1"}},
				{Hash: 3632233996, Server: &Memcached{Name: "host2"}},
				{Hash: 3632233998, Server: &Memcached{Name: "host3"}},
			},
			PickedServerName: "host3",
		},
	}

	r := &Ring{}
	for _, c := range cases {
		r.Table = c.Table
		s := r.Pick([]byte("test"))
		if len(s) != 1 {
			t.Fatal("expected 1 server but not")
		}
		if s[0].Name != c.PickedServerName {
			t.Errorf("expected host3 but not: %s", s[0].Name)
		}
	}
}
