package router

type Config struct {
	Servers []ConfigServer `yaml:"servers"`
}

type ConfigServer struct {
	Name   string `yaml:"name"`
	Host   string `yaml:"host"`
	Port   int    `yaml:"port"`
	Status string `yaml:"status"`
	Phase  string `yaml:"phase"`
}

func (c *Config) ToRouter() []*Memcached {
	servers := make([]*Memcached, len(c.Servers))
	for i, v := range c.Servers {
		status := StatusNormal
		switch v.Status {
		case "add":
			status = StatusAdding
		case "delete":
			status = StatusDeleting
		}
		phase := PhaseReadWrite
		switch v.Phase {
		case "do":
			phase = PhaseDeleteOnly
		case "wo":
			phase = PhaseWriteOnly
		}
		servers[i] = &Memcached{
			Name:   v.Name,
			Host:   v.Host,
			Port:   v.Port,
			Status: status,
			Phase:  phase,
		}
	}
	return nil
}
