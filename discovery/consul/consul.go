package consul

import (
	"fmt"
	"math/rand"
	"path"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/docker/swarm/discovery"
	consul "github.com/hashicorp/consul/api"
)

type ConsulDiscoveryService struct {
	heartbeat time.Duration
	client    *consul.Client
	prefix    string
	lastIndex uint64
	machines  []string
	machine   string
	uris      []string
}

func init() {
	discovery.Register("consul", &ConsulDiscoveryService{})
}

func (s *ConsulDiscoveryService) Initialize(uris string, heartbeat int) error {
	parts := strings.SplitN(uris, "/", 2)
	if len(parts) < 2 {
		return fmt.Errorf("invalid format %q, missing <path>", uris)
	}

	s.heartbeat = time.Duration(heartbeat) * time.Second
	s.prefix = parts[1] + "/"

	s.uris = strings.Split(parts[0], ",")

	err := s.GetMembers()
	if err != nil {
		return err
	}

	err = s.InitializeClient()
	return err
}

func (s *ConsulDiscoveryService) InitializeClient() error {
	// pick a random node
	if len(s.machines) < 1 {
		fmt.Errorf("No consul members found")
	}
	s.machine = s.machines[rand.Intn(len(s.machines))]

	// create client
	config := consul.DefaultConfig()
	config.Address = s.machine
	client, errclient := consul.NewClient(config)
	if errclient != nil {
		return errclient
	}
	s.client = client

	// initialize path
	kv := s.client.KV()
	p := &consul.KVPair{Key: s.prefix, Value: nil}
	_, errput := kv.Put(p, nil)
	if errput != nil {
		return errput
	}

	_, meta, errget := kv.Get(s.prefix, nil)
	if errget != nil {
		return errget
	}

	s.lastIndex = meta.LastIndex
	return nil
}

func (s *ConsulDiscoveryService) GetMembers() error {
	config := consul.DefaultConfig()
	for idxip := range s.uris {
		config.Address = s.uris[idxip]
		client, errclient := consul.NewClient(config)
		if errclient != nil {
			continue
		}

		catalog := client.Catalog()
		queryopts := &consul.QueryOptions{}
		nodes, _, errnodes := catalog.Nodes(queryopts)
		if errnodes != nil {
			continue
		}

		for idxnode := range nodes {
			member := nodes[idxnode].Address + ":8500"
			s.machines = append(s.machines, member)
			log.WithField("name", "consul").Debug("Add member to pool ", member)
		}
		break
	}

	if len(s.machines) < 1 {
		return fmt.Errorf("Failed to get members")
	}

	return nil
}

func (s *ConsulDiscoveryService) Fetch() ([]*discovery.Entry, error) {
	kv := s.client.KV()
	pairs, _, err := kv.List(s.prefix, nil)
	if err != nil {
		errclient := s.InitializeClient()
		if errclient != nil {
			return nil, errclient
		}
		return nil, err
	}

	addrs := []string{}
	for _, pair := range pairs {
		if pair.Key == s.prefix {
			continue
		}
		addrs = append(addrs, string(pair.Value))
	}

	return discovery.CreateEntries(addrs)
}

func (s *ConsulDiscoveryService) Watch(callback discovery.WatchCallback) {
	for _ = range s.waitForChange() {
		log.WithField("name", "consul").Debug("Discovery watch triggered")
		entries, err := s.Fetch()
		if err == nil {
			callback(entries)
		}
	}
}

func (s *ConsulDiscoveryService) Register(addr string) error {
	kv := s.client.KV()
	p := &consul.KVPair{Key: path.Join(s.prefix, addr), Value: []byte(addr)}
	_, err := kv.Put(p, nil)
	if err != nil {
		errclient := s.InitializeClient()
		if errclient != nil {
			return errclient
		}
		return err
	}
	return err
}

func (s *ConsulDiscoveryService) waitForChange() <-chan uint64 {
	c := make(chan uint64)
	go func() {
		for {
			kv := s.client.KV()
			option := &consul.QueryOptions{
				WaitIndex: s.lastIndex,
				WaitTime:  s.heartbeat}
			_, meta, err := kv.List(s.prefix, option)
			if err != nil {
				_ = s.InitializeClient()
				break
			}
			s.lastIndex = meta.LastIndex
			c <- s.lastIndex
		}
		close(c)
	}()
	return c
}
