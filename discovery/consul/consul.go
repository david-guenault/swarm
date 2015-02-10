package consul

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	consul "github.com/armon/consul-api"
	"github.com/docker/swarm/discovery"
)

type ConsulDiscoveryService struct {
	heartbeat time.Duration
	client    *consul.Client
	prefix    string
	lastIndex uint64
}

func init() {
	discovery.Register("consul", &ConsulDiscoveryService{})
}

func (s *ConsulDiscoveryService) Initialize(uris string, heartbeat int) error {
	parts := strings.SplitN(uris, "/", 2)
	if len(parts) < 2 {
		return fmt.Errorf("invalid format %q, missing <path>", uris)
	}

	path := parts[1]
	addr := ""

	listip := strings.Split(parts[0], ",")
	for idxip := range listip {
		log.WithField("name", "consul").Debug("Trying ", listip[idxip])
		resp, errget := http.Get(strings.Join([]string{"http://", listip[idxip]}, ""))
		if errget != nil {
			log.WithField("name", "consul").Debug("Get error ", errget)
			continue
		}
		defer resp.Body.Close()
		body, errbody := ioutil.ReadAll(resp.Body)
		if errbody != nil {
			log.WithField("name", "consul").Debug("Read body error ", errbody)
			continue
		}
		if string(body) != "Consul Agent" {
			log.WithField("name", "consul").Debug("Not a consul agent ")
			continue
		} else {
			addr = listip[idxip]
			log.WithField("name", "consul").Debug("Found available node ", addr)
			break
		}
	}

	s.heartbeat = time.Duration(heartbeat) * time.Second
	s.prefix = path + "/"

	if addr == "" {
		return fmt.Errorf("No available consul nodes at %q", uris)
	}

	config := consul.DefaultConfig()
	config.Address = addr

	client, err := consul.NewClient(config)
	if err != nil {
		return err
	}
	s.client = client

	kv := s.client.KV()
	p := &consul.KVPair{Key: s.prefix, Value: nil}
	if _, err = kv.Put(p, nil); err != nil {
		return err
	}
	_, meta, err := kv.Get(s.prefix, nil)
	if err != nil {
		return err
	}
	s.lastIndex = meta.LastIndex
	return nil
}
func (s *ConsulDiscoveryService) Fetch() ([]*discovery.Entry, error) {
	kv := s.client.KV()
	pairs, _, err := kv.List(s.prefix, nil)
	if err != nil {
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
				log.WithField("name", "consul").Errorf("Discovery error: %v", err)
				break
			}
			s.lastIndex = meta.LastIndex
			c <- s.lastIndex
		}
		close(c)
	}()
	return c
}
