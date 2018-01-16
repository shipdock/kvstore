package kvstore

import (
	"encoding/json"
	"fmt"
	"github.com/docker/docker/api/types/swarm"
	"path"
	"path/filepath"
)

type Node struct {
	ID             string
	Labels         map[string]string
	Role           string
	Availability   string
	Hostname       string
	NanoCPUs       int64
	MemoryBytes    int64
	EngineVersion  string
	State          string
	Address        string
}

type Nodes struct {
	proxy *Proxy
}

func NewNodes(kvstore *KVStore) (*Nodes, error) {
	rootPath := TrimRelative(filepath.Clean(path.Join(kvstore.RootPath, "nodes")))
	p, err := NewProxy(
		kvstore,
		rootPath,
		func(v []byte) (interface{}, error) {
			s := &Node{}
			err := json.Unmarshal(v, s)
			if err != nil {
				return nil, err
			}
			return s, nil
		}, nil)
	if err != nil {
		return nil, err
	}
	service := &Nodes{
		proxy: p,
	}
	return service, nil
}

func (ss *Nodes) NewNode(base *swarm.Node) *Node {
	s := &Node{
		Hostname:       base.Description.Hostname,
		ID:             base.ID,
		Labels:         base.Spec.Labels,
		Role:           string(base.Spec.Role),
		Availability:   string(base.Spec.Availability),
		NanoCPUs:       base.Description.Resources.NanoCPUs,
		MemoryBytes:    base.Description.Resources.MemoryBytes,
		EngineVersion:  base.Description.Engine.EngineVersion,
		State:          string(base.Status.State),
		Address:        base.Status.Addr,
	}
	if s.Labels == nil {
		s.Labels = make(map[string]string)
	}
	return s
}

func (ss *Nodes) Put(service *swarm.Node) error {
	v := ss.NewNode(service)
	return ss.proxy.Put(v.Hostname, v)
}

func (ss *Nodes) Delete(k string) error {
	return ss.proxy.Delete(k)
}

func (ss *Nodes) Get(k string) (*Node, error) {
	v, err := ss.proxy.Get(k)
	if err != nil {
		return nil, err
	}
	cv, ok := v.(*Node)
	if !ok {
		return nil, fmt.Errorf("type assertion failed")
	}
	return cv, nil
}

func (ss *Nodes) List(recursive bool) (map[string]*Node, error) {
	im, err := ss.proxy.List(recursive)
	if err != nil {
		return nil, err
	}
	rs := make(map[string]*Node)
	for k, v := range im {
		rs[k] = v.(*Node)
	}
	return rs, nil
}

func (ss *Nodes) Sync(ls []swarm.Node) error {
	lsm := make(map[string]interface{})
	for _, s := range ls {
		lsm[s.Description.Hostname] = ss.NewNode(&s)
	}
	return ss.proxy.Sync(lsm)
}
