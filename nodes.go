package kvstore

import (
	"encoding/json"
	"fmt"
	"github.com/docker/docker/api/types/swarm"
	"path"
	"path/filepath"
)

type Nodes struct {
	proxy *Proxy
}

type Node struct {
	Hostname     string
	Role         string
	Availability string
	CPU          int64
	MEM          int64
	State        string
	Address      string
	Labels       map[string]string
}

func (ss *Nodes) NewNode(node *swarm.Node) *Node {
	s := &Node{
		Hostname:     node.Description.Hostname,
		Role:         string(node.Spec.Role),
		Availability: string(node.Spec.Availability),
		CPU:          node.Description.Resources.NanoCPUs,
		MEM:          node.Description.Resources.MemoryBytes,
		State:        string(node.Status.State),
		Address:      string(node.Status.Addr),
		Labels:       node.Spec.Labels,
	}
	if s.Labels == nil {
		s.Labels = make(map[string]string)
	}
	return s
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
	node := &Nodes{
		proxy: p,
	}
	return node, nil
}

func (ss *Nodes) Put(node *swarm.Node) error {
	v := ss.NewNode(node)
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
