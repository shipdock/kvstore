package kvstore

import (
	"encoding/json"
	"fmt"
	types "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/network"
	"path"
	"path/filepath"
)

type Network struct {
	ID        string
	Name      string
	Owner     string
	OwnerName string
	Driver    string
	Config    []network.IPAMConfig
	Labels    map[string]string
}

func NewNetwork(base *types.NetworkResource) *Network {
	n := &Network{
		ID:     base.ID,
		Name:   base.Name,
		Driver: base.Driver,
		Config: base.IPAM.Config,
		Labels: make(map[string]string),
	}
	if len(base.Labels) > 0 {
		if value, ok := base.Labels["com.docker.swarm.owner"]; ok {
			n.Owner = value
		}
		if value, ok := base.Labels["com.docker.swarm.owner.name"]; ok {
			n.OwnerName = value
		}
		for k, v := range base.Labels {
			n.Labels[k] = v
		}
	}
	return n
}

type Networks struct {
	proxy *Proxy
}

func NewNetworks(kvstore *KVStore) (*Networks, error) {
	rootPath := TrimRelative(filepath.Clean(path.Join(kvstore.RootPath, "networks")))
	p, err := NewProxy(
		kvstore,
		rootPath,
		func(v []byte) (interface{}, error) {
			s := &Network{}
			err := json.Unmarshal(v, s)
			if err != nil {
				return nil, err
			}
			return s, nil
		}, nil)
	if err != nil {
		return nil, err
	}
	n := &Networks{
		proxy: p,
	}
	return n, nil
}

func (ss *Networks) Put(Network *types.NetworkResource) error {
	v := NewNetwork(Network)
	return ss.proxy.Put(v.ID, v)
}

func (ss *Networks) Delete(k string) error {
	return ss.proxy.Delete(k)
}

func (ss *Networks) Get(k string) (*Network, error) {
	v, err := ss.proxy.Get(k)
	if err != nil {
		return nil, err
	}
	cv, ok := v.(*Network)
	if !ok {
		return nil, fmt.Errorf("type assertion failed")
	}
	return cv, nil
}

func (ss *Networks) List(recursive bool) (map[string]*Network, error) {
	im, err := ss.proxy.List(recursive)
	if err != nil {
		return nil, err
	}
	rs := make(map[string]*Network)
	for k, v := range im {
		rs[k] = v.(*Network)
	}
	return rs, nil
}

func (ss *Networks) Sync(ls []types.NetworkResource) error {
	lsm := make(map[string]interface{})
	for _, s := range ls {
		lsm[s.ID] = NewNetwork(&s)
	}
	return ss.proxy.Sync(lsm)
}
