package kvstore

import (
	"encoding/json"
	"fmt"
	types "github.com/docker/docker/api/types"
	"os"
	"path"
	"path/filepath"
	"strings"
	"github.com/shipdock/libkv/store"
)

type NetInfo struct {
	Name       string
	Driver     string
	IPAddress  string
	Gateway    string
	MacAddress string
}

type MountInfo struct {
	Name       string
	Driver     string
}

type Container struct {
	ID          string
	Name        string
	ServiceName string
	TaskNum     string
	Owner       string
	OwnerName   string
	Networks    map[string]NetInfo
	Mounts      map[string]MountInfo
	Labels      map[string]string
}

func containerName(container *types.Container) string {
	result := TrimRelative(container.Names[0])
	result = strings.TrimSuffix(result, filepath.Ext(result))
	result = result + "-" + container.ID[0:8]
	return result
}

func NewContainer(base *types.Container, networks map[string]*Network) *Container {
	sn := ""
	tn := ""
	if len(base.Labels) > 0 {
		if taskname, ok := base.Labels["com.docker.swarm.task.name"]; ok {
			arr := strings.Split(taskname, ".")
			if len(arr) > 2 {
				sn = arr[0]
				tn = arr[1]
			}
		}
	}
	c := &Container{
		ID:          base.ID,
		Name:        containerName(base),
		ServiceName: sn,
		TaskNum:     tn,
		Networks:    make(map[string]NetInfo),
		Mounts:      make(map[string]MountInfo),
		Labels:      make(map[string]string),
	}
	for k, v := range base.NetworkSettings.Networks {
		if k == "ingress" {
			continue
		}
		net, ok := networks[v.NetworkID]
		if !ok {
			continue
		}
		n := &NetInfo{
			Name:       k,
			Driver:     net.Driver,
			Gateway:    v.Gateway,
			IPAddress:  v.IPAddress,
			MacAddress: v.MacAddress,
		}
		c.Networks[k] = *n
	}
	if len(base.Labels) > 0 {
		for k, v := range base.Labels {
			c.Labels[k] = v
		}
		if val, ok := base.Labels["com.docker.swarm.owner"]; ok {
			c.Owner = val
		}
		if val, ok := base.Labels["com.docker.swarm.owner.name"]; ok {
			c.OwnerName = val
		}
	}
	for _, m := range base.Mounts {
		c.Mounts[m.Name] = *&MountInfo{
			Name: m.Name,
			Driver: m.Driver,
		}
	}
	return c
}

type Containers struct {
	proxy *Proxy
	networks *Networks
	containersPath string
}

func NewContainers(kvstore *KVStore, networks *Networks) (*Containers, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	rootPath := TrimRelative(filepath.Clean(path.Join(kvstore.RootPath, "containers", hostname)))
	p, err := NewProxy(
		kvstore,
		rootPath,
		func(v []byte) (interface{}, error) {
			s := &Container{}
			err := json.Unmarshal(v, s)
			if err != nil {
				return nil, err
			}
			return s, nil
		}, nil)
	if err != nil {
		return nil, err
	}
	Container := &Containers{
		proxy: p,
		networks: networks,
		containersPath: path.Join(kvstore.RootPath, "containers"),
	}
	return Container, nil
}

func (ss *Containers) Put(container *types.Container) error {
	networks, err := ss.GetNetworkIDMap()
	if err != nil {
		return err
	}
	c := NewContainer(container, networks)
	return ss.proxy.Put(c.Name, c)
}

func (ss *Containers) Delete(k string) error {
	return ss.proxy.Delete(k)
}

func (ss *Containers) Get(k string) (*Container, error) {
	v, err := ss.proxy.Get(k)
	if err != nil {
		return nil, err
	}
	cv, ok := v.(*Container)
	if !ok {
		return nil, fmt.Errorf("type assertion failed")
	}
	return cv, nil
}

func (ss *Containers) List(recursive bool) (map[string]*Container, error) {
	im, err := ss.proxy.List(recursive)
	if err != nil {
		return nil, err
	}
	rs := make(map[string]*Container)
	for k, v := range im {
		rs[k] = v.(*Container)
	}
	return rs, nil
}

// List() returns this host's container list
// ListAll returns all containers in this cluster
func (ss *Containers) ListAll() (map[string]*Container, error) {
	kvs, err := ss.proxy.kvstore.List(ss.containersPath, true)
	if err != nil && err != store.ErrKeyNotFound {
		return nil, err
	}
	results := make(map[string]*Container)
	for _, kv := range kvs {
		c := &Container{}
		if err := json.Unmarshal(kv.Value, c); err != nil {
			continue
		}
		results[c.Name] = c
	}
	return results, nil
}

func (ss *Containers) Sync(ls []types.Container) error {
	lsm := make(map[string]interface{})
	networks, err := ss.GetNetworkIDMap()
	if err != nil {
		return err
	}
	for _, container := range ls {
		c := NewContainer(&container, networks)
		lsm[c.Name] = c
	}
	return ss.proxy.Sync(lsm)
}

func (ss *Containers) GetNetworkIDMap() (map[string]*Network, error) {
	base, err := ss.networks.List(true)
	if err != nil {
		return nil, err
	}
	results := make(map[string]*Network)
	for _, v := range base {
		results[v.ID] = v
	}
	return results, nil
}