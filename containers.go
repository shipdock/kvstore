package kvstore

import (
	"encoding/json"
	"fmt"
	types "github.com/docker/docker/api/types"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type NetInfo struct {
	Name       string
	IPAddress  string
	Gateway    string
	MacAddress string
}

type Container struct {
	ID          string
	Name        string
	ServiceName string
	TaskNum     string
	Networks    map[string]NetInfo
	Labels      map[string]string
}

func containerName(container *types.Container) string {
	result := TrimRelative(container.Names[0])
	result = strings.TrimSuffix(result, filepath.Ext(result))
	result = result + "-" + container.ID[0:8]
	return result
}

func NewContainer(base *types.Container) *Container {
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
		Labels:      make(map[string]string),
	}
	for k, v := range base.NetworkSettings.Networks {
		if k == "ingress" {
			continue
		}
		n := &NetInfo{
			Name:       k,
			Gateway:    v.Gateway,
			IPAddress:  v.IPAddress,
			MacAddress: v.MacAddress,
		}
		c.Networks[k] = *n
	}
	for k, v := range base.Labels {
		c.Labels[k] = v
	}
	return c
}

type Containers struct {
	proxy *Proxy
}

func NewContainers(kvstore *KVStore) (*Containers, error) {
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
	}
	return Container, nil
}

func (ss *Containers) Put(container *types.Container) error {
	c := NewContainer(container)
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

func (ss *Containers) Sync(ls []types.Container) error {
	lsm := make(map[string]interface{})
	for _, container := range ls {
		c := NewContainer(&container)
		lsm[c.Name] = c
	}
	return ss.proxy.Sync(lsm)
}
