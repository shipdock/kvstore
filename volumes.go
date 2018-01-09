package kvstore

import (
	"encoding/json"
	"fmt"
	types "github.com/docker/docker/api/types"
	"os"
	"path"
	"path/filepath"
	"reflect"
)

type Volume struct {
	Name      string
	Driver    string
	Owner     string
	OwnerName string
	Labels    map[string]string
}

func NewVolume(base *types.Volume) *Volume {
	vol := &Volume{
		Name:   base.Name,
		Driver: base.Driver,
		Labels: make(map[string]string),
	}
	if len(base.Labels) > 0 {
		if value, ok := base.Labels["com.docker.swarm.owner"]; ok {
			vol.Owner = value
		}
		if value, ok := base.Labels["com.docker.swarm.owner.name"]; ok {
			vol.OwnerName = value
		}
		for k, v := range base.Labels {
			vol.Labels[k] = v
		}
	}
	return vol
}

type Volumes struct {
	proxy *Proxy
}

func NewVolumes(kvstore *KVStore) (*Volumes, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	rootPath := TrimRelative(filepath.Clean(path.Join(kvstore.RootPath, "volumes", hostname)))
	p, err := NewProxy(
		kvstore,
		rootPath,
		func(v []byte) (interface{}, error) {
			s := &Volume{}
			err := json.Unmarshal(v, s)
			if err != nil {
				return nil, err
			}
			return s, nil
		},
		func(local, remote interface{}) bool {
			vl := local.(*Volume)
			vr := remote.(*Volume)
			if len(vl.Owner) == 0 && len(vr.Owner) == 0 {
				return reflect.DeepEqual(local, remote)
			}
			if len(vl.Owner) == 0 && len(vr.Owner) != 0 {
				// avoid update
				return true
			}
			if len(vl.Owner) != 0 && len(vr.Owner) == 0 {
				// must update
				return false
			}
			return reflect.DeepEqual(local, remote)
		})
	if err != nil {
		return nil, err
	}
	v := &Volumes{
		proxy: p,
	}
	return v, nil
}

func (ss *Volumes) Put(Volume *types.Volume) error {
	v := NewVolume(Volume)
	return ss.proxy.Put(v.Name, v)
}

func (ss *Volumes) Delete(k string) error {
	return ss.proxy.Delete(k)
}

func (ss *Volumes) Get(k string) (*Volume, error) {
	v, err := ss.proxy.Get(k)
	if err != nil {
		return nil, err
	}
	cv, ok := v.(*Volume)
	if !ok {
		return nil, fmt.Errorf("type assertion failed")
	}
	return cv, nil
}

func (ss *Volumes) List(recursive bool) (map[string]*Volume, error) {
	im, err := ss.proxy.List(recursive)
	if err != nil {
		return nil, err
	}
	rs := make(map[string]*Volume)
	for k, v := range im {
		rs[k] = v.(*Volume)
	}
	return rs, nil
}

func (ss *Volumes) Sync(ls []*types.Volume) error {
	lsm := make(map[string]interface{})
	for _, s := range ls {
		lsm[s.Name] = NewVolume(s)
	}
	return ss.proxy.Sync(lsm)
}
