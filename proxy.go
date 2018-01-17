package kvstore

import (
	"encoding/json"
	"github.com/shipdock/libkv/store"
	log "github.com/sirupsen/logrus"
	"path"
	"reflect"
)

type Unmarshaller func(v []byte) (interface{}, error)
type Comparator func(a, b interface{}) bool

type Proxy struct {
	kvstore   store.Store
	rootPath  string
	unmarshal Unmarshaller
	compare   Comparator
}

func NewProxy(kvstore *KVStore, rootPath string, unmarshaller Unmarshaller, comparator Comparator) (*Proxy, error) {
	c := &Proxy{
		kvstore:   kvstore.Store,
		rootPath:  rootPath,
		unmarshal: unmarshaller,
		compare:   comparator,
	}
	return c, nil
}

func (c *Proxy) Put(key string, value interface{}) error {
	bv, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	target := path.Join(c.rootPath, key)
	log.Debugf("PUT:%s", target)
	if err := c.kvstore.Put(target, bv, &store.WriteOptions{IsDir: false}); err != nil {
		return err
	}
	return nil
}

func (c *Proxy) Delete(key string) error {
	target := path.Join(c.rootPath, key)
	log.Debugf("DELETE:%s", target)
	return c.kvstore.Delete(target)
}

func (c *Proxy) Get(key string) (interface{}, error) {
	kv, err := c.kvstore.Get(path.Join(c.rootPath, key))
	if err != nil {
		return nil, err
	}
	rv, err := c.unmarshal(kv.Value)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func (c *Proxy) List(recursive bool) (map[string]interface{}, error) {
	kvs, err := c.kvstore.List(path.Join(c.rootPath), recursive)
	if err != nil {
		if err == store.ErrKeyNotFound {
			return make(map[string]interface{}), nil
		}
		return nil, err
	}
	rl := make(map[string]interface{})
	for _, kv := range kvs {
		if len(kv.Value) == 0 {
			continue
		}
		v, err := c.unmarshal(kv.Value)
		// do not return unmarshal error
		// interface's struct can be changed and sometimes it can be fail
		// just ignore unmarshal error (treat not exist) to overwrite interface struct
		if err != nil {
			continue
		}
		rl[path.Base(kv.Key)] = v
	}
	return rl, nil
}

func (c *Proxy) Sync(lvm map[string]interface{}) error {
	// build local/remote values
	rvm, err := c.List(true)
	if err != nil && err != store.ErrKeyNotFound {
		return err
	}
	// compare & update
	for lk, lv := range lvm {
		rv, ok := rvm[lk]
		if ok {
			// local exist, remote exist (compare & put)
			if c.compare != nil {
				if !c.compare(lv, rv) {
					c.Put(lk, lv)
				}

			} else if !reflect.DeepEqual(lv, rv) {
				c.Put(lk, lv)
			}
		} else {
			// local exist, remote not-exist (put)
			if err := c.Put(lk, lv); err != nil {
				return err
			}
		}
	}
	for rk, _ := range rvm {
		_, ok := lvm[rk]
		if !ok {
			// local not-exist, remote exist (delete)
			if err := c.Delete(rk); err != nil {
				return err
			}
		}
	}
	return nil
}
