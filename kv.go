package kvstore

import (
	"bytes"
	"fmt"
	"github.com/shipdock/libkv/store"
	log "github.com/sirupsen/logrus"
	"path"
	"path/filepath"
)

type KVs struct {
	kvstore  *KVStore
	rootPath string
}

func NewKVs(kvstore *KVStore, parentPath string) (*KVs, error) {
	rootPath := TrimRelative(filepath.Clean(path.Join(kvstore.RootPath, parentPath)))
	kvs := &KVs{
		kvstore:  kvstore,
		rootPath: rootPath,
	}
	return kvs, nil
}

func (kvs *KVs) Put(owner, k, v string) error {
	target := TrimRelative(path.Join(kvs.rootPath, owner, k))
	log.Debugf("PUT:%s %s", target, v)
	if err := kvs.kvstore.Store.Put(target, bytes.NewBufferString(v).Bytes(), &store.WriteOptions{IsDir: false}); err != nil {
		return err
	}
	return nil
}

func (kvs *KVs) Delete(owner, k string) error {
	target := TrimRelative(path.Join(kvs.rootPath, owner, k))
	log.Debugf("DELETE:%s", target)
	return kvs.kvstore.Store.Delete(target)
}

func (kvs *KVs) Get(owner, k string) (*string, error) {
	kv, err := kvs.kvstore.Store.Get(TrimRelative(path.Join(kvs.rootPath, owner, k)))
	if err != nil {
		return nil, err
	}
	rs := string(kv.Value)
	return &rs, nil
}

func (kvs *KVs) List(owner string, recursive bool) (map[string]string, error) {
	basePath := path.Join(kvs.rootPath, owner)
	list, err := kvs.kvstore.Store.List(basePath, recursive)
	if err != nil {
		if err == store.ErrKeyNotFound {
			return make(map[string]string), nil
		}
		return nil, err
	}
	rl := make(map[string]string)
	for _, kv := range list {
		if len(kv.Value) == 0 {
			continue
		}
		r := string(kv.Value)
		rk, err := filepath.Rel(basePath, kv.Key)
		if err != nil {
			fmt.Errorf("ERR:%s", err)
			continue
		}
		rl[rk] = r
	}
	return rl, nil
}

func (kvs *KVs) Sync(owner string, ms map[string]string) error {
	// build local/remote values
	rvm, err := kvs.List(owner, true)
	if err != nil && err != store.ErrKeyNotFound {
		return err
	}
	// compare & update
	for lk, lv := range ms {
		rv, ok := rvm[lk]
		if ok {
			// local exist, remote exist (compare & put)
			if lv != rv {
				if err := kvs.Put(owner, lk, lv); err != nil {
					return err
				}
			}
		} else {
			// local exist, remote not-exist (put)
			if err := kvs.Put(owner, lk, lv); err != nil {
				return err
			}
		}
	}
	for rk, _ := range rvm {
		_, ok := ms[rk]
		if !ok {
			// local not-exist, remote exist (delete)
			if err := kvs.Delete(owner, rk); err != nil {
				return err
			}
		}
	}
	return nil
}
