package kvstore

import (
	"encoding/json"
	"fmt"
	"github.com/shipdock/libkv"
	"github.com/shipdock/libkv/store"
	"github.com/shipdock/libkv/store/consul"
	"github.com/shipdock/libkv/store/etcd"
	log "github.com/sirupsen/logrus"
	"net/url"
	"strings"
	"time"
	"path"
)

type KVStore struct {
	Store      store.Store
	Services   *Services
	Networks   *Networks
	Volumes    *Volumes
	Containers *Containers
	Nodes      *Nodes
	RootPath   string
}

func NewKVStore(storeUrl, connectionTimeout, username, password string) (*KVStore, error) {
	uri, err := url.Parse(storeUrl)
	if err != nil {
		return nil, err
	}
	var backend store.Backend
	switch scheme := strings.ToLower(uri.Scheme); scheme {
	case "consul":
		backend = store.CONSUL
	case "etcd":
		backend = store.ETCD
	//case "zookeeper":
	//	backend = store.ZK
	//case "boltdb":
	//	backend = store.BOLTDB
	default:
		return nil, fmt.Errorf("unsupported uri schema: %+v (url:%s)", uri, storeUrl)
	}
	config := &store.Config{}
	if len(connectionTimeout) > 0 {
		timeout, err := time.ParseDuration(connectionTimeout)
		if err != nil {
			return nil, err
		}
		config.ConnectionTimeout = timeout
	} else {
		config.ConnectionTimeout = (time.Second * 3)
	}
	if len(username) > 0 && len(password) > 0 {
		config.Username = username
		config.Password = password
	}
	store, err := libkv.NewStore(
		backend,
		[]string{uri.Host},
		config,
	)
	if err != nil {
		return nil, err
	}
	kvstore := &KVStore{
		Store:    store,
		RootPath: uri.Path,
	}
	if services, err := NewServices(kvstore); err != nil {
		return nil, err
	} else {
		kvstore.Services = services
	}
	if networks, err := NewNetworks(kvstore); err != nil {
		return nil, err
	} else {
		kvstore.Networks = networks
	}
	if volumes, err := NewVolumes(kvstore); err != nil {
		return nil, err
	} else {
		kvstore.Volumes = volumes
	}
	if containers, err := NewContainers(kvstore); err != nil {
		return nil, err
	} else {
		kvstore.Containers = containers
	}
	if nodes, err := NewNodes(kvstore); err != nil {
		return nil, err
	} else {
		kvstore.Nodes = nodes
	}
	return kvstore, nil
}

func (k *KVStore) Close() {
	k.Store.Close()
}

func (k *KVStore) Put(key string, val interface{}) error {
	log.Debugf("PUT:%s", key)
	bv, err := json.Marshal(val)
	if err != nil {
		return err
	}
	if err := k.Store.Put(key, bv, &store.WriteOptions{IsDir: false}); err != nil {
		return err
	}
	return nil
}

func (k *KVStore) deleteEmptyParents(target string) error {
	if target == "." || target == "/" {
		return nil
	}
	kvs, err := k.Store.List(target, true)
	if err != nil {
		return err
	}
	if len(kvs) > 0 {
		return nil
	}
	if err := k.Store.DeleteTree(target); err != nil {
		return err
	}
	return k.deleteEmptyParents(path.Dir(target))
}

func (k *KVStore) Remove(key string) error {
	log.Debugf("DEL:%s", key)
	if err := k.Store.DeleteTree(key); err != nil {
		if err != store.ErrKeyNotFound {
			return err
		}
	}
	k.deleteEmptyParents(path.Dir(key))
	return nil
}

func init() {
	consul.Register()
	etcd.Register()
	//zookeeper.Register()
	//boltdb.Register()
}
