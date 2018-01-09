package kvstore

import (
	"fmt"
	"github.com/shipdock/libkv"
	"github.com/shipdock/libkv/store"
	"github.com/shipdock/libkv/store/consul"
	"github.com/shipdock/libkv/store/etcd"
	"net/url"
	"strings"
	"time"
)

type KVStore struct {
	Store      store.Store
	Services   *Services
	Networks   *Networks
	Volumes    *Volumes
	Containers *Containers
	RootPath   string
}

func NewKVStore(storeUrl, connectionTimeout string) (*KVStore, error) {
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
	timeout, err := time.ParseDuration(connectionTimeout)
	if err != nil {
		return nil, err
	}
	store, err := libkv.NewStore(
		backend,
		[]string{uri.Host},
		&store.Config{
			ConnectionTimeout: timeout,
		},
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
	return kvstore, nil
}

func (k *KVStore) Close() {
	k.Store.Close()
}

func init() {
	consul.Register()
	etcd.Register()
	//zookeeper.Register()
	//boltdb.Register()
}
