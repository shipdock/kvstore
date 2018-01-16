package kvstore

import (
	"encoding/json"
	"fmt"
	"github.com/docker/docker/api/types/swarm"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

const INGRESS_NETWORK_PREFIX = "10.255."

type Service struct {
	ID             string
	Name           string
	Owner          string
	OwnerName      string
	VirtualIP      string
	ResolutionMode string
	Ports          []swarm.PortConfig
	Labels         map[string]string
}

type Services struct {
	proxy *Proxy
}

func NewServices(kvstore *KVStore) (*Services, error) {
	rootPath := TrimRelative(filepath.Clean(path.Join(kvstore.RootPath, "services")))
	p, err := NewProxy(
		kvstore,
		rootPath,
		func(v []byte) (interface{}, error) {
			s := &Service{}
			err := json.Unmarshal(v, s)
			if err != nil {
				return nil, err
			}
			return s, nil
		}, nil)
	if err != nil {
		return nil, err
	}
	service := &Services{
		proxy: p,
	}
	return service, nil
}

func buildPortConfigs(config string) []swarm.PortConfig {
	var results []swarm.PortConfig
	for _, entry := range strings.Split(config, ",") {
		pc := &swarm.PortConfig{}
		kvs := strings.Split(entry, "/")
		if len(kvs) < 2 {
			pc.Protocol = "tcp"
		} else {
			pc.Protocol = swarm.PortConfigProtocol(kvs[1])
		}
		port, err := strconv.ParseUint(kvs[0], 10, 32)
		if err != nil {
			continue
		}
		pc.TargetPort = uint32(port)
		pc.PublishedPort = uint32(port)
		pc.PublishMode = "macvlan"
		results = append(results, *pc)
	}
	return results
}

func (ss *Services) NewService(base *swarm.Service) *Service {
	s := &Service{
		ID:             base.ID,
		Name:           base.Spec.Name,
		Labels:         make(map[string]string),
		ResolutionMode: "dnsrr",
	}
	if len(base.Spec.Labels) > 0 {
		if value, ok := base.Spec.Labels[LABEL_OWNER]; ok {
			s.Owner = value
		}
		if value, ok := base.Spec.Labels[LABEL_OWNERNAME]; ok {
			s.OwnerName = value
		}
		if value, ok := base.Spec.Labels[LABEL_SERVICE_IP]; ok {
			s.VirtualIP = value
		}
		if value, ok := base.Spec.Labels[LABEL_SERVICE_PORTS]; ok {
			s.Ports = buildPortConfigs(value)
		}
		for k, v := range base.Spec.Labels {
			s.Labels[k] = v
		}
	}
	if len(s.VirtualIP) == 0 {
		for _, entry := range base.Endpoint.VirtualIPs {
			if strings.HasPrefix(entry.Addr, INGRESS_NETWORK_PREFIX) {
				continue
			}
			s.VirtualIP = strings.Split(entry.Addr, "/")[0]
			break
		}
	}
	if len(s.VirtualIP) > 0 {
		s.ResolutionMode = "vip"
	} else {
		s.ResolutionMode = "dnsrr"
	}
	for _, port := range base.Endpoint.Ports {
		s.Ports = append(s.Ports, port)
	}
	return s
}

func (ss *Services) Put(service *swarm.Service) error {
	v := ss.NewService(service)
	return ss.proxy.Put(v.Name, v)
}

func (ss *Services) Delete(k string) error {
	return ss.proxy.Delete(k)
}

func (ss *Services) Get(k string) (*Service, error) {
	v, err := ss.proxy.Get(k)
	if err != nil {
		return nil, err
	}
	cv, ok := v.(*Service)
	if !ok {
		return nil, fmt.Errorf("type assertion failed")
	}
	return cv, nil
}

func (ss *Services) List(recursive bool) (map[string]*Service, error) {
	im, err := ss.proxy.List(recursive)
	if err != nil {
		return nil, err
	}
	rs := make(map[string]*Service)
	for k, v := range im {
		rs[k] = v.(*Service)
	}
	return rs, nil
}

func (ss *Services) Sync(ls []swarm.Service) error {
	lsm := make(map[string]interface{})
	for _, s := range ls {
		lsm[s.Spec.Name] = ss.NewService(&s)
	}
	return ss.proxy.Sync(lsm)
}
