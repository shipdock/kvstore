package kvstore

import "strings"

const LABEL_OWNER = "com.docker.swarm.owner"
const LABEL_OWNERNAME = "com.docker.swarm.owner.name"
const LABEL_SERVICE_IP = "com.navercorp.shipdock.lb.service_ip"
const LABEL_SERVICE_PORTS = "com.navercorp.shipdock.lb.service_ports"

func TrimRelative(str string) string {
	str = strings.TrimSpace(str)
	str = strings.TrimLeft(str, "/")
	str = strings.TrimRight(str, "/")
	str = strings.TrimSpace(str)
	return str
}
