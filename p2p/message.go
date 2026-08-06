package p2p

import "net"

//message
type RPC struct{
	from    net.Addr
	Payload []byte
} 