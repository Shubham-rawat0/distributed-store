package p2p

import "net"

//peer is remote node
type Peer interface{
	Send([]byte)	error
	Close()		 	error
	// RemoteAddr returns the remote network address, if known.
	RemoteAddr() 	net.Addr
}

//handle communication between nodes
type Transport interface{
	Dial(addr string)   error
	ListenAndAccept() 	error
	Consume() 		    <-chan RPC
	Close() 			error
}