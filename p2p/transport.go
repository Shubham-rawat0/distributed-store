package p2p

import "net"

//peer is remote node
type Peer interface{
	Send([]byte)	error
	net.Conn
	CloseStream()
}

//handle communication between nodes
type Transport interface{
	Addr()				string
	Dial(addr string)   error
	ListenAndAccept() 	error
	Consume() 		    <-chan RPC
	Close() 			error
}