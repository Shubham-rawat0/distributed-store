package p2p

import (
	"fmt"
	"net"
)

//remote node over tcp established conn
type TCPPeer struct{
	conn 			net.Conn
	// dial and retrieve a conn => outbound == true
	// accept and retrieve a conn => outbound == false
	outbound		bool
}

func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer{
	return &TCPPeer{
		conn,
		outbound,
	}
}

type TCPTransportOpts struct{
	ListenAddr	 	 string
	HandshakeFunc	 HandshakeFunc
	Decoder 		 Decoder
	OnPeer			 func(Peer) error
}

type TCPTransport struct{
	TCPTransportOpts 
	listener 	  	 	net.Listener
	rpcch			 	chan RPC
}	

func NewTCPTransport(opts TCPTransportOpts) *TCPTransport{
	return &TCPTransport{
		TCPTransportOpts: opts,
		rpcch:			  make(chan RPC),
	}
}

func (p *TCPPeer)Close() error{
	return p.conn.Close()
}

//return read only channel to read incoming msg received from another peer
func (t *TCPTransport) Consume() <-chan RPC{
	return t.rpcch
}

func (t *TCPTransport) ListenAndAccept() error{
	var err error

	t.listener,err=net.Listen("tcp",t.ListenAddr)
	if err!=nil{
		return err
	}

	go t.startAcceptLoop()

	return nil
}

func (t *TCPTransport) startAcceptLoop(){
	for {
		conn, err:= t.listener.Accept()
		if err!=nil{
			fmt.Printf("tcp accept error %s\n",err)
		}

		fmt.Printf("New incoming connection %v\n",conn )

		go t.handleConn(conn)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn) {
	var err error

	defer func(){
		fmt.Println("droppin peer connection",err)
		conn.Close()
	}()

	peer:=NewTCPPeer(conn,true)

	if err:=t.HandshakeFunc(peer);err!=nil{
		return
	}

	if t.OnPeer!=nil{
		if err=t.OnPeer(peer);err!=nil{
			return
		}
	}
	
	//read loop
	rpc:=RPC{}

	for{
		err:=t.Decoder.Decode(conn,&rpc)
		
		if err!=nil{
			fmt.Printf("TCP error: %s\n",err)
			return
		}

		rpc.from = conn.RemoteAddr()

		//sending rpc (msg) to channel
		t.rpcch <- rpc
	}

}