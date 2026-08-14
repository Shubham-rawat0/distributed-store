package p2p

import (
	"errors"
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

func (p *TCPPeer) Send(b []byte) error{
	_ ,err:=p.conn.Write(b)
	return err
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

func (p *TCPPeer) RemoteAddr() net.Addr{
	return p.conn.RemoteAddr()
}

func (p *TCPPeer)Close() error{
	return p.conn.Close()
}

//return read only channel to read incoming msg received from another peer
func (t *TCPTransport) Consume() <-chan RPC{
	return t.rpcch
}

//implements Transport interface
func (t *TCPTransport) Close() error{
	return t.listener.Close()
}

//Dial implements Transport interface, Dial connects to the address on the named network.
func (t *TCPTransport)Dial(addr string) error{
	conn , err:=net.Dial("tcp",addr)
	if err!=nil{
		return err
	}

	go t.handleConn(conn, true)
	return nil
}

func (t *TCPTransport) ListenAndAccept() error{
	var err error

	t.listener,err=net.Listen("tcp",t.ListenAddr)
	if err!=nil{
		return err
	}

	go t.startAcceptLoop()

	fmt.Println("TCP transport listening in port",t.ListenAddr)

	return nil
}

func (t *TCPTransport) startAcceptLoop(){
	for {
		conn, err:= t.listener.Accept()
		
		if errors.Is(err,net.ErrClosed){
			return
		}

		if err!=nil{
			fmt.Printf("tcp accept error %s\n",err)
		}

		fmt.Printf("New incoming connection %v\n",conn )

		go t.handleConn(conn,false)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn, outbound bool) {
	var err error

	defer func(){
		fmt.Println("dropping peer connection",err)
		conn.Close()
	}()

	peer:=NewTCPPeer(conn,outbound)

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