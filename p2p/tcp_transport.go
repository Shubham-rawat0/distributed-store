package p2p

import (
	"errors"
	"fmt"
	"net"
	"sync"
)

//remote node over tcp established conn
type TCPPeer struct{
	//underlying connection of peer
	net.Conn
	// dial and retrieve a conn => outbound == true
	// accept and retrieve a conn => outbound == false
	outbound		bool
	wg				*sync.WaitGroup
}

func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer{
	return &TCPPeer{
		Conn: 		conn,
		outbound:	outbound,
		wg:			&sync.WaitGroup{},
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
		rpcch:			  make(chan RPC, 1024),
	}
}

func (p *TCPPeer) CloseStream(){
	p.wg.Done()
}

func (p *TCPPeer) Send(b []byte) error{
	_ ,err:=p.Conn.Write(b)
	return err
}


//return read only channel to read incoming msg received from another peer
func (t *TCPTransport) Consume() <-chan RPC{
	return t.rpcch
}

//implements Transport interface
func (t *TCPTransport) Close() error{
	return t.listener.Close()
}

func (t *TCPTransport) Addr()string{
	return t.ListenAddr
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
	for{
		rpc:=RPC{}

		err:=t.Decoder.Decode(conn,&rpc)
		
		if err!=nil{
			fmt.Printf("TCP error: %s\n",err)
			return
		}

		rpc.From = conn.RemoteAddr().String()

		if rpc.Stream{
			peer.wg.Add(1)
			fmt.Printf("%s incoming stream\n",conn.RemoteAddr())
			peer.wg.Wait()
			fmt.Printf("%s stream closed, resuming read loop\n",conn.RemoteAddr())
			continue
		}
		//sending rpc (msg) to channel
		t.rpcch <- rpc
	}

}