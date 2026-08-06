package main

import (
	"fmt"
	"log"

	"github.com/Shubham-rawat0/distributed-store/p2p"
)

func OnPeer(p2p.Peer) error{
	fmt.Println("----------")
	return nil 
}

func main(){

	tcpOpts:=p2p.TCPTransportOpts{
		ListenAddr:		":3000",
		HandshakeFunc:  p2p.NOPHandshakeFunc,
		Decoder:     	p2p.DefaultDecoder{},	
	}

	fmt.Println("start main")

	tr:=p2p.NewTCPTransport(tcpOpts)

	go func(){
		for {
			msg:= <- tr.Consume()
			fmt.Println("message :",msg)
		}
	}()

	if err:=tr.ListenAndAccept(); err!=nil{
		log.Fatal(err)
	}

	select{}
}