package main

import (
	"bytes"
	"log"

	"github.com/Shubham-rawat0/distributed-store/p2p"
)

func makeServer(listenAddr string, nodes ...string) *FileServer{

	tcptransportOpts:=p2p.TCPTransportOpts{
		ListenAddr:		 listenAddr,
		HandshakeFunc:   p2p.NOPHandshakeFunc,
		Decoder: 		 p2p.DefaultDecoder{},
	}

	tcpTransport:=p2p.NewTCPTransport(tcptransportOpts)

	fileServerOpts:= FileServerOpts{
		StorageRoot: 		listenAddr + "3000_network",
		PathTransformFunc:  CASPathTransformFunc,
		Transport:          tcpTransport,	
		BootStrapNodes:     nodes,
	}
	
	s:= NewFileServer(fileServerOpts)
	tcpTransport.OnPeer=s.OnPeer
	return s
}

func main(){

	s1:=makeServer(":3000","")
	s2:=makeServer(":4000",":3000")

	go func (){
		log.Fatal((s1.Start()))
	}()

	s2.Start()
	// data:=bytes.NewReader([]byte("my big data file"))

	// // s2.StoreFile("key",data)
}