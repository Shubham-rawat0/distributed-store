package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/Shubham-rawat0/distributed-store/p2p"
)

type FileServerOpts struct{
	StorageRoot			string
	PathTransformFunc	PathTransformFunc
	Transport			p2p.Transport
	BootStrapNodes		[]string
}

type FileServer struct{
	FileServerOpts
	peerLock			sync.Mutex
	peers				map[string]p2p.Peer
	store 				*store
	quitch				chan struct{}
}

func NewFileServer(opts FileServerOpts,)*FileServer{
	storeOpts:= storeOpts{
		Root: 				opts.StorageRoot,
		PathTransformFunc:  opts.PathTransformFunc,
	}

	return &FileServer{
		FileServerOpts: opts,
		store: 			NewStore(storeOpts),
		quitch:         make(chan struct{}),
		peers:			make(map[string]p2p.Peer),
	}
}

func (s *FileServer) Stop(){
	close(s.quitch)
}

func (s *FileServer) OnPeer(p p2p.Peer)error{
	s.peerLock.Lock()
	defer s.peerLock.Unlock()
	s.peers[p.RemoteAddr().String()] = p

	log.Printf("connected with remote %s",p.RemoteAddr())
	return nil
}

func (s *FileServer) loop(){
	defer func(){
		fmt.Println("File server stopped due to user quit action")
		s.Transport.Close()
	}()

	for {
		select {
		case msg:= <- s.Transport.Consume():
			fmt.Println("msg --- : ",msg)
		
		case <-s.quitch:
			return
		}
	}
}

func (s *FileServer) bootStrapNetworks() error{
	for _ , addr:= range s.BootStrapNodes{
		if len(addr)==0{
			continue
		}
		go func(addr string){
		fmt.Println("attempting to connect to the remote address",addr)
			if err:=s.Transport.Dial(addr);err!=nil{
				log.Println("dial err:",err)
			}
		}(addr)

	}
	return nil
}

func (s *FileServer) Start() error{
	if err:=s.Transport.ListenAndAccept(); err!=nil{
		return err
	}

	s.bootStrapNetworks()
	s.loop()

	return nil
}