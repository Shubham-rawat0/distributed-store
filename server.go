package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

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

type Message struct{
	From 		string
	Payload		any
}

type MessageStoreFile struct{
	Key 		string
	Size		int64
}

type MessageGetFile struct{
	Key string
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

func (s *FileServer) stream(msg *Message)  error{
	peers:=[]io.Writer{}
	for _ , peer:=range s.peers{
		peers=append(peers, peer)
	}

 	// MultiWriter creates a writer that duplicates its writes to all the provided writers,
	mw:=io.MultiWriter(peers...)
	return gob.NewEncoder(mw).Encode(msg)			
}

func (s *FileServer) broadcast(msg *Message) error{
	buf:=new(bytes.Buffer)
	if err:= gob.NewEncoder(buf).Encode(msg);err!=nil{
		return err
	}

	for _ ,peer := range s.peers{
		peer.Send([]byte{p2p.IncomingMessage})
		if err:=peer.Send(buf.Bytes());err!=nil{
			return err
		}
	}

	return nil
}

func (s *FileServer) Get(key string) (io.Reader, error){
	if s.store.Has(key){
		fmt.Printf("%s serving file %s from disk\n",s.Transport.Addr(),key)
		_,r,err:= s.store.Read(key)
		return r,err
	}

	fmt.Printf("%s don't have %s file in disk, fetching from network...\n",s.Transport.Addr(),key)
	msg:=Message{
		Payload: MessageGetFile{
			Key: key,
		},
	}

	if err:=s.broadcast(&msg);err!=nil{
		return nil,err
	}

	time.Sleep(time.Millisecond*500)

	for _ ,peer:=range s.peers{
		var fileSize int64
		binary.Read(peer,binary.LittleEndian,&fileSize)
		n,err:=s.store.Write(key,io.LimitReader(peer,fileSize))
		if err!=nil{
			return nil,err
		}

		fmt.Printf("%s received %d bytes over the network from %s",s.Transport.Addr(),n,peer.RemoteAddr())
		peer.CloseStream()
	}
	
		_,r,err:= s.store.Read(key)
		return r,err
}

func (s *FileServer) Store(key string , r io.Reader) error{
	var (
		fileBuffer =new(bytes.Buffer)
	    tee = io.TeeReader(r , fileBuffer)
		)

	size,err:=s.store.Write(key,tee)
	if err!=nil{
		return err
	}
	
	msg := Message{
		Payload: MessageStoreFile{
			Key:key,
			Size:size,
		},
	}

	if err:=s.broadcast(&msg);err!=nil{
		return err
	}

	time.Sleep(time.Millisecond*5)

	for _ ,peer := range s.peers{
		peer.Send([]byte{p2p.IncomingStream})
		n,err:=io.Copy(peer,fileBuffer)
		if err!=nil{
			return err
		}

		fmt.Println("received and written bytes",n)
	}

	return nil
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
		fmt.Println("File server stopped due to error or user quit action")
		s.Transport.Close()
	}()

	for {
		select {
		case rpc:= <- s.Transport.Consume():
			var msg Message
			if err:=gob.NewDecoder(bytes.NewBuffer(rpc.Payload)).Decode(&msg);err!=nil{
				log.Println("decoding error:",err)
				return 
			}
			if err:=s.handleMessage(rpc.From, &msg);err!=nil{
				log.Println("handle msg error:",err)
			}

		case <-s.quitch:
			return
		}
	}
}

func (s *FileServer) handleMessage(from string , msg *Message) error{
	switch v := msg.Payload.(type){
	case MessageStoreFile:
		return s.handleMessageStoreFile(from , v)
	case MessageGetFile:
		return s.handleMessageGetFile(from,v)
	}

	return nil
}

func (s *FileServer) handleMessageGetFile(from string,msg MessageGetFile) error{
	if !s.store.Has(msg.Key){
		return fmt.Errorf("file (%s) not present in disk, fetching from network...\n",msg)
	}

	fmt.Printf("%s serving file %s over the network\n",s.Transport.Addr(),msg.Key)
	fileSize,r,err:=s.store.Read(msg.Key)
	if err!=nil{
		return err
	}

	rc,ok:=r.(io.ReadCloser)
	if ok{
		fmt.Println("closing readcloser")
		defer rc.Close()
	}

	peer,ok:=s.peers[from]
	if !ok{
		return fmt.Errorf("peer %s not in peer map",peer)
	}

	peer.Send([]byte{p2p.IncomingStream})
	binary.Write(peer,binary.LittleEndian,fileSize)

	n,err:=io.Copy(peer,r)
	if err!=nil{
		return err
	}
	fmt.Printf("%s written %d bytes over the network to %s\n",s.Transport.Addr(),n,from)
	return nil
}

func (s *FileServer) handleMessageStoreFile(from string, msg MessageStoreFile) error{
	peer ,ok:=s.peers[from]
	if !ok{
		return fmt.Errorf("peer %s couldn't be found in peer list",from)
	}

	n,err:= s.store.Write(msg.Key,io.LimitReader(peer,int64(msg.Size)))
	if err!=nil{
		return err
	}
	
	log.Printf("%s written %d bytes to disk \n",s.Transport.Addr(),n)

	peer.CloseStream()
	return nil
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

func init(){
	gob.Register(MessageStoreFile{})
	gob.Register(MessageGetFile{})
}