package main

import "github.com/Shubham-rawat0/distributed-store/p2p"


type FileServerOpts struct{
	StorageRoot			string
	PathTransformFunc	PathTransformFunc
	Transport			p2p.Transport
}

type FileServer struct{
	FileServerOpts
	store 				*store
}

func NewFileServer(opts FileServerOpts,)*FileServer{
	storeOpts:= storeOpts{
		Root: 				opts.StorageRoot,
		PathTransformFunc:  opts.PathTransformFunc,
	}

	return &FileServer{
		FileServerOpts: opts,
		store: 			NewStore(storeOpts),
	}
}

func (s *FileServerOpts) Start() error{
	if err:=s.Transport.ListenAndAccept(); err!=nil{
		return err
	}

	return nil
}