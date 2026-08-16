package p2p

import (
	"encoding/gob"
	"io"
)

// r is the source of the file data. It may be a network connection,
// a file, or any other type that implements io.Reader. io.Reader has Read() method
type Decoder interface{
	Decode(io.Reader ,*RPC) error
}

type GOBDecoder struct{}

func (dec GOBDecoder) Decode(r io.Reader,msg *RPC) error{
	return gob.NewDecoder(r).Decode(msg)
}

type DefaultDecoder struct{}

func (dec DefaultDecoder) Decode(r io.Reader,msg *RPC) error{
	peekBuf:=make([]byte,1)

	if _,err:=r.Read(peekBuf);err!=nil{
		return nil
	}

	//no need to decode if streaming
	stream:=peekBuf[0]==IncomingStream
	if stream{
		msg.Stream=true
		return nil
	}

	buf := make([]byte,1020)
	n,err:=r.Read(buf)
	if err!=nil{
		return err
	}

	msg.Payload=buf[:n]
	return nil
}