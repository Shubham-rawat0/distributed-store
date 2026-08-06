package p2p

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestTCPTransport(t *testing.T){
	listenAddr:=":3000"

	tcpOpts:=TCPTransportOpts{
		ListenAddr:listenAddr,
		HandshakeFunc : NOPHandshakeFunc,
		Decoder:     DefaultDecoder{},	
	}

	
	tr:=NewTCPTransport(tcpOpts)

	assert.Equal(t,tr.ListenAddr,listenAddr)

	//server
	err := tr.ListenAndAccept()
	assert.NoError(t, err)

	defer tr.listener.Close()
	select{}
}