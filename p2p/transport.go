package p2p

//peer is remote node
type Peer interface{
	Close() error
}

//handle communication between nodes
type Transport interface{
	ListenAndAccept() error
	consume() 		  <-chan RPC
}