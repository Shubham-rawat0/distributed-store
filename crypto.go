package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"io"
)

func generateId() string{
	buf:=make([]byte,32)
	io.ReadFull(rand.Reader,buf)
	return hex.EncodeToString(buf)
}

func hashKey(key string) string{
	hash:=md5.Sum([]byte(key))
	return hex.EncodeToString(hash[:])
}

func newEncryptionKey()[]byte{
	keyBuf:=make([]byte,32)
	io.ReadFull(rand.Reader,keyBuf)
	return keyBuf
}


func copyStream(stream cipher.Stream, blockSize int, src io.Reader, dst io.Writer) (int, error) {
	var (
		buf = make([]byte, 32*1024)
		nw  = blockSize
	)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			stream.XORKeyStream(buf, buf[:n]) //encrypting , decrypting
			nn, err := dst.Write(buf[:n])
			if err != nil {
				return 0, err
			}
			nw += nn
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
	}
	return nw, nil
}

func copyDecrypt(key []byte,src io.Reader,dst io.Writer)(int,error){
	block ,err:=aes.NewCipher(key) 
	if err!=nil{
		return 0,err
	}

	//read the iv from reader i.e, the block.BlockSize() bytes
	iv:=make([]byte,block.BlockSize())
	if _ ,err:=src.Read(iv);err!=nil{
		return 0,err
	}
		stream := cipher.NewCTR(block,iv) 

	return copyStream(stream, block.BlockSize(), src, dst)
}

func copyEncrypt(key []byte,src io.Reader,dst io.Writer)(int,error){
	block ,err:=aes.NewCipher(key) //generate a new cipher bloack configured with key
	if err!=nil{
		return 0,err
	}

	//initial value 
	iv:= make([]byte,block.BlockSize())	//16 byte
	
	//crypto generated random iv
	if _, err:=io.ReadFull(rand.Reader,iv);err!=nil{
		return 0,err
	}

	if _,err:=dst.Write(iv);err!=nil{ //sending same iv to dst
		return 0,err
	}
	stream := cipher.NewCTR(block,iv) //return aes-ctr stream (ctr mode) which implements encryption method
	
	return copyStream(stream, block.BlockSize(), src, dst)
}

//encryption
// key + IV
//    ↓
//  AES-CTR
//    ↓
// keystream
//    ↓
// plaintext XOR keystream
//    ↓
// ciphertext


// decryption
// key + SAME IV
//    ↓
//  AES-CTR
//    ↓
// SAME keystream
//    ↓
// ciphertext XOR keystream
//    ↓
// plaintext