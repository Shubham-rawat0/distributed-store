package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
)

func newEncryptionKey()[]byte{
	keyBuf:=make([]byte,32)
	io.ReadFull(rand.Reader,keyBuf)
	return keyBuf
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

	var (
		buf = make([]byte,32*1024)
		stream = cipher.NewCTR(block,iv) 
	)

	for {
		n,err:=src.Read(buf)
		if n>0{
			stream.XORKeyStream(buf,buf[:n])
			if _,err:=dst.Write(buf[:n]);err!=nil{
				return 0,err
			}
		}
		if err==io.EOF{
			break
		}
		if err!=nil{
		return 0,err
		}
	}
	return 0,nil
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

	var (
		buf = make([]byte,32*1024)
		stream = cipher.NewCTR(block,iv) //return aes-ctr stream (ctr mode) which implements encryption method
	)
	for {
		n,err:=src.Read(buf)  //read from src to buf
		if n>0{
			stream.XORKeyStream(buf,buf[:n]) //encrypting
			if _ ,err:=dst.Write(buf[:n]);err!=nil{
				return 0,err
			}
		}
		if err==io.EOF{
			break
		}
		if err!=nil{
		return 0,err
		}
	}

	return 0,nil
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