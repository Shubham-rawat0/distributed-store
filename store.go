package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

const defaultRootFolderName = "disStorage"

func CASPathTransformFunc(key string) PathKey{
	hash:=sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	blocksize:=5
	sliceLen:=len(hashStr)/blocksize
	paths:=make([]string,sliceLen)

	for i:=range sliceLen{
		from ,to := i*blocksize,(i*blocksize)+blocksize
		paths[i]=hashStr[from:to]
	}

	return PathKey{
		Pathname: strings.Join(paths,"/"),
		Filename: hashStr,
	}
}

func (p PathKey) FirstPathName() string{
	paths:= strings.Split(p.Pathname,"/")
	if len(paths)==0{
		return ""
	}
	return paths[0]
}

func (p PathKey) FullPath() string{
	return fmt.Sprintf("%s/%s",p.Pathname,p.Filename)
}

type PathTransformFunc func(string) PathKey

type PathKey struct{
	Pathname 	string
	Filename	string
}

type storeOpts struct{
	Root 			  string
	PathTransformFunc PathTransformFunc
}

var DefaultPathTransformFunc = func(key string) PathKey{
	return PathKey{
		Pathname:key,
		Filename: key,
	}
} 

type store struct{
	storeOpts	
}

func NewStore(opts storeOpts) *store{
	if opts.PathTransformFunc==nil{
		opts.PathTransformFunc=DefaultPathTransformFunc
	}

	if opts.Root==""{
		opts.Root=defaultRootFolderName
	}

	return &store{
		storeOpts: opts,
	}
}

func (s *store) Has(key string) bool{
	pathKey:=s.PathTransformFunc(key)
	fullPathWithRoot:=fmt.Sprintf("%s/%s",s.Root,pathKey.FullPath())
	_ ,err:=os.Stat(fullPathWithRoot)

	return !errors.Is(err,os.ErrNotExist)
}

func (s *store) Clear() error{
	return os.RemoveAll(s.Root)
}

func (s *store) Delete(key string) error{
	pathKey:=s.PathTransformFunc(key)
	
	defer func(){
		log.Println("deleted file from disk: ",pathKey.FullPath())
	}()
	firstPathNameWithRoot:=fmt.Sprintf("%s/%s",s.Root,pathKey.FirstPathName())
	return os.RemoveAll(firstPathNameWithRoot)
}

func (s *store) Write(key string, r io.Reader) (int64 , error){
	return s.writeStream(key , r)
}

func (s *store) Read(key string)(int64,io.Reader , error){
	return s.readStream(key)
}

func (s *store) readStream(key string)(int64,io.ReadCloser , error){
	pathKey:=s.PathTransformFunc(key)
	fullPathWithRoot:=fmt.Sprintf("%s/%s",s.Root,pathKey.FullPath())

	file,err:=os.Open(fullPathWithRoot)
	if err!=nil{
		return 0,nil,err
	}
	fi,err:= file.Stat()
	if err!=nil{
		return 0,nil,err
	}

	return fi.Size(),file,err
}

func (s *store) WriteDecrypt(encKey []byte,key string, r io.Reader)(int64,error){
	pathKey :=s.PathTransformFunc(key)
	pathNameWithRoot:=fmt.Sprintf("%s/%s",s.Root,pathKey.Pathname)
	if err:=os.MkdirAll(pathNameWithRoot,os.ModePerm);err!=nil{
		return 0,err
	}

	fullPathNameWithRoot :=fmt.Sprintf("%s/%s",s.Root,pathKey.FullPath())
	f,err:=os.Create(fullPathNameWithRoot)
	if err!=nil{
		return 0,err
	}

	n,err:=copyDecrypt(encKey,r,f)
	return int64(n),err
}

// r is the source of the file data. It may be a network connection,
// a file, or any other type that implements io.Reader.
func (s *store) writeStream(key string, r io.Reader) (int64,error){
	pathKey :=s.PathTransformFunc(key)
	pathNameWithRoot:=fmt.Sprintf("%s/%s",s.Root,pathKey.Pathname)
	if err:=os.MkdirAll(pathNameWithRoot,os.ModePerm);err!=nil{
		return 0,err
	}

	fullPathNameWithRoot :=fmt.Sprintf("%s/%s",s.Root,pathKey.FullPath())
	f,err:=os.Create(fullPathNameWithRoot)
	if err!=nil{
		return 0,err
	}

	return io.Copy(f,r)
}