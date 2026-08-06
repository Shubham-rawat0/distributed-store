package main

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

func TestPathTransformFunc(t *testing.T){
	key:="lolbckuchbhi"
	PathKey:=CASPathTransformFunc(key)
	fmt.Println(PathKey.Pathname," --- ",PathKey.Filename)
}

func TestStore(t *testing.T){
	opts:=storeOpts{
		PathTransformFunc: CASPathTransformFunc,
	}
	s:=NewStore(opts)

	key:="abeyghanta"
	data:=[]byte("lol bc majaak hai kya")

	if err:=s.writeStream(key,bytes.NewReader(data));err!=nil{
		t.Error(err)
	}

	r,err:=s.Read(key)
	if err!=nil{
		t.Error(err)
	}

	b ,err:=io.ReadAll(r)
	fmt.Println("data: ",data," b: ",b)
	if string(b)!=string(data){
		t.Errorf("want %s have %s",data , b)
	}
}