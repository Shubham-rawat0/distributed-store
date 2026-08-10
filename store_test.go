package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestPathTransformFunc(t *testing.T){
	key:="lolbckuchbhi"
	PathKey:=CASPathTransformFunc(key)
	fmt.Println(PathKey.Pathname," --- ",PathKey.Filename)
}

func TestStoredeletekey(t *testing.T){
	opts:=storeOpts{
		PathTransformFunc: CASPathTransformFunc,
	}
	s:=NewStore(opts)

	key:="abeyghanta"
	data:=[]byte("lol bc majaak hai kya")

	if err:=s.writeStream(key,bytes.NewReader(data));err!=nil{
		fmt.Println("written")
		t.Error(err)
	}

	if err:=s.Delete(key);err!=nil{
		fmt.Println("deleted")
		t.Error(err)
	}

	f,_:=os.Create("new")
	f.Write([]byte("lol"))
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

	if ok:=s.Has(key);!ok{
		t.Errorf("expected to have key %s",key)
	}
	
	r,err:=s.Read(key)
	if err!=nil{
		t.Error(err)
	}

	b ,err:=io.ReadAll(r)
	fmt.Println("data: ",string(data)," b: ",string(b))
	if string(b)!=string(data){
		t.Errorf("want %s have %s",data , b)
	}

	s.Delete(key)
}