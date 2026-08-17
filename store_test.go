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
	s:=newStore()
	defer teardown(t,s)

	for i:=range 1{
		key:=fmt.Sprintf("foo_%d",i)
		data:=fmt.Appendf([]byte{},"lol bc majaak hai kya, %d baar marunga",i)

		if _,err:=s.writeStream(key,bytes.NewReader(data));err!=nil{
			t.Error(err)
		}

		if ok:=s.Has(key);!ok{
			t.Errorf("expected to have key %s",key)
		}

		_,r,err:=s.Read(key)
		if err!=nil{
			t.Error(err)
		}

		b ,err:=io.ReadAll(r)
		fmt.Println("data: ",string(data)," b: ",string(b))
		if string(b)!=string(data){
			t.Errorf("want %s have %s",data , b)
		}

		if err:=s.Delete(key);err!=nil{
			t.Error(err)
		}

		if ok:=s.Has(key);!ok{
			t.Errorf("expected to have key %s",key)
		}
	}
}

func newStore() *store{
	opts:=storeOpts{
		PathTransformFunc: CASPathTransformFunc,
	}
	return NewStore(opts)
}

func teardown(t *testing.T , s *store){
	if err:=s.Clear();err!=nil{
		t.Error(err)
	}
}
