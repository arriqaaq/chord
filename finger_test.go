package chord

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"reflect"
	"testing"

	cm "github.com/zebra-uestc/chord/models/chord"
)

func TestNewFingerTable(t *testing.T) {
	//newFingerTable(node *cm.Node, m int)中的node.Id是一个byte数组,m为偏移量
	//NewInode(id string, addr string)将id转换为size为20位的byte数组
	//sha1.New().Size()=20
	//对于newFingerTable来说，返回的每个fingerEntry由fingerId和Node二元组组成
	//Node为输入的node
	//fingerId为起始id转换为byte数组后的id再加上offset
	g := newFingerTable(NewInode("8", "0.0.0.0:8003"), sha1.New().Size())
	for i, j := range g {
		fmt.Printf("%d, %x, %x\n", i, j.Id, j.RemoteNode.Id)
	}
}

func TestNewFingerEntry(t *testing.T) {
	hashSize := sha1.New().Size() * 8
	id := GetHashID([]byte("0.0.0.0:8083"))
	xInt := (&big.Int{}).SetBytes(id)
	for i := 0; i < 100; i++ {
		nextHash := fingerID(id, i, hashSize)
		aInt := (&big.Int{}).SetBytes(nextHash)

		fmt.Printf("%d, %d %d\n", xInt, aInt, hashSize)
	}
}

func Test_newFingerTable(t *testing.T) {
	type args struct {
		node *cm.Node
		m    int
	}
	tests := []struct {
		name string
		args args
		want fingerTable
	}{
		// TODO: Add test cases.
		// {"1", args{NewInode("8", "0.0.0.0:8083"), 1}, fingerTable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newFingerTable(tt.args.node, tt.args.m); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newFingerTable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newFingerEntry(t *testing.T) {
	type args struct {
		id   []byte
		node *cm.Node
	}
	tests := []struct {
		name string
		args args
		want *fingerEntry
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newFingerEntry(tt.args.id, tt.args.node); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newFingerEntry() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_fingerID(t *testing.T) {
	type args struct {
		n []byte
		i int
		m int
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fingerID(tt.args.n, tt.args.i, tt.args.m); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("fingerID() = %v, want %v", got, tt.want)
			}
		})
	}
}
