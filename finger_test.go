package chord

import (
	"crypto/sha1"
	"fmt"
	"github.com/arriqaaq/chord/internal"
	"reflect"
	"testing"
)

func TestNewFingerTable(t *testing.T) {
	g := newFingerTable(NewInode("8", "0.0.0.0:8003"), sha1.New().Size())
	for i, j := range g {
		fmt.Printf("%d, %x, %x\n", i, j.Id, j.Node.Id)
	}
}

func Test_newFingerTable(t *testing.T) {
	type args struct {
		node *internal.Node
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
		node *internal.Node
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
