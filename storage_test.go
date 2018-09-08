package chord

import (
	"hash"
	"reflect"
	"testing"

	"github.com/arriqaaq/chord/internal"
)

func Test_arrayStore_hashKey(t *testing.T) {
	type fields struct {
		data map[string]string
		Hash hash.Hash
	}
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &arrayStore{
				data: tt.fields.data,
				Hash: tt.fields.Hash,
			}
			got, err := a.hashKey(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("arrayStore.hashKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("arrayStore.hashKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_arrayStore_Get(t *testing.T) {
	type fields struct {
		data map[string]string
		Hash hash.Hash
	}
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &arrayStore{
				data: tt.fields.data,
				Hash: tt.fields.Hash,
			}
			got, err := a.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("arrayStore.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("arrayStore.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_arrayStore_Set(t *testing.T) {
	type fields struct {
		data map[string]string
		Hash hash.Hash
	}
	type args struct {
		key   string
		value string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &arrayStore{
				data: tt.fields.data,
				Hash: tt.fields.Hash,
			}
			if err := a.Set(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("arrayStore.Set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_arrayStore_Delete(t *testing.T) {
	type fields struct {
		data map[string]string
		Hash hash.Hash
	}
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &arrayStore{
				data: tt.fields.data,
				Hash: tt.fields.Hash,
			}
			if err := a.Delete(tt.args.key); (err != nil) != tt.wantErr {
				t.Errorf("arrayStore.Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_arrayStore_Between(t *testing.T) {
	type fields struct {
		data map[string]string
		Hash hash.Hash
	}
	type args struct {
		from []byte
		to   []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*internal.KV
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &arrayStore{
				data: tt.fields.data,
				Hash: tt.fields.Hash,
			}
			got, err := a.Between(tt.args.from, tt.args.to)
			if (err != nil) != tt.wantErr {
				t.Errorf("arrayStore.Between() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("arrayStore.Between() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_arrayStore_MDelete(t *testing.T) {
	type fields struct {
		data map[string]string
		Hash hash.Hash
	}
	type args struct {
		keys []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &arrayStore{
				data: tt.fields.data,
				Hash: tt.fields.Hash,
			}
			if err := a.MDelete(tt.args.keys...); (err != nil) != tt.wantErr {
				t.Errorf("arrayStore.MDelete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
