package chord

import (
	"hash"
	"reflect"
	"testing"

	"github.com/arriqaaq/chord/models"
)

func TestNewMapStore(t *testing.T) {
	type args struct {
		hashFunc func() hash.Hash
	}
	tests := []struct {
		name string
		args args
		want Storage
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewMapStore(tt.args.hashFunc); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewMapStore() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mapStore_hashKey(t *testing.T) {
	type fields struct {
		data map[string]string
		Hash func() hash.Hash
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
			a := &mapStore{
				data: tt.fields.data,
				Hash: tt.fields.Hash,
			}
			got, err := a.hashKey(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("mapStore.hashKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mapStore.hashKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mapStore_Get(t *testing.T) {
	type fields struct {
		data map[string]string
		Hash func() hash.Hash
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
			a := &mapStore{
				data: tt.fields.data,
				Hash: tt.fields.Hash,
			}
			got, err := a.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("mapStore.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mapStore.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mapStore_Set(t *testing.T) {
	type fields struct {
		data map[string]string
		Hash func() hash.Hash
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
			a := &mapStore{
				data: tt.fields.data,
				Hash: tt.fields.Hash,
			}
			if err := a.Set(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("mapStore.Set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_mapStore_Delete(t *testing.T) {
	type fields struct {
		data map[string]string
		Hash func() hash.Hash
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
			a := &mapStore{
				data: tt.fields.data,
				Hash: tt.fields.Hash,
			}
			if err := a.Delete(tt.args.key); (err != nil) != tt.wantErr {
				t.Errorf("mapStore.Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_mapStore_Between(t *testing.T) {
	type fields struct {
		data map[string]string
		Hash func() hash.Hash
	}
	type args struct {
		from []byte
		to   []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*models.KV
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &mapStore{
				data: tt.fields.data,
				Hash: tt.fields.Hash,
			}
			got, err := a.Between(tt.args.from, tt.args.to)
			if (err != nil) != tt.wantErr {
				t.Errorf("mapStore.Between() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mapStore.Between() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mapStore_MDelete(t *testing.T) {
	type fields struct {
		data map[string]string
		Hash func() hash.Hash
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
			a := &mapStore{
				data: tt.fields.data,
				Hash: tt.fields.Hash,
			}
			if err := a.MDelete(tt.args.keys...); (err != nil) != tt.wantErr {
				t.Errorf("mapStore.MDelete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
