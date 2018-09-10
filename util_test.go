package chord

import (
	"testing"
	"time"
)

func Test_isEqual(t *testing.T) {
	type args struct {
		a []byte
		b []byte
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isEqual(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("isEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_isPowerOfTwo(t *testing.T) {
	type args struct {
		num int
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPowerOfTwo(tt.args.num); got != tt.want {
				t.Errorf("isPowerOfTwo() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_randStabilize(t *testing.T) {
	type args struct {
		min time.Duration
		max time.Duration
	}
	tests := []struct {
		name string
		args args
		want time.Duration
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := randStabilize(tt.args.min, tt.args.max); got != tt.want {
				t.Errorf("randStabilize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_betweenRightIncl(t *testing.T) {
	t.Parallel()

	type args struct {
		key []byte
		a   []byte
		b   []byte
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"1", args{[]byte{1, 0, 0, 0}, []byte{0, 0, 0, 0}, []byte{1, 0, 0, 0}}, true},
		{"2", args{[]byte{1, 1, 1, 1}, []byte{1, 1, 1, 0}, []byte{1, 1, 1, 1}}, true},
		{"3", args{[]byte{1, 1, 1, 1, 1}, []byte{0}, []byte{1, 1, 1, 1}}, false},
		{"4", args{[]byte{1, 1, 1, 1, 1}, []byte{0}, []byte{1, 1, 1, 1, 1, 1}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := betweenRightIncl(tt.args.key, tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("betweenRightIncl() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_between(t *testing.T) {
	type args struct {
		key []byte
		a   []byte
		b   []byte
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := between(tt.args.key, tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("between() = %v, want %v", got, tt.want)
			}
		})
	}
}
