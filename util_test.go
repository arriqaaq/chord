package chord

import (
	"strconv"
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

func TestRL(t *testing.T) {
	t.Parallel()

	min := GetHashID("0.0.0.0:8081")
	max := GetHashID("0.0.0.0:8083")
	for i := 2; i < 100; i++ {
		val := strconv.Itoa(i)
		key := GetHashID(val)
		if got := betweenRightIncl(key, min, max); got != true {
			t.Errorf("betweenRightIncl() %s %x = %v, want %v", val, key, got, true)
		}
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
		{
			"5",
			args{
				[]byte{4, 40, 171},
				[]byte{53, 106, 25, 43, 121, 19, 176, 76, 84, 87, 77, 24, 194, 141, 70, 230, 57, 84, 40, 171},
				[]byte{4, 40, 171},
			},
			true,
		},
		{"6", args{GetHashID("11"), GetHashID("1"), GetHashID("20")}, true},
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
