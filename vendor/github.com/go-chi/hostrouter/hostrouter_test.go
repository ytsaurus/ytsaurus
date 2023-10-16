package hostrouter

import "testing"

func Test_getWildcardHost(t *testing.T) {
	type args struct {
		host string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"no wildcard in 1-part host", args{"com"}, "com"},
		{"wildcard in 2-part", args{"dot.com"}, "*.com"},
		{"wildcard in 3-part", args{"amazing.dot.com"}, "*.dot.com"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getWildcardHost(tt.args.host); got != tt.want {
				t.Errorf("getWildcardHost() = %v, want %v", got, tt.want)
			}
		})
	}
}
