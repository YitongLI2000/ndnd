package engine

import "testing"

func TestNewDefaultFaceTcpLocality(t *testing.T) {
	tests := []struct {
		name      string
		transport string
		wantLocal bool
	}{
		{name: "IPv4 loopback", transport: "tcp://127.0.0.1:6363", wantLocal: true},
		{name: "IPv4 loopback explicit", transport: "tcp4://127.0.0.1:6363", wantLocal: true},
		{name: "IPv6 loopback", transport: "tcp6://[::1]:6363", wantLocal: true},
		{name: "localhost", transport: "tcp://localhost:6363", wantLocal: true},
		{name: "remote IPv4", transport: "tcp4://192.0.2.1:6363", wantLocal: false},
		{name: "remote IPv6", transport: "tcp6://[2001:db8::1]:6363", wantLocal: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("NDN_CLIENT_TRANSPORT", test.transport)
			face := NewDefaultFace()
			if got := face.IsLocal(); got != test.wantLocal {
				t.Fatalf("NewDefaultFace().IsLocal() = %v, want %v", got, test.wantLocal)
			}
		})
	}
}
