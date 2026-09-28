package bus

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/guid"
)

func TestNullChecksum(t *testing.T) {
	parts := [][]byte{[]byte("binary\x00\xff"), {}, nil}
	for _, tc := range []struct {
		name                  string
		fixed, variable, part bool
		corrupt               string
	}{
		{"all present", true, true, true, ""},
		{"all omitted", false, false, false, ""},
		{"fixed omitted", false, true, true, ""},
		{"variable omitted", true, false, true, ""},
		{"parts omitted", true, true, false, ""},
		{"bad fixed", true, true, true, "fixed"},
		{"bad variable", true, true, true, "variable"},
		{"bad part", true, true, true, "part"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			packet := newMessagePacket(guid.New(), parts, packetFlagsNone, tc.part)
			fixed := packet.fixHeader.data(tc.fixed)
			variable := packet.varHeader.data(tc.variable)
			if tc.corrupt == "fixed" {
				binary.LittleEndian.PutUint64(fixed[28:], 1)
			}
			if tc.corrupt == "variable" {
				binary.LittleEndian.PutUint64(variable[len(variable)-8:], 1)
			}
			raw := append(append(fixed, variable...), parts[0]...)
			if tc.corrupt == "part" {
				raw[len(raw)-1] ^= 1
			}
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			require.NoError(t, b.SetDeadline(time.Now().Add(2*time.Second)))
			done := make(chan struct{})
			go func() { defer close(done); _, _ = a.Write(raw); _ = a.Close() }()
			got, err := NewBus(b, Options{}).Receive()
			_ = b.Close()
			<-done
			if tc.corrupt != "" {
				require.ErrorContains(t, err, "checksum mismatch")
			} else {
				require.NoError(t, err)
				require.Equal(t, parts, got.parts)
			}
		})
	}
}
