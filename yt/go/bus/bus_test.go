package bus

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/guid"
)

func connPair() (a, b net.Conn, err error) {
	var lsn net.Listener
	lsn, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return
	}
	defer func() { _ = lsn.Close() }()

	a, err = net.Dial("tcp", lsn.Addr().String())
	if err != nil {
		return
	}

	b, err = lsn.Accept()
	if err != nil {
		_ = a.Close()
		a = nil
	}
	return
}

func TestBusPingPong(t *testing.T) {
	a, b, err := connPair()
	require.NoError(t, err)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	busA := NewBus(a, Options{})
	busB := NewBus(b, Options{})

	message := [][]byte{
		[]byte("ping"),
		[]byte("pong"),
		[]byte(""),
		nil,
	}

	err = busA.Send(guid.New(), message, &busSendOptions{})
	require.NoError(t, err)

	transferred, err := busB.Receive()
	require.NoError(t, err)
	require.Equal(t, packetMessage, transferred.fixHeader.typ)
	require.Equal(t, message, transferred.parts)
}
