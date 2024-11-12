package tcptest

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

// GetFreePort returns free local tcp port.
func GetFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()

	p := l.Addr().(*net.TCPAddr).Port

	return p, nil
}

// GetFreeAddr returns free localhost tcp address.
func GetFreeAddr() (string, error) {
	port, err := GetFreePort()
	if err != nil {
		return "", err
	}
	return makeLocalhostAddr(port), nil
}

// WaitForAddr tries to connect to given addr with constant backoff.
//
// Returns error if port is not ready after timeout.
func WaitForAddr(addr string, timeout time.Duration) error {
	stopTimer := time.NewTimer(timeout)
	defer stopTimer.Stop()

	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	for {
		select {
		case <-stopTimer.C:
			return fmt.Errorf("no server started listening on %s after timeout %s", addr, timeout)
		case <-ticker.C:
			if err := addrIsReady(addr); err != nil {
				break
			}
			return nil
		}
	}
}

// WaitForPort tries to connect to given local port with constant backoff.
//
// Returns error if port is not ready after timeout.
func WaitForPort(port int, timeout time.Duration) error {
	return WaitForAddr(makeLocalhostAddr(port), timeout)
}

func addrIsReady(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	return conn.Close()
}

func makeLocalhostAddr(port int) string {
	return net.JoinHostPort("localhost", strconv.Itoa(port))
}
