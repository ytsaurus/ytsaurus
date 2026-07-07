package sockets

import "testing"

func TestInmemSocket(t *testing.T) {
	l := NewInmemSocket("test", 0)
	defer func() { _ = l.Close() }()
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			_, _ = conn.Write([]byte("hello"))
			_ = conn.Close()
		}
	}()

	conn, err := l.Dial("test", "test")
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 5)
	_, err = conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}

	if string(buf) != "hello" {
		t.Fatalf("expected `hello`, got %s", string(buf))
	}

	_ = l.Close()
	_, err = l.Dial("test", "test")
	if err != errClosed {
		t.Fatalf("expected `errClosed` error, got %v", err)
	}
}
