package httpserver

import (
	"context"
	"net"
	"net/http"
	"time"
)

type HTTPServer struct {
	http.Server

	// ready is closed when http server is ready to handle http requests.
	ready chan struct{}
	// realAddress contains the address (ip and port) on which the server is actually running.
	realAddress string
}

func New(endpoint string, handler http.Handler) *HTTPServer {
	return &HTTPServer{
		Server: http.Server{Addr: endpoint, Handler: handler},
		ready:  make(chan struct{}),
	}
}

// Run listens and serves http requests. It returns only after the server is stopped.
func (s *HTTPServer) Run() {
	addr := s.Addr
	if addr == "" {
		addr = ":http"
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}

	s.realAddress = ln.Addr().String()
	close(s.ready)

	err = s.Serve(ln)
	if err != nil && err != http.ErrServerClosed {
		panic(err)
	}
}

// WaitReady returns only after the server is ready to handle http requests.
func (s *HTTPServer) WaitReady() {
	<-s.ready
}

// IsReady returns true if the server is ready to handle http requests.
func (s *HTTPServer) IsReady() bool {
	select {
	case <-s.ready:
		return true
	default:
		return false
	}
}

// RealAddress returns the address the server is running on.
func (s *HTTPServer) RealAddress() string {
	if s.IsReady() {
		return s.realAddress
	} else {
		return ""
	}
}

// Stop tries to stop the server synchronously. It panics if the server is not stopped after 5s.
func (s *HTTPServer) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		panic(err)
	}
}
