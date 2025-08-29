package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	grpcreflection "google.golang.org/grpc/reflection"

	logzap "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/library/go/core/log/zap/asynczap"
	"go.ytsaurus.tech/yt/go/ytlog/selfrotate"
	"go.ytsaurus.tech/yt/yt/go/gpuagent/internal/agent"
	"go.ytsaurus.tech/yt/yt/go/gpuagent/internal/agent/nv"
	"go.ytsaurus.tech/yt/yt/go/gpuagent/internal/pb"
)

type args struct {
	unixSocket string
	tcpPort    int
	logfile    string
	loglevel   zapcore.Level
	debug      bool
}

func parseArgs() (*args, error) {
	unixSocketVar := flag.String("socket", "", "Unix socket to listen on")
	tcpPortVar := flag.Int("tcp", 0, "TCP port to listen on")
	logfileVar := flag.String("logfile", "gpuagent.log", "Log file")
	loglevelVar := flag.String("loglevel", "info", "Log level")
	debugVar := flag.Bool("debug", false, "")

	flag.Parse()

	hasUnixSocket := unixSocketVar != nil && *unixSocketVar != ""
	hasTcpPort := tcpPortVar != nil && *tcpPortVar != 0

	if hasUnixSocket && hasTcpPort {
		return nil, fmt.Errorf("Either unix socket or tcp port must be specified")
	}
	if !hasUnixSocket && !hasTcpPort {
		return nil, fmt.Errorf("Either unix socket or tcp port must be specified")
	}

	loglevel, err := zapcore.ParseLevel(*loglevelVar)
	if err != nil {
		return nil, fmt.Errorf("Invalid log level")
	}

	if hasUnixSocket {
		socket, ok := strings.CutPrefix(*unixSocketVar, "unix://")
		if !ok {
			return nil, fmt.Errorf("Invalid unix socket path, should be `unix:///path/to/socket`")
		}
		return &args{unixSocket: socket, logfile: *logfileVar, loglevel: loglevel, debug: *debugVar}, nil
	}

	return &args{tcpPort: *tcpPortVar, logfile: *logfileVar, loglevel: loglevel, debug: *debugVar}, nil
}

func createLogger(a *args) (l *logzap.Logger, stop func(), err error) {
	rotateOptions := selfrotate.Options{
		Name:           a.logfile,
		MaxKeep:        2,
		MaxSize:        1 << 20,
		MinFreeSpace:   0.05,
		Compress:       selfrotate.CompressNone,
		RotateInterval: selfrotate.RotateInterval(time.Hour * 24),
	}

	logwriter, err := selfrotate.New(rotateOptions)
	if err != nil {
		log.Fatalf("Fail to create logger: %v", err)
		return
	}

	encoder := zap.NewProductionEncoderConfig()
	encoder.EncodeTime = zapcore.ISO8601TimeEncoder
	core := asynczap.NewCore(zapcore.NewJSONEncoder(encoder), logwriter, a.loglevel, asynczap.Options{})

	stop = func() {
		core.Stop()
		_ = logwriter.Close()
	}

	zl := zap.New(core, zap.AddCallerSkip(1), zap.AddCaller())
	return &logzap.Logger{L: zl}, stop, err
}

func createGPUProvider(args *args, l *logzap.Logger) (agent.GPUProvider, error) {
	if args.debug {
		return agent.NewDummyGPUProvider(l)
	}
	return nv.New(l)
}

func main() {
	args, err := parseArgs()
	if err != nil {
		log.Fatalf("Invalid arguments: %v", err)
		return
	}

	l, stopl, err := createLogger(args)
	if err != nil {
		log.Fatalf("Fail to create logger: %v", err)
		return
	}
	defer stopl()

	gpuProvider, err := createGPUProvider(args, l)
	if err != nil {
		l.Fatalf("Fail to create GPU provider: %v", err)
		return
	}

	err = gpuProvider.Init()
	if err != nil {
		l.Fatalf("%v", err)
		return
	}
	defer gpuProvider.Shutdown()

	var listener net.Listener = nil

	if args.unixSocket != "" {
		if _, err := os.Stat(args.unixSocket); !os.IsNotExist(err) {
			if err := os.RemoveAll(args.unixSocket); err != nil {
				l.Fatalf("Fail to remove existing socket: %v", err)
				return
			}
		}

		listener, err = net.Listen("unix", args.unixSocket)
		if err != nil {
			l.Fatalf("Fail to listen socket: %v", err)
			return
		}
	} else {
		listener, err = net.Listen("tcp", fmt.Sprintf("localhost:%d", args.tcpPort))
		if err != nil {
			l.Fatalf("Fail to listen port: %v", err)
			return
		}
	}
	defer listener.Close()

	server := grpc.NewServer()
	grpcreflection.Register(server)

	service, err := agent.New(gpuProvider, l)
	if err != nil {
		l.Fatalf("Fail to create service: %v", err)
		return
	}

	pb.RegisterGpuAgentServer(server, service)

	err = server.Serve(listener)
	if err != nil {
		l.Fatalf("Fail to start server: %v", err)
		os.Exit(1)
		return
	}

	l.Info("Shutdown")
}
