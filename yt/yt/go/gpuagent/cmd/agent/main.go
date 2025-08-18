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
	socket   string
	logfile  string
	loglevel zapcore.Level
}

func parseArgs() (*args, error) {
	socketVar := flag.String("socket", "unix:///var/run/yt-gpu-agent.sock", "Unix socket to listen on")
	logfileVar := flag.String("logfile", "gpuagent.log", "Log file")
	loglevelVar := flag.String("loglevel", "info", "Log level")

	flag.Parse()

	socket, ok := strings.CutPrefix(*socketVar, "unix://")
	if !ok {
		return nil, fmt.Errorf("Invalid unix socket path")
	}

	loglevel, err := zapcore.ParseLevel(*loglevelVar)
	if err != nil {
		return nil, fmt.Errorf("Invalid log level")
	}

	return &args{socket: socket, logfile: *logfileVar, loglevel: loglevel}, nil
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

	if _, err := os.Stat(args.socket); !os.IsNotExist(err) {
		if err := os.RemoveAll(args.socket); err != nil {
			l.Fatalf("Fail to remove existing socket: %v", err)
			return
		}
	}

	gpuProvider, err := nv.New(l)
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

	listener, err := net.Listen("unix", args.socket)
	if err != nil {
		l.Fatalf("Fail to listen socket: %v", err)
		return
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
