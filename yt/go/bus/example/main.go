package main

import (
	"context"
	"flag"
	zp "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"time"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/bus"
)

var (
	Counter = 0
	Log     = zap.Must(zp.Config{
		Level:            zp.NewAtomicLevelAt(zp.InfoLevel),
		Encoding:         "console",
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:     "msg",
			LevelKey:       "level",
			TimeKey:        "ts",
			CallerKey:      "caller",
			EncodeLevel:    zapcore.CapitalColorLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
	})
	address = flag.String("address", "localhost:1234", "Address of YT bus server")
	flood   = flag.Bool("flood", true, "Flood")
)

func singleRun() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := bus.DialBus(ctx, bus.Config{
		Address: *address,
		Logger:  Log,
	})

	if err != nil {
		return err
	}

	defer client.Close()

	if err != nil {
		Log.Error("Blya", log.Error(err))
		return err
	}

	if err != nil {
		Log.Error("Blya", log.Error(err))
		return err
	}

	if err := client.Send([][]byte{[]byte("hello world")}); err != nil {
		Log.Error("Blya", log.Error(err))
		return err
	}

	data, err := client.Receive()
	if err != nil {
		return err
	}

	for _, d := range data {
		Log.Info("Receive data", log.Any("counter", Counter), log.Any("data", string(d)))
		Counter++
	}

	return nil
}

func main() {
	flag.Parse()
	Log.Info("Ping", log.Any("args", map[string]interface{}{
		"address": *address,
		"flood":   *flood,
	}))

	if err := singleRun(); err != nil {
		Log.Error("Run error", log.Error(err))
		return
	}

	if *flood {
		for i := 0; i < 1000; i++ {
			if err := singleRun(); err != nil {
				Log.Error("Run error", log.Error(err))
				return
			}
		}
	}
}
