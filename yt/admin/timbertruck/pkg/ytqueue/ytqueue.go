package ytqueue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.ytsaurus.tech/yt/admin/timbertruck/internal/misc"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
	"go.ytsaurus.tech/yt/go/compression"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ytrpc"
)

const YTTvmID = 2031010

type Config struct {
	Cluster      string `yaml:"cluster"`
	QueuePath    string `yaml:"queue_path"`
	ProducerPath string `yaml:"producer_path"`

	RPCProxyRole     string `yaml:"rpc_proxy_role"`
	CompressionCodec string `yaml:"compression_codec"`
}

type OutputConfig struct {
	Cluster      string
	QueuePath    string
	ProducerPath string

	RPCProxyRole     string
	CompressionCodec string
	SessionID        string

	TVMFn yt.TVMFn
	Token string

	Logger *slog.Logger
}

func NewOutput(ctx context.Context, config OutputConfig) (out pipelines.Output[pipelines.Row], err error) {
	arcLogger := misc.NewArcadiaLevelCappingLogger(config.Logger, "ytclient")
	ytConfig := yt.Config{
		Proxy:     config.Cluster,
		ProxyRole: config.RPCProxyRole,
		Logger:    arcLogger.Structured(),
	}
	if config.TVMFn != nil {
		ytConfig.TVMFn = config.TVMFn
	} else if config.Token != "" {
		ytConfig.Token = config.Token
	}

	yc, err := ytrpc.NewClient(&ytConfig)
	if err != nil {
		return
	}

	createSessionResult, err := yc.CreateQueueProducerSession(
		ctx,
		ypath.Path(config.ProducerPath),
		ypath.Path(config.QueuePath),
		config.SessionID,
		&yt.CreateQueueProducerSessionOptions{},
	)
	if err != nil {
		err = fmt.Errorf("cannot create queue session: %w", err)
		return
	}

	var compressionCodec compression.Codec
	if config.CompressionCodec != "" {
		compressionCodec = newCompressionCodec(config.CompressionCodec)
		config.Logger.Debug("compression codec is set", "compression_codec", compressionCodec.GetID().String())
	}
	out = &output{
		yc:               yc,
		queuePath:        ypath.Path(config.QueuePath),
		producerPath:     ypath.Path(config.ProducerPath),
		sessionID:        config.SessionID,
		epoch:            createSessionResult.Epoch,
		compressionCodec: compressionCodec,

		logger: config.Logger,
	}
	return
}

type ytRow struct {
	Value          []byte `yson:"value"`
	Codec          string `yson:"codec"`
	SourceURI      string `yson:"source_uri"`
	SequenceNumber int64  `yson:"$sequence_number"`
}

type output struct {
	yc               yt.Client
	queuePath        ypath.Path
	producerPath     ypath.Path
	sessionID        string
	epoch            int64
	compressionCodec compression.Codec

	logger *slog.Logger
}

func (o *output) Add(ctx context.Context, meta pipelines.RowMeta, row pipelines.Row) {
	codec := "null"
	value := row.Payload
	if o.compressionCodec != nil && o.compressionCodec.GetID() != compression.CodecIDNone {
		codec = o.compressionCodec.GetID().String()
		value = o.compressValue(ctx, row.Payload)
	}
	ytRow := ytRow{
		Value:          value,
		Codec:          codec,
		SourceURI:      o.sessionID,
		SequenceNumber: row.SeqNo,
	}

	retryInterval := 1 * time.Second
	maxRetryInterval := 1 * time.Minute
	for {
		_, err := o.yc.PushQueueProducer(ctx, o.producerPath, o.queuePath, o.sessionID, o.epoch, []any{ytRow}, nil)
		if err == nil {
			break
		}
		o.logger.Warn("error pushing queue, will retry", "error", err)
		time.Sleep(retryInterval)
		retryInterval *= 2
		if retryInterval > maxRetryInterval {
			retryInterval = maxRetryInterval
		}
	}
}

func (o *output) compressValue(ctx context.Context, value []byte) []byte {
	retryInterval := time.Second
	maxRetryInterval := time.Minute
	for {
		compressedValue, err := o.compressionCodec.Compress(value)
		if err == nil {
			return compressedValue
		}
		o.logger.Error("error compress value before pushing queue, will retry", "error", err)
		time.Sleep(retryInterval)
		retryInterval *= 2
		if retryInterval > maxRetryInterval {
			retryInterval = maxRetryInterval
		}
	}
}

func (o *output) Close(ctx context.Context) {
	o.yc.Stop()
}

func newCompressionCodec(codecName string) compression.Codec {
	switch codecName {
	case compression.CodecIDZstd6.String():
		return compression.NewCodec(compression.CodecIDZstd6)
	default:
		return compression.CodecNone{}
	}
}
