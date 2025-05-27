package ytqueue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.ytsaurus.tech/yt/admin/timbertruck/internal/misc"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
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

	BatchSize int
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

	var compressor *compressor
	if config.CompressionCodec != "" {
		compressor, err = newCompressor(config.BatchSize, config.CompressionCodec)
		if err != nil {
			err = fmt.Errorf("failed to create compressor: %w", err)
			return
		}
		config.Logger.Debug("Compression codec is set", "compression_codec", compressor.codec())
	}
	out = &output{
		yc:           yc,
		queuePath:    ypath.Path(config.QueuePath),
		producerPath: ypath.Path(config.ProducerPath),
		sessionID:    config.SessionID,
		epoch:        createSessionResult.Epoch,

		compressor: compressor,

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
	yc           yt.Client
	queuePath    ypath.Path
	producerPath ypath.Path
	sessionID    string
	epoch        int64

	compressor *compressor

	logger *slog.Logger
}

func (o *output) Add(ctx context.Context, meta pipelines.RowMeta, row pipelines.Row) {
	codec := "null"
	value := row.Payload
	if o.compressor != nil {
		codec = o.compressor.codec()
		value = o.compressor.compress(row.Payload)
	}
	ytRow := ytRow{
		Value:          value,
		Codec:          codec,
		SourceURI:      o.sessionID,
		SequenceNumber: row.SeqNo,
	}

	retry(
		func() error {
			_, err := o.yc.PushQueueProducer(ctx, o.producerPath, o.queuePath, o.sessionID, o.epoch, []any{ytRow}, nil)
			return err
		},
		func(err error) { o.logger.Warn("error pushing queue, will retry", "error", err) },
	)
}

func (o *output) Close(ctx context.Context) {
	retry(
		func() error {
			return o.yc.RemoveQueueProducerSession(
				ctx,
				o.producerPath,
				o.queuePath,
				o.sessionID,
				&yt.RemoveQueueProducerSessionOptions{},
			)
		},
		func(err error) { o.logger.Warn("cannot remove queue producer session, will retry", "error", err) },
	)
	o.yc.Stop()
	if o.compressor != nil {
		o.compressor.close()
	}
}

func retry(action func() error, errHandler func(error)) {
	retryInterval := time.Second
	maxRetryInterval := time.Minute
	for {
		err := action()
		if err == nil {
			return
		}
		errHandler(err)
		time.Sleep(retryInterval)
		retryInterval *= 2
		if retryInterval > maxRetryInterval {
			retryInterval = maxRetryInterval
		}
	}
}
