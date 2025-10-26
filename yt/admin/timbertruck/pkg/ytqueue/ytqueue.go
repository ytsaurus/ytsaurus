package ytqueue

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/ttlog"
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

	// BytesPerRowsBatch specifies the minimum total size in bytes of compressed rows to batch before pushing to YT queue.
	// If 0, each row is pushed immediately.
	//
	// Default value is 0.
	BytesPerRowsBatch int `yaml:"bytes_per_rows_batch"`
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

	// BytesPerRow specifies the maximum size in bytes of a row to compress.
	BytesPerRow int

	// BytesPerRowsBatch specifies the minimum total size in bytes of compressed rows to batch before pushing to YT queue.
	// If 0, each row is pushed immediately.
	BytesPerRowsBatch int

	OnSent func(meta pipelines.RowMeta)
}

func NewOutput(ctx context.Context, config OutputConfig) (out pipelines.Output[pipelines.Row], err error) {
	arcLogger := ttlog.NewArcadiaLevelCappingLogger(config.Logger, "ytclient")
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
		compressor, err = newCompressor(config.BytesPerRow, config.CompressionCodec)
		if err != nil {
			err = fmt.Errorf("failed to create compressor: %w", err)
			return
		}
		config.Logger.Debug("Compression codec is set", "compression_codec", compressor.codec())
	}
	o := &output{
		yc:           yc,
		queuePath:    ypath.Path(config.QueuePath),
		producerPath: ypath.Path(config.ProducerPath),
		sessionID:    config.SessionID,
		epoch:        createSessionResult.Epoch,

		compressor: compressor,

		bytesPerRowsBatch: config.BytesPerRowsBatch,

		logger: config.Logger,
		onSent: config.OnSent,
	}
	o.startAsync()
	out = o
	return
}

type ytRow struct {
	Value          []byte     `yson:"value"`
	Codec          string     `yson:"codec"`
	SourceURI      string     `yson:"source_uri"`
	Meta           *ytRowMeta `yson:"meta,omitempty"`
	SequenceNumber int64      `yson:"$sequence_number"`
}

type ytRowMeta struct {
	FirstBytesHash string `yson:"first_bytes_hash,omitempty"`
}

type output struct {
	yc           yt.Client
	queuePath    ypath.Path
	producerPath ypath.Path
	sessionID    string
	epoch        int64

	compressor *compressor

	logger *slog.Logger

	bytesPerRowsBatch int
	toCompress        chan sendItem
	toSend            chan sendItem
	compressorDone    chan struct{}
	senderDone        chan struct{}

	onSent func(meta pipelines.RowMeta)
}

type sendItem struct {
	ctx  context.Context
	row  ytRow
	meta pipelines.RowMeta
}

func (o *output) startAsync() {
	o.toCompress = make(chan sendItem, 1)
	o.toSend = make(chan sendItem, 1)
	o.compressorDone = make(chan struct{})
	o.senderDone = make(chan struct{})

	go o.compressorLoop()
	go o.senderLoop()
}

func (o *output) compressorLoop() {
	for item := range o.toCompress {
		if o.compressor != nil {
			item.row.Codec = o.compressor.codec()
			uncompressedSize := len(item.row.Value)
			startedAt := time.Now()
			item.row.Value = o.compressor.compress(item.row.Value)
			o.logger.Debug("Row compressed", "seq_no", item.row.SequenceNumber, "codec", item.row.Codec, "uncompressed_size", uncompressedSize, "compressed_size", len(item.row.Value), "duration_ms", time.Since(startedAt).Milliseconds())
		}
		o.toSend <- item
	}
	close(o.toSend)
	close(o.compressorDone)
}

func (o *output) senderLoop() {
	var batch []sendItem
	var totalRowsBytes int

	for item := range o.toSend {
		batch = append(batch, item)
		totalRowsBytes += len(item.row.Value)
		if totalRowsBytes >= o.bytesPerRowsBatch {
			o.flushBatch(batch)
			batch = batch[:0]
			totalRowsBytes = 0
		}
	}

	o.flushBatch(batch)
	close(o.senderDone)
}

func (o *output) flushBatch(batch []sendItem) {
	if len(batch) == 0 {
		return
	}

	rows := make([]any, len(batch))
	batchSize := 0
	for i := range batch {
		rows[i] = batch[i].row
		batchSize += len(batch[i].row.Value)
	}

	startedAt := time.Now()
	retry(
		func() error {
			_, err := o.yc.PushQueueProducer(batch[len(batch)-1].ctx, o.producerPath, o.queuePath, o.sessionID, o.epoch, rows, nil)
			return err
		},
		func(err error) { o.logger.Warn("error pushing queue, will retry", "error", err) },
	)

	o.logger.Debug("Rows pushed to YT Queue", "rows_count", len(batch), "batch_size_bytes", batchSize, "first_seq_no", batch[0].row.SequenceNumber, "last_seq_no", batch[len(batch)-1].row.SequenceNumber, "duration_ms", time.Since(startedAt).Milliseconds())

	if o.onSent != nil {
		o.onSent(batch[len(batch)-1].meta)
	}
}

func (o *output) Add(ctx context.Context, meta pipelines.RowMeta, row pipelines.Row) {
	var rowMeta *ytRowMeta
	if meta.Begin.LogicalOffset == 0 {
		// The buffer used to accumulate the row is assumed to be large enough (default is 16 MB),
		// so if len(row.Payload) < 1024, it means the entire file is smaller than 1024 bytes,
		// and we're hashing the whole file.
		hash := sha256.Sum256(row.Payload[:min(1024, len(row.Payload))])
		rowMeta = &ytRowMeta{FirstBytesHash: hex.EncodeToString(hash[:])}
	}
	// Copy payload to avoid aliasing with upstream buffer which may be mutated after Process returns.
	payloadCopy := bytes.Clone(row.Payload)
	ytRow := ytRow{
		Value:          payloadCopy,
		Codec:          "null",
		SourceURI:      o.sessionID,
		SequenceNumber: row.SeqNo,
		Meta:           rowMeta,
	}
	startedAt := time.Now()
	message := "Row pushed to send queue"
	if err := o.pushToSendQueue(ctx, meta, ytRow); err != nil {
		message = fmt.Sprintf("Row not pushed to send queue: %s", err.Error())
	}
	o.logger.Debug(message, "seq_no", row.SeqNo, "wait_ms", time.Since(startedAt).Milliseconds())
}

func (o *output) pushToSendQueue(ctx context.Context, meta pipelines.RowMeta, row ytRow) error {
	select {
	case o.toCompress <- sendItem{ctx: ctx, meta: meta, row: row}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o *output) Close(ctx context.Context) {
	close(o.toCompress)
	<-o.compressorDone
	<-o.senderDone
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
