package pipelines

type Row struct {
	Payload []byte
	SeqNo   int64
}

type SkipRowReason string

const (
	SkipRowReasonTruncated          SkipRowReason = "truncated"
	SkipRowReasonCompressedTooLarge SkipRowReason = "compressed_too_large"
	SkipRowReasonUnparsed           SkipRowReason = "unparsed"
)

// AllSkipRowReasons contains all defined skip reasons.
var AllSkipRowReasons = []SkipRowReason{
	SkipRowReasonTruncated,
	SkipRowReasonCompressedTooLarge,
	SkipRowReasonUnparsed,
}

// SkippedRowInfo contains information about a skipped row.
type SkippedRowInfo struct {
	Reason SkipRowReason
	Offset FilePosition
	Attrs  map[string]any
}

type LargeRowKind string

const (
	LargeRowKindUncompressed LargeRowKind = "uncompressed"
	LargeRowKindCompressed   LargeRowKind = "compressed"
)

// AllLargeRowKinds contains all defined large row kinds.
var AllLargeRowKinds = []LargeRowKind{
	LargeRowKindUncompressed,
	LargeRowKindCompressed,
}

// LargeRowInfo contains information about a row that approaches the row size limit.
type LargeRowInfo struct {
	Kind   LargeRowKind
	Offset FilePosition
	Attrs  map[string]any
}
