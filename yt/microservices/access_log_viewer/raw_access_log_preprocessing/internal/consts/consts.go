package consts

const (
	CreationTimeAttribute  = "creation_time"
	MaxTimestampAttribute  = "max_timestamp"
	MinTimestampAttribute  = "min_timestamp"
	PartitionsNodeName     = "partitions"
	ProcessedAtAttribute   = "alv_processed_at"
	ParsedNodeName         = "parsed"
	ParsedUnsortedNodeName = "parsed_unsorted"
)

type ContextKey struct{}

var SpanIDKey = ContextKey{}
