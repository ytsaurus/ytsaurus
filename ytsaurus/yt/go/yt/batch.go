package yt

// RowBatch is serialized blob containing batch of rows.
type RowBatch interface {
	// Len returns estimated memory consumption by this object.
	Len() int
}

type RowBatchWriter interface {
	TableWriter

	Batch() RowBatch
}
