package jobfs

type FileRow struct {
	FilePath  string `yson:"filename,omitempty"`
	IsDir     bool   `yson:"is_dir,omitempty"`
	PartIndex int    `yson:"part_index,omitempty"`
	Data      []byte `yson:"data,omitempty"`

	// For space files. Row with DataSize != 0, encodes DataSize of zeroes.
	DataSize int64 `yson:"data_size,omitempty"`
}
