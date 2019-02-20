package yt

type TableWriter interface {
	Write(value interface{}) error
	Close() error
}

type TableReader interface {
	Scan(value interface{}) error
	Next() bool
	Err() error
	Close() error
}
