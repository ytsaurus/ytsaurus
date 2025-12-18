package zstdsync

import (
	"encoding/binary"
	"fmt"
)

const (
	// MaxZstdFrameLength is the maximum size of a zstd frame.
	//
	// MaxZstdFrameLength is equivalent to MaxZstdFrameLength constant in the server code (yt/yt/core/logging/zstd_compression.cpp).
	MaxZstdFrameLength = 5263360

	// SyncTagLength is the length of the sync tag in bytes.
	SyncTagLength = 32
)

// SyncTagPrefix is the prefix of the sync tag.
var SyncTagPrefix = []byte{
	0x50, 0x2A, 0x4D, 0x18, // zstd skippable frame magic number
	0x18, 0x00, 0x00, 0x00, // data size: 128-bit ID + 64-bit offset

	// 128-bit sync tag ID
	0xF6, 0x79, 0x9C, 0x4E, 0xD1, 0x09, 0x90, 0x7E,
	0x29, 0x91, 0xD9, 0xE6, 0xBE, 0xE4, 0x84, 0x40,

	// 64-bit offset is written separately.
}

// WriteSyncTag creates a sync tag with the given offset.
// The sync tag is 32 bytes total: 24 bytes prefix + 8 bytes offset.
func WriteSyncTag(offset int64) []byte {
	syncTag := make([]byte, SyncTagLength)
	copy(syncTag, SyncTagPrefix)
	binary.LittleEndian.PutUint64(syncTag[len(SyncTagPrefix):], uint64(offset))
	return syncTag
}

// ReadSyncTagOffset reads the offset from a sync tag.
// The sync tag must be at least len(SyncTagPrefix) + 8 bytes.
// Panics if the buffer is too short.
func ReadSyncTagOffset(syncTag []byte) int64 {
	if len(syncTag) < len(SyncTagPrefix)+8 {
		panic(fmt.Sprintf("sync tag buffer too short: got %d bytes, need at least %d", len(syncTag), len(SyncTagPrefix)+8))
	}
	return int64(binary.LittleEndian.Uint64(syncTag[len(SyncTagPrefix):]))
}
