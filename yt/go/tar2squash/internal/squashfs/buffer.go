package squashfs

import "bytes"

type buffer struct {
	bytes.Buffer
}

func (b *buffer) Block() uint32 {
	return uint32(b.Len()) / metadataBlockSize
}

func (b *buffer) BlockStart() int64 {
	block := int64(b.Len() / metadataBlockSize)
	return block * (metadataBlockSize + 2)
}

func (b *buffer) Offset() uint16 {
	return uint16(b.Len() % metadataBlockSize)
}

func (b *buffer) Ref() int64 {
	return (b.BlockStart() << 16) | int64(b.Offset())
}

func (b *buffer) BlockCount() int {
	return (b.Len() + (metadataBlockSize - 1)) / metadataBlockSize
}
