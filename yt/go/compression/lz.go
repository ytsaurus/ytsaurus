package compression

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/pierrec/lz4"

	"go.ytsaurus.tech/library/go/core/xerrors"
)

type CodecLz4 struct{}

func (c *CodecLz4) Compress(block []byte) ([]byte, error) {
	return lz4BlockCompress(block, lz4.CompressBlock)
}

func (c *CodecLz4) Decompress(block []byte) ([]byte, error) {
	return lz4BlockDecompress(block, lz4.UncompressBlock)
}

func (c *CodecLz4) GetID() CodecID {
	return CodecIDLz4
}

type CodecLz4HighCompression struct{}

func (c *CodecLz4HighCompression) Compress(block []byte) ([]byte, error) {
	return lz4BlockCompress(block, func(src, dst []byte, hashTable []int) (_ int, err error) {
		return lz4.CompressBlockHC(src, dst, 0)
	})
}

func (c *CodecLz4HighCompression) Decompress(block []byte) ([]byte, error) {
	return lz4BlockDecompress(block, lz4.UncompressBlock)
}

func (c *CodecLz4HighCompression) GetID() CodecID {
	return CodecIDLz4HighCompression
}

type lz4HeaderSignature uint32

const (
	lz4HeaderSignatureV1 lz4HeaderSignature = (1 << 30) + 1
	lz4HeaderSignatureV2 lz4HeaderSignature = (1 << 30) + 2
)

type lz4Header struct {
	Signature lz4HeaderSignature
	Size      uint32
}

const maxLz4BlockSize = 1 << 30

type lz4BlockHeader struct {
	CompressedSize   uint32
	UncompressedSize uint32
}

type lz4CompressFunc func(src, dst []byte, hashTable []int) (_ int, err error)

var lz4SignatureV1MaxLength = math.MaxInt32

func lz4BlockCompress(block []byte, compress lz4CompressFunc) ([]byte, error) {
	if len(block) == 0 {
		return nil, nil
	}

	var out bytes.Buffer

	h := lz4Header{
		Signature: lz4HeaderSignatureV1,
		Size:      uint32(len(block)),
	}

	useExtendedHeader := len(block) > lz4SignatureV1MaxLength
	if useExtendedHeader {
		h.Signature = lz4HeaderSignatureV2
		h.Size = 0
	}

	if err := binary.Write(&out, binary.LittleEndian, uint32(h.Signature)); err != nil {
		return nil, err
	}
	if err := binary.Write(&out, binary.LittleEndian, h.Size); err != nil {
		return nil, err
	}

	if useExtendedHeader {
		if err := binary.Write(&out, binary.LittleEndian, uint64(len(block))); err != nil {
			return nil, err
		}
	}

	for inputOffset, inputSize := 0, len(block); inputSize > 0; {
		blockSize := inputSize
		if blockSize > maxLz4BlockSize {
			blockSize = maxLz4BlockSize
		}

		compressedBlockSizeBound := blockSize
		if compressedBlockSizeBound < lz4.CompressBlockBound(blockSize) {
			compressedBlockSizeBound = lz4.CompressBlockBound(blockSize)
		}

		uncompressedBlock := block[inputOffset : inputOffset+blockSize]
		compressedBlock := make([]byte, compressedBlockSizeBound)
		compressedSize, err := compress(uncompressedBlock, compressedBlock, nil)
		if err != nil {
			return nil, err
		}

		blockHeader := lz4BlockHeader{
			CompressedSize:   uint32(compressedSize),
			UncompressedSize: uint32(blockSize),
		}

		if err := binary.Write(&out, binary.LittleEndian, blockHeader.CompressedSize); err != nil {
			return nil, err
		}
		if err := binary.Write(&out, binary.LittleEndian, blockHeader.UncompressedSize); err != nil {
			return nil, err
		}

		if _, err := out.Write(compressedBlock[:compressedSize]); err != nil {
			return nil, err
		}

		inputOffset += blockSize
		inputSize -= blockSize
	}

	return out.Bytes(), nil
}

type lz4DecompressFunc func(src, dst []byte) (int, error)

func lz4BlockDecompress(block []byte, decompress lz4DecompressFunc) ([]byte, error) {
	if len(block) == 0 {
		return nil, nil
	}

	if len(block) < 8 {
		return nil, xerrors.Errorf("unable to decode header: invalid length: %d", len(block))
	}

	var out bytes.Buffer

	h := lz4Header{
		Signature: lz4HeaderSignature(binary.LittleEndian.Uint32(block[:4])),
		Size:      binary.LittleEndian.Uint32(block[4:8]),
	}

	inputOffset := 8
	totalUncompressedSize := uint64(h.Size)
	if h.Signature == lz4HeaderSignatureV2 {
		if len(block) < 16 {
			return nil, xerrors.Errorf("unable to decode v2 header: invalid length: %d", len(block))
		}

		totalUncompressedSize = binary.LittleEndian.Uint64(block[8:16])
		inputOffset += 8
	}

	for inputOffset < len(block) {
		if len(block) < inputOffset+8 {
			return nil, xerrors.Errorf("unable to decode block header: invalid length")
		}

		blockHeader := lz4BlockHeader{
			CompressedSize:   binary.LittleEndian.Uint32(block[inputOffset : inputOffset+4]),
			UncompressedSize: binary.LittleEndian.Uint32(block[inputOffset+4 : inputOffset+8]),
		}
		inputOffset += 8

		if len(block) < inputOffset+int(blockHeader.CompressedSize) {
			return nil, xerrors.Errorf("not enough data to decode block: header expects %d bytes, got %d",
				blockHeader.CompressedSize, len(block)-inputOffset)
		}

		compressedBlock := block[inputOffset : inputOffset+int(blockHeader.CompressedSize)]
		uncompressedBlock := make([]byte, blockHeader.UncompressedSize)
		uncompressedSize, err := decompress(compressedBlock, uncompressedBlock)
		if err != nil {
			return nil, err
		}

		if uncompressedSize != int(blockHeader.UncompressedSize) {
			return nil, xerrors.Errorf("uncompressed block size mismatch: expected %d, got %d",
				blockHeader.UncompressedSize, uncompressedSize)
		}

		if _, err := out.Write(uncompressedBlock); err != nil {
			return nil, err
		}

		inputOffset += int(blockHeader.CompressedSize)
	}

	if uint64(out.Len()) != totalUncompressedSize {
		return nil, xerrors.Errorf("uncompressed data size mismatch: expected %d, got %d", out.Len(), h.Size)
	}

	return out.Bytes(), nil
}
