// Package squashfs implements writing SquashFS file system images using zlib
// compression for data blocks (inodes and directory entries are written
// uncompressed for simplicity).
//
// Note that SquashFS requires directory entries to be sorted, i.e. files and
// directories need to be added in the correct order.
//
// This package intentionally only implements a subset of SquashFS. Notably,
// block devices, character devices and xattrs are not supported.
package squashfs

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// inode contains a block number + offset within that block.
type Inode int64

const (
	zlibCompression = 1 + iota
	lzmaCompression
	lzoCompression
	xzCompression
	lz4Compression
)

const (
	invalidFragment = 0xFFFFFFFF
	invalidXattr    = 0xFFFFFFFF
)

type superblock struct {
	Magic               uint32
	Inodes              uint32
	MkfsTime            int32
	BlockSize           uint32
	Fragments           uint32
	Compression         uint16
	BlockLog            uint16
	Flags               uint16
	NoIds               uint16
	Major               uint16
	Minor               uint16
	RootInode           Inode
	BytesUsed           int64
	IDTableStart        int64
	XattrIDTableStart   int64
	InodeTableStart     int64
	DirectoryTableStart int64
	FragmentTableStart  int64
	LookupTableStart    int64
}

const (
	dirType = 1 + iota
	fileType
	symlinkType
	blkdevType
	chrdevType
	fifoType
	socketType
	// The larger types are used for e.g. sparse files, xattrs, etc.
	ldirType
	lregType
	lsymlinkType
	lblkdevType
	lchrdevType
	lfifoType
	lsocketType
)

type inodeHeader struct {
	InodeType   uint16
	Mode        uint16
	UID         uint16
	GID         uint16
	Mtime       int32
	InodeNumber uint32
}

// fileType
type regInodeHeader struct {
	inodeHeader

	// full byte offset from the start of the file system, e.g. 96 for first
	// file contents. Not using fragments limits us to 2^32-1-96 (≈ 4GiB) bytes
	// of file contents.
	StartBlock uint32
	Fragment   uint32
	Offset     uint32
	FileSize   uint32

	// Followed by a uint32 array of compressed block sizes.
}

// lregType
type lregInodeHeader struct {
	inodeHeader

	// full byte offset from the start of the file system, e.g. 96 for first
	// file contents. Not using fragments limits us to 2^32-1-96 (≈ 4GiB) bytes
	// of file contents.
	StartBlock uint64
	FileSize   uint64
	Sparse     uint64
	Nlink      uint32
	Fragment   uint32
	Offset     uint32
	Xattr      uint32

	// Followed by a uint32 array of compressed block sizes.
}

// symlinkType
type symlinkInodeHeader struct {
	inodeHeader

	Nlink       uint32
	SymlinkSize uint32

	// Followed by a byte array of SymlinkSize bytes.
}

// chrdevType and blkdevType
type devInodeHeader struct {
	inodeHeader

	Nlink uint32
	Rdev  uint32
}

// fifoType and socketType
type ipcInodeHeader struct {
	inodeHeader

	Nlink uint32
}

// dirType
type dirInodeHeader struct {
	inodeHeader

	StartBlock  uint32
	Nlink       uint32
	FileSize    uint16
	Offset      uint16
	ParentInode uint32
}

// ldirType
type ldirInodeHeader struct {
	inodeHeader

	Nlink       uint32
	FileSize    uint32
	StartBlock  uint32
	ParentInode uint32
	Icount      uint16
	Offset      uint16
	Xattr       uint32
}

type dirHeader struct {
	Count       uint32
	StartBlock  uint32
	InodeOffset uint32
}

func (d *dirHeader) Unmarshal(b []byte) {
	_ = b[11]
	e := binary.LittleEndian
	d.Count = e.Uint32(b)
	d.StartBlock = e.Uint32(b[4:])
	d.InodeOffset = e.Uint32(b[8:])
}

type dirEntry struct {
	Offset      uint16
	InodeNumber int16
	EntryType   uint16
	Size        uint16

	// Followed by a byte array of Size bytes.
}

func (d *dirEntry) Unmarshal(b []byte) {
	_ = b[7]
	e := binary.LittleEndian
	d.Offset = e.Uint16(b)
	d.InodeNumber = int16(e.Uint16(b[2:]))
	d.EntryType = e.Uint16(b[4:])
	d.Size = e.Uint16(b[6:])
}

// xattr types
const (
	XattrTypeUser = iota
	XattrTypeTrusted
	XattrTypeSecurity
)

var xattrPrefix = map[int]string{
	XattrTypeUser:     "user.",
	XattrTypeTrusted:  "trusted.",
	XattrTypeSecurity: "security.",
}

type Xattr struct {
	Type     uint16
	FullName string
	Value    []byte
}

func XattrFromAttr(attr string, val []byte) Xattr {
	for typ, prefix := range xattrPrefix {
		if !strings.HasPrefix(attr, prefix) {
			continue
		}
		return Xattr{
			Type:     uint16(typ),
			FullName: strings.TrimPrefix(attr, prefix),
			Value:    val,
		}
	}
	return Xattr{}
}

type xattrID struct {
	Xattr uint64
	Count uint32
	Size  uint32
}

func writeIDTable(w io.WriteSeeker, ids []uint32) (start int64, err error) {
	metaOff, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, ids); err != nil {
		return 0, err
	}

	if err := binary.Write(w, binary.LittleEndian, uint16(buf.Len())|0x8000); err != nil {
		return 0, err
	}
	if _, err := io.Copy(w, &buf); err != nil {
		return 0, err
	}
	off, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	return off, binary.Write(w, binary.LittleEndian, metaOff)
}

type fullDirEntry struct {
	startBlock  uint32
	offset      uint16
	inodeNumber uint32
	entryType   uint16
	name        string
}

const (
	magic             = 0x73717368
	dataBlockSize     = 131072
	metadataBlockSize = 8192
	majorVersion      = 4
	minorVersion      = 0
)

type Writer struct {
	// Root represents the file system root. Like all directories, Flush must be
	// called precisely once.
	Root *Directory

	xattrs   []Xattr
	xattrIDs []xattrID

	w io.WriteSeeker

	sb       superblock
	inodeBuf bytes.Buffer
	dirBuf   bytes.Buffer

	writeInodeNumTo map[string][]int64
}

// TODO: document what this is doing and what it is used for
func slog(block uint32) uint16 {
	for i := uint16(12); i <= 20; i++ {
		if block == (1 << i) {
			return i
		}
	}
	return 0
}

// filesystemFlags returns flags for a SquashFS file system created by this
// package (disabling most features for now).
func filesystemFlags() uint16 {
	const (
		noI = 1 << iota // uncompressed metadata
		noD             // uncompressed data
		_
		noF               // uncompressed fragments
		noFrag            // never use fragments
		alwaysFrag        // always use fragments
		duplicateChecking // de-duplication
		exportable        // exportable via NFS
		noX               // uncompressed xattrs
		noXattr           // no xattrs
		compopt           // compressor-specific options present?
	)
	return noI | noF | noFrag | noX | noXattr
}

// NewWriter returns a Writer which will write a SquashFS file system image to w
// once Flush is called.
//
// Create new files and directories with the corresponding methods on the Root
// directory of the Writer.
//
// File data is written to w even before Flush is called.
func NewWriter(w io.WriteSeeker, mkfsTime time.Time) (*Writer, error) {
	// Skip over superblock to the data area, we come back to the superblock
	// when flushing.
	if _, err := w.Seek(96, io.SeekStart); err != nil {
		return nil, err
	}
	wr := &Writer{
		w: w,
		sb: superblock{
			Magic:             magic,
			MkfsTime:          int32(mkfsTime.Unix()),
			BlockSize:         dataBlockSize,
			Fragments:         0,
			Compression:       zlibCompression,
			BlockLog:          slog(dataBlockSize),
			Flags:             filesystemFlags(),
			NoIds:             1, // just one uid/gid mapping (for root)
			Major:             majorVersion,
			Minor:             minorVersion,
			XattrIDTableStart: -1, // not present
			LookupTableStart:  -1, // not present
		},
		writeInodeNumTo: make(map[string][]int64),
	}
	wr.Root = &Directory{
		w:        wr,
		name:     "", // root
		modTime:  mkfsTime,
		xattrRef: invalidXattr,
	}
	return wr, nil
}

// Directory represents a SquashFS directory.
type Directory struct {
	w          *Writer
	name       string
	modTime    time.Time
	dirEntries []fullDirEntry
	parent     *Directory

	xattrRef uint32
}

func (d *Directory) path() string {
	if d.parent == nil {
		return d.name
	}
	return filepath.Join(d.parent.path(), d.name)
}

type File struct {
	w       *Writer
	d       *Directory
	off     int64
	size    uint32
	name    string
	modTime time.Time
	mode    uint16

	inodeStartBlock uint32
	inodeOffset     uint16
	inode           uint32

	// buf accumulates at least dataBlockSize bytes, at which point a new block
	// is being written.
	buf bytes.Buffer

	// blocksizes stores, for each block of dataBlockSize bytes (uncompressed),
	// the number of bytes the block compressed down to.
	blocksizes []uint32

	// compBuf is used for holding a block during compression to avoid memory
	// allocations.
	compBuf *bytes.Buffer
	// zlibWriter is re-used for each compressed block
	zlibWriter *zlib.Writer

	xattrRef uint32
}

// Directory creates a new directory with the specified name and modTime.
func (d *Directory) Directory(name string, modTime time.Time) *Directory {
	return &Directory{
		w:        d.w,
		name:     name,
		modTime:  modTime,
		parent:   d,
		xattrRef: invalidXattr,
	}
}

func (d *Directory) SetXattrs(xattrs []Xattr) {
	d.xattrRef = d.w.addXattrs(xattrs)
}

func (w *Writer) addXattrs(xattrs []Xattr) uint32 {
	xattrRef := uint32(len(w.xattrs))

	w.xattrs = append(w.xattrs, xattrs[0]) // TODO: support multiple
	size := len(xattrs[0].FullName) + len(xattrs[0].Value)
	w.xattrIDs = append(w.xattrIDs, xattrID{
		// Xattr is populated in writeXattrTables
		Count: 1, // TODO: support multiple
		Size:  uint32(size),
	})

	return xattrRef
}

func (w *Writer) NewFile(modTime time.Time, mode uint16, xattrs []Xattr) (*File, error) {
	off, err := w.w.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	// zlib.BestSpeed results in only a 2x slow-down over no compression
	// (compared to >4x slow-down with DefaultCompression), but generates
	// results which are in the same ball park (10% larger).
	zw, err := zlib.NewWriterLevel(nil, zlib.BestSpeed)
	if err != nil {
		return nil, err
	}

	xattrRef := uint32(invalidXattr)
	if len(xattrs) > 0 {
		xattrRef = w.addXattrs(xattrs)
	}

	return &File{
		w:          w,
		off:        off,
		modTime:    modTime,
		mode:       mode,
		compBuf:    bytes.NewBuffer(make([]byte, dataBlockSize)),
		zlibWriter: zw,
		xattrRef:   xattrRef,
	}, nil
}

// File creates a file with the specified name, modTime and mode. The returned
// io.WriterCloser must be closed after writing the file.
func (d *Directory) File(name string, modTime time.Time, mode uint16, xattrs []Xattr) (io.WriteCloser, error) {
	off, err := d.w.w.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	// zlib.BestSpeed results in only a 2x slow-down over no compression
	// (compared to >4x slow-down with DefaultCompression), but generates
	// results which are in the same ball park (10% larger).
	zw, err := zlib.NewWriterLevel(nil, zlib.BestSpeed)
	if err != nil {
		return nil, err
	}

	xattrRef := uint32(invalidXattr)
	if len(xattrs) > 0 {
		xattrRef = d.w.addXattrs(xattrs)
	}

	return &File{
		w:          d.w,
		d:          d,
		off:        off,
		name:       name,
		modTime:    modTime,
		mode:       mode,
		compBuf:    bytes.NewBuffer(make([]byte, dataBlockSize)),
		zlibWriter: zw,
		xattrRef:   xattrRef,
	}, nil
}

// Symlink creates a symbolic link from newname to oldname with the specified
// modTime and mode.
func (d *Directory) Symlink(oldname, newname string, modTime time.Time, mode os.FileMode) error {
	startBlock := d.w.inodeBuf.Len() / metadataBlockSize
	offset := d.w.inodeBuf.Len() - startBlock*metadataBlockSize

	if err := binary.Write(&d.w.inodeBuf, binary.LittleEndian, symlinkInodeHeader{
		inodeHeader: inodeHeader{
			InodeType:   symlinkType,
			Mode:        uint16(mode),
			UID:         0,
			GID:         0,
			Mtime:       int32(modTime.Unix()),
			InodeNumber: d.w.sb.Inodes + 1,
		},
		Nlink:       1, // TODO(later): when is this not 1?
		SymlinkSize: uint32(len(oldname)),
	}); err != nil {
		return err
	}

	if _, err := d.w.inodeBuf.Write([]byte(oldname)); err != nil {
		return err
	}

	d.dirEntries = append(d.dirEntries, fullDirEntry{
		startBlock:  uint32(startBlock),
		offset:      uint16(offset),
		inodeNumber: d.w.sb.Inodes + 1,
		entryType:   symlinkType,
		name:        newname,
	})

	d.w.sb.Inodes++
	return nil
}

func (d *Directory) dev(typ uint16, name string, maj, min int, modTime time.Time, mode os.FileMode) error {
	startBlock := d.w.inodeBuf.Len() / metadataBlockSize
	offset := d.w.inodeBuf.Len() - startBlock*metadataBlockSize

	if err := binary.Write(&d.w.inodeBuf, binary.LittleEndian, devInodeHeader{
		inodeHeader: inodeHeader{
			InodeType:   typ,
			Mode:        uint16(mode),
			UID:         0,
			GID:         0,
			Mtime:       int32(modTime.Unix()),
			InodeNumber: d.w.sb.Inodes + 1,
		},
		Nlink: 1,
		Rdev:  uint32((maj << 8) | (min & 0xff) | ((min & ^0xff) << 12)),
	}); err != nil {
		return err
	}

	d.dirEntries = append(d.dirEntries, fullDirEntry{
		startBlock:  uint32(startBlock),
		offset:      uint16(offset),
		inodeNumber: d.w.sb.Inodes + 1,
		entryType:   typ,
		name:        name,
	})

	d.w.sb.Inodes++
	return nil
}

func (d *Directory) BlockDevice(name string, maj, min int, modTime time.Time, mode os.FileMode) error {
	return d.dev(blkdevType, name, maj, min, modTime, mode)
}

func (d *Directory) CharDevice(name string, maj, min int, modTime time.Time, mode os.FileMode) error {
	return d.dev(chrdevType, name, maj, min, modTime, mode)
}

func (d *Directory) ipc(typ uint16, name string, modTime time.Time, mode os.FileMode) error {
	startBlock := d.w.inodeBuf.Len() / metadataBlockSize
	offset := d.w.inodeBuf.Len() - startBlock*metadataBlockSize

	if err := binary.Write(&d.w.inodeBuf, binary.LittleEndian, ipcInodeHeader{
		inodeHeader: inodeHeader{
			InodeType:   typ,
			Mode:        uint16(mode),
			UID:         0,
			GID:         0,
			Mtime:       int32(modTime.Unix()),
			InodeNumber: d.w.sb.Inodes + 1,
		},
		Nlink: 1, // TODO(later): when is this not 1?
	}); err != nil {
		return err
	}

	d.dirEntries = append(d.dirEntries, fullDirEntry{
		startBlock:  uint32(startBlock),
		offset:      uint16(offset),
		inodeNumber: d.w.sb.Inodes + 1,
		entryType:   typ,
		name:        name,
	})

	d.w.sb.Inodes++
	return nil
}

func (d *Directory) Socket(name string, modTime time.Time, mode os.FileMode) error {
	return d.ipc(socketType, name, modTime, mode)
}

func (d *Directory) Fifo(name string, modTime time.Time, mode os.FileMode) error {
	return d.ipc(fifoType, name, modTime, mode)
}

// Flush writes directory entries and creates inodes for the directory.
func (d *Directory) Flush() error {
	dirBufStartBlock := d.w.dirBuf.Len() / metadataBlockSize
	dirBufOffset := d.w.dirBuf.Len()

	currentBlock := int64(-1)
	currentInodeOffset := int64(-1)
	var subdirs int
	for i, de := range d.dirEntries {
		if de.entryType == dirType {
			subdirs++
		}

		if int64(de.startBlock) != currentBlock {
			var count uint32
			for j := i; j < len(d.dirEntries); j++ {
				if de.startBlock == d.dirEntries[j].startBlock {
					count++
				} else {
					break
				}
			}

			dh := dirHeader{
				Count:       count - 1,
				StartBlock:  de.startBlock * (metadataBlockSize + 2),
				InodeOffset: de.inodeNumber,
			}

			if err := binary.Write(&d.w.dirBuf, binary.LittleEndian, &dh); err != nil {
				return err
			}

			currentBlock = int64(de.startBlock)
			currentInodeOffset = int64(de.inodeNumber)
		}

		inodeDelta := int64(de.inodeNumber) - currentInodeOffset
		if inodeDelta > math.MaxInt16 || inodeDelta < math.MinInt16 {
			return fmt.Errorf("can't encode inode delta %d", inodeDelta)
		}

		if err := binary.Write(&d.w.dirBuf, binary.LittleEndian, &dirEntry{
			Offset:      de.offset,
			InodeNumber: int16(inodeDelta),
			EntryType:   de.entryType,
			Size:        uint16(len(de.name) - 1),
		}); err != nil {
			return err
		}
		if _, err := d.w.dirBuf.Write([]byte(de.name)); err != nil {
			return err
		}
	}

	startBlock := d.w.inodeBuf.Len() / metadataBlockSize
	offset := d.w.inodeBuf.Len() - startBlock*metadataBlockSize
	inodeBufOffset := d.w.inodeBuf.Len()

	// parentInodeOffset is the offset (in bytes) of the ParentInode field
	// within a dirInodeHeader or ldirInodeHeader
	var parentInodeOffset int64

	if len(d.dirEntries) > 256 || d.w.dirBuf.Len()-dirBufOffset > metadataBlockSize || d.xattrRef != invalidXattr {
		parentInodeOffset = (2 + 2 + 2 + 2 + 4 + 4) + 4 + 4 + 4
		if err := binary.Write(&d.w.inodeBuf, binary.LittleEndian, ldirInodeHeader{
			inodeHeader: inodeHeader{
				InodeType: ldirType,
				Mode: unix.S_IRUSR | unix.S_IWUSR | unix.S_IXUSR |
					unix.S_IRGRP | unix.S_IXGRP |
					unix.S_IROTH | unix.S_IXOTH,
				UID:         0,
				GID:         0,
				Mtime:       int32(d.modTime.Unix()),
				InodeNumber: d.w.sb.Inodes + 1,
			},

			Nlink:       uint32(subdirs + 2 - 1), // + 2 for . and ..
			FileSize:    uint32(d.w.dirBuf.Len()-dirBufOffset) + 3,
			StartBlock:  uint32(dirBufStartBlock * (metadataBlockSize + 2)),
			ParentInode: d.w.sb.Inodes + 2, // invalid
			Icount:      0,                 // no directory index
			Offset:      uint16(dirBufOffset - dirBufStartBlock*metadataBlockSize),
			Xattr:       d.xattrRef,
		}); err != nil {
			return err
		}
	} else {
		parentInodeOffset = (2 + 2 + 2 + 2 + 4 + 4) + 4 + 4 + 2 + 2
		if err := binary.Write(&d.w.inodeBuf, binary.LittleEndian, dirInodeHeader{
			inodeHeader: inodeHeader{
				InodeType: dirType,
				Mode: unix.S_IRUSR | unix.S_IWUSR | unix.S_IXUSR |
					unix.S_IRGRP | unix.S_IXGRP |
					unix.S_IROTH | unix.S_IXOTH,
				UID:         0,
				GID:         0,
				Mtime:       int32(d.modTime.Unix()),
				InodeNumber: d.w.sb.Inodes + 1,
			},

			StartBlock:  uint32(dirBufStartBlock * (metadataBlockSize + 2)),
			Nlink:       uint32(subdirs + 2 - 1), // + 2 for . and ..
			FileSize:    uint16(d.w.dirBuf.Len()-dirBufOffset) + 3,
			Offset:      uint16(dirBufOffset - dirBufStartBlock*metadataBlockSize),
			ParentInode: d.w.sb.Inodes + 2, // invalid
		}); err != nil {
			return err
		}
	}

	path := d.path()
	for _, offset := range d.w.writeInodeNumTo[path] {
		// Directly manipulating unread data in bytes.Buffer via Bytes(), as per
		// https://groups.google.com/d/msg/golang-nuts/1ON9XVQ1jXE/8j9RaeSYxuEJ
		b := d.w.inodeBuf.Bytes()
		binary.LittleEndian.PutUint32(b[offset:offset+4], d.w.sb.Inodes+1)
	}

	if d.parent != nil {
		parentPath := filepath.Dir(d.path())
		if parentPath == "." {
			parentPath = ""
		}
		d.w.writeInodeNumTo[parentPath] = append(d.w.writeInodeNumTo[parentPath], int64(inodeBufOffset)+parentInodeOffset)
		d.parent.dirEntries = append(d.parent.dirEntries, fullDirEntry{
			startBlock:  uint32(startBlock),
			offset:      uint16(offset),
			inodeNumber: d.w.sb.Inodes + 1,
			entryType:   dirType,
			name:        d.name,
		})
	} else { // root
		d.w.sb.RootInode = Inode((startBlock*(metadataBlockSize+2))<<16 | offset)
	}

	d.w.sb.Inodes++

	return nil
}

// Write implements io.Writer
func (f *File) Write(p []byte) (n int, err error) {
	n, err = f.buf.Write(p)
	if n > 0 {
		// Keep track of the uncompressed file size.
		f.size += uint32(n)
		for f.buf.Len() >= dataBlockSize {
			if err := f.writeBlock(); err != nil {
				return 0, err
			}
		}
	}
	return n, err
}

func (f *File) writeBlock() error {
	n := f.buf.Len()
	if n > dataBlockSize {
		n = dataBlockSize
	}
	// Feed dataBlockSize bytes to the compressor
	b := f.buf.Bytes()
	block := b[:n]
	rest := b[n:]
	/*
		f.compBuf.Reset()
		f.zlibWriter.Reset(f.compBuf)
		if _, err := f.zlibWriter.Write(block); err != nil {
			return err
		}
		if err := f.zlibWriter.Close(); err != nil {
			return err
		}

		size := f.compBuf.Len()
		if size > len(block) {
			// Copy uncompressed data: Linux returns i/o errors when it encounters a
			// compressed block which is larger than the uncompressed data:
			// https://github.com/torvalds/linux/blob/3ca24ce9ff764bc27bceb9b2fd8ece74846c3fd3/fs/squashfs/block.c#L150
			size = len(block) | (1 << 24) // SQUASHFS_COMPRESSED_BIT_BLOCK
			if _, err := f.w.w.Write(block); err != nil {
				return err
			}
		} else {
			if _, err := io.Copy(f.w.w, f.compBuf); err != nil {
				return err
			}
		}
	*/
	// Copy uncompressed data: Linux returns i/o errors when it encounters a
	// compressed block which is larger than the uncompressed data:
	// https://github.com/torvalds/linux/blob/3ca24ce9ff764bc27bceb9b2fd8ece74846c3fd3/fs/squashfs/block.c#L150
	size := len(block) | (1 << 24) // SQUASHFS_COMPRESSED_BIT_BLOCK
	if _, err := f.w.w.Write(block); err != nil {
		return err
	}

	f.blocksizes = append(f.blocksizes, uint32(size))

	// Keep the rest in f.buf for the next write
	copy(b, rest)
	f.buf.Truncate(len(rest))
	return nil
}

// Close implements io.Closer
func (f *File) Close() error {
	for f.buf.Len() > 0 {
		if err := f.writeBlock(); err != nil {
			return err
		}
	}

	f.inode = f.w.sb.Inodes + 1
	f.w.sb.Inodes++

	f.inodeStartBlock = uint32(f.w.inodeBuf.Len() / metadataBlockSize)
	f.inodeOffset = uint16(uint32(f.w.inodeBuf.Len()) - f.inodeStartBlock*metadataBlockSize)

	if err := binary.Write(&f.w.inodeBuf, binary.LittleEndian, lregInodeHeader{
		inodeHeader: inodeHeader{
			InodeType:   lregType,
			Mode:        f.mode,
			UID:         0,
			GID:         0,
			Mtime:       int32(f.modTime.Unix()),
			InodeNumber: f.inode,
		},
		StartBlock: uint64(f.off),
		FileSize:   uint64(f.size),
		Nlink:      1,
		Fragment:   invalidFragment,
		Offset:     0,
		Xattr:      f.xattrRef,
	}); err != nil {
		return err
	}

	if err := binary.Write(&f.w.inodeBuf, binary.LittleEndian, f.blocksizes); err != nil {
		return err
	}

	if f.d != nil {
		f.d.AttachFile(f, f.name)
	}

	return nil
}

func (d *Directory) AttachFile(f *File, name string) {
	d.dirEntries = append(d.dirEntries, fullDirEntry{
		startBlock:  f.inodeStartBlock,
		offset:      f.inodeOffset,
		inodeNumber: f.inode,
		entryType:   fileType,
		name:        name,
	})
}

func writeXattr(w io.Writer, xattrs []Xattr) error {
	for _, attr := range xattrs {
		if err := binary.Write(w, binary.LittleEndian, struct {
			Type     uint16
			NameSize uint16
		}{
			Type:     attr.Type,
			NameSize: uint16(len(attr.FullName)),
		}); err != nil {
			return err
		}
		if _, err := w.Write([]byte(attr.FullName)); err != nil {
			return err
		}

		if err := binary.Write(w, binary.LittleEndian, struct {
			ValSize uint32
		}{
			ValSize: uint32(len(attr.Value)),
		}); err != nil {
			return err
		}

		if _, err := w.Write(attr.Value); err != nil {
			return err
		}
	}
	return nil
}

type xattrTableHeader struct {
	XattrTableStart uint64
	XattrIds        uint32
	Unused          uint32
}

func (w *Writer) writeXattrTables() (int64, error) {
	if len(w.xattrs) == 0 {
		return -1, nil
	}
	off, err := w.w.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	xattrTableStart := uint64(off)

	var xattrBuf bytes.Buffer
	if err := writeXattr(&xattrBuf, w.xattrs); err != nil {
		return 0, err
	}
	xattrBlocks := (xattrBuf.Len() + (metadataBlockSize - 1)) / metadataBlockSize

	if err := w.writeMetadataChunks(&xattrBuf); err != nil {
		return 0, err
	}

	// write xattr id table
	off, err = w.w.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	idTableOff := uint64(off)
	var xattrIDBuf bytes.Buffer
	size := uint64(0)
	for _, id := range w.xattrIDs {
		id.Xattr = uint64(size)
		size += uint64(id.Size) + 8 /* sizeof(Type+NameSize+ValSize) */
		if err := binary.Write(&xattrIDBuf, binary.LittleEndian, id); err != nil {
			return 0, err
		}
	}
	if err := w.writeMetadataChunks(&xattrIDBuf); err != nil {
		return 0, err
	}

	// xattr table header
	off, err = w.w.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if err := binary.Write(w.w, binary.LittleEndian, xattrTableHeader{
		XattrTableStart: xattrTableStart,
		XattrIds:        uint32(len(w.xattrs)),
	}); err != nil {
		return 0, err
	}
	// write block index
	for i := 0; i < xattrBlocks; i++ {
		if err := binary.Write(w.w, binary.LittleEndian, struct {
			BlockOffset uint64
		}{
			BlockOffset: idTableOff + (uint64(i) * (8192 + 2 /* sizeof(uint16) */)),
		}); err != nil {
			return 0, err
		}
	}
	return off, nil
}

// writeMetadataChunks copies from r to w in blocks of metadataBlockSize bytes
// each, prefixing each block with a uint16 length header, setting the
// uncompressed bit.
func (w *Writer) writeMetadataChunks(r io.Reader) error {
	buf := make([]byte, metadataBlockSize)
	for {
		buf = buf[:metadataBlockSize]
		n, err := r.Read(buf)
		if err != nil {
			if err == io.EOF { // done
				return nil
			}
			return err
		}
		buf = buf[:n]
		if err := binary.Write(w.w, binary.LittleEndian, uint16(len(buf))|0x8000); err != nil {
			return err
		}
		if _, err := w.w.Write(buf); err != nil {
			return err
		}
	}
}

// Flush writes the SquashFS file system. The Writer must not be used after
// calling Flush.
func (w *Writer) Flush() error {
	// (1) superblock will be written later

	// (2) compressor-specific options omitted

	// (3) data has already been written

	// (4) write inode table
	off, err := w.w.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	w.sb.InodeTableStart = off

	if err := w.writeMetadataChunks(&w.inodeBuf); err != nil {
		return err
	}

	// (5) write directory table
	off, err = w.w.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	w.sb.DirectoryTableStart = off

	if err := w.writeMetadataChunks(&w.dirBuf); err != nil {
		return err
	}

	// (6) fragment table omitted
	off, err = w.w.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	w.sb.FragmentTableStart = off

	// (7) export table omitted

	// (8) write uid/gid lookup table
	idTableStart, err := writeIDTable(w.w, []uint32{0})
	if err != nil {
		return err
	}
	w.sb.IDTableStart = idTableStart

	// (9) xattr table
	off, err = w.writeXattrTables()
	if err != nil {
		return err
	}
	w.sb.XattrIDTableStart = off

	off, err = w.w.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	w.sb.BytesUsed = off

	// Pad to 4096, required for the kernel to be able to access all pages
	if pad := off % 4096; pad > 0 {
		padding := make([]byte, 4096-pad)
		if _, err := w.w.Write(padding); err != nil {
			return err
		}
	}

	// (1) Write superblock
	if _, err := w.w.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return binary.Write(w.w, binary.LittleEndian, &w.sb)
}
