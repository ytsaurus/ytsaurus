package squashfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/xerrors"
)

type Reader struct {
	r     io.ReaderAt
	super superblock
}

func NewReader(r io.ReaderAt) (*Reader, error) {
	var sb superblock

	if err := binary.Read(io.NewSectionReader(r, 0, int64(binary.Size(sb))), binary.LittleEndian, &sb); err != nil {
		return nil, fmt.Errorf("reading superblock: %v", err)
	}

	if got, want := sb.Magic, uint32(magic); got != want {
		return nil, fmt.Errorf("invalid magic (not a SquashFS image?): got %x, want %x", got, want)
	}

	return &Reader{
		r:     r,
		super: sb,
	}, nil
}

// TODO: maybe mmap instead of seeking?

func (r *Reader) inode(i Inode) (blockoffset int64, offset int64) {
	return int64(i >> 16), int64(i & 0xFFFF)
}

type blockReader struct {
	r      io.ReadSeeker
	lenBuf [2]byte
	buf    []byte
	i      int64

	off int64 // TODO: remove this once using mmap
}

func (br *blockReader) Read(p []byte) (n int, err error) {
	if br.i >= int64(len(br.buf)) {
		br.i = 0
		if _, err := io.ReadFull(br.r, br.lenBuf[:]); err != nil {
			return 0, err
		}
		l := binary.LittleEndian.Uint16(br.lenBuf[:])
		// uncompressed := l&0x8000 > 0
		l &= 0x7FFF
		// log.Printf("block of len %d, uncompressed: %v", l, uncompressed)
		if int(l) > cap(br.buf) {
			br.buf = make([]byte, int(l))
		}
		br.buf = br.buf[:l]
		if _, err := io.ReadFull(br.r, br.buf); err != nil {
			return 0, err
		}
		//log.Printf("(retry) n = %v, err = %v", n, err)
	}
	n = copy(p, br.buf[br.i:])
	br.i += int64(n)
	return n, err
}

func (br *blockReader) Close() error {
	blockReaderPool.Put(br)
	return nil
}

var blockReaderPool = sync.Pool{
	New: func() interface{} {
		return &blockReader{
			buf: make([]byte, 0, metadataBlockSize),
		}
	},
}

func (r *Reader) blockReader(blockoffset, offset int64) (io.ReadCloser, error) {
	// log.Printf("blockoffset %v (%x), offset %v (%x)", blockoffset, blockoffset, offset, offset)
	br := blockReaderPool.Get().(*blockReader)
	br.buf = br.buf[:0]
	br.r = io.NewSectionReader(r.r, blockoffset, 5500*1024*1024) // TODO: correct limit? can we use IntMax
	br.off = blockoffset
	br.i = 0
	//log.Printf("discarding %d bytes", offset)
	for n := int64(0); n < offset; {
		remaining := offset - n
		if remaining > metadataBlockSize {
			remaining = metadataBlockSize
		}
		nn, err := br.Read(br.buf[:remaining])
		if err != nil {
			return nil, err
		}
		n += int64(nn)
	}
	return br, nil
}

// TODO: define an inode type to use instead of interface{}?
func (r *Reader) readInode(i Inode) (interface{}, error) {
	blockoffset, offset := r.inode(i)
	br, err := r.blockReader(r.super.InodeTableStart+blockoffset, offset)
	if err != nil {
		return nil, err
	}
	defer br.Close()

	// We need the inode type before we know which type to pass to binary.Read,
	// so we need to read it twice:
	var inodeType uint16
	typeBuf := bytes.NewBuffer(make([]byte, 0, binary.Size(inodeType)))
	if err := binary.Read(io.TeeReader(br, typeBuf), binary.LittleEndian, &inodeType); err != nil {
		return nil, err
	}
	br = ioutil.NopCloser(io.MultiReader(typeBuf, br))

	// var ih inodeHeader
	// if err := binary.Read(br, binary.LittleEndian, &ih); err != nil {
	// 	return err
	// }
	// //log.Printf("ih: %+v", ih)

	//log.Printf("inode type: %v", inodeType)
	switch inodeType {
	case dirType:
		var di dirInodeHeader
		if err := binary.Read(br, binary.LittleEndian, &di); err != nil {
			return nil, err
		}
		return di, nil

	case fileType:
		var ri regInodeHeader
		if err := binary.Read(br, binary.LittleEndian, &ri); err != nil {
			return nil, err
		}
		return ri, nil

	case symlinkType:
		var si symlinkInodeHeader
		if err := binary.Read(br, binary.LittleEndian, &si); err != nil {
			return nil, err
		}
		return si, nil

	case ldirType:
		var di ldirInodeHeader
		if err := binary.Read(br, binary.LittleEndian, &di); err != nil {
			return nil, err
		}
		return di, nil

	case lregType:
		var di lregInodeHeader
		if err := binary.Read(br, binary.LittleEndian, &di); err != nil {
			return nil, err
		}
		return di, nil

		// TODO:
		// blkdevType
		// chrdevType
		// fifoType
		// socketType
		// // The larger types are used for e.g. sparse files, xattrs, etc.
		// ldirType
		// lsymlinkType
		// lblkdevType
		// lchrdevType
		// lfifoType
		// lsocketType

	}
	return nil, fmt.Errorf("unknown inode type %d", inodeType)
}

func (r *Reader) RootInode() Inode {
	return r.super.RootInode
}

func (r *Reader) Stat(name string, i Inode) (os.FileInfo, error) {
	inode, err := r.readInode(i)
	if err != nil {
		return nil, err
	}
	//log.Printf("i %d, inode: %T, %+v", i, inode, inode)
	switch x := inode.(type) {
	case dirInodeHeader:
		return &FileInfo{
			name:    name,
			size:    int64(x.FileSize),
			mode:    os.ModeDir | os.FileMode(x.Mode),
			modTime: time.Unix(int64(x.Mtime), 0),
			Inode:   i,
		}, nil

	case ldirInodeHeader:
		return &FileInfo{
			name:    name,
			size:    int64(x.FileSize),
			mode:    os.ModeDir | os.FileMode(x.Mode),
			modTime: time.Unix(int64(x.Mtime), 0),
			Inode:   i,
		}, nil

	case regInodeHeader:
		mode := os.FileMode(x.Mode & 0777)
		if x.Mode&syscall.S_ISUID != 0 {
			mode |= os.ModeSetuid
		}
		return &FileInfo{
			name:    name,
			size:    int64(x.FileSize),
			mode:    mode,
			modTime: time.Unix(int64(x.Mtime), 0),
			Inode:   i,
		}, nil

	case lregInodeHeader:
		mode := os.FileMode(x.Mode & 0777)
		if x.Mode&syscall.S_ISUID != 0 {
			mode |= os.ModeSetuid
		}
		return &FileInfo{
			name:    name,
			size:    int64(x.FileSize),
			mode:    mode,
			modTime: time.Unix(int64(x.Mtime), 0),
			Inode:   i,
		}, nil

	case symlinkInodeHeader:
		return &FileInfo{
			name:    name,
			size:    int64(x.SymlinkSize),
			mode:    os.ModeSymlink | os.FileMode(x.Mode),
			modTime: time.Unix(int64(x.Mtime), 0),
			Inode:   i,
		}, nil
	}

	return nil, fmt.Errorf("unknown inode type %T", inode)
}

func (r *Reader) ReadLink(i Inode) (string, error) {
	// TODO: reduce code duplication with readInode
	blockoffset, offset := r.inode(i)
	br, err := r.blockReader(r.super.InodeTableStart+blockoffset, offset)
	if err != nil {
		return "", err
	}
	defer br.Close()

	// We need the inode type before we know which type to pass to binary.Read,
	// so we need to read it twice:
	var inodeType uint16
	typeBuf := bytes.NewBuffer(make([]byte, 0, binary.Size(inodeType)))
	if err := binary.Read(io.TeeReader(br, typeBuf), binary.LittleEndian, &inodeType); err != nil {
		return "", err
	}
	br = ioutil.NopCloser(io.MultiReader(typeBuf, br))

	if inodeType != symlinkType {
		return "", fmt.Errorf("invalid inode type: got %d instead of symlink", inodeType)
	}
	var si symlinkInodeHeader
	if err := binary.Read(br, binary.LittleEndian, &si); err != nil {
		return "", err
	}

	// Assumption: r.r is positioned right after the inode
	buf := make([]byte, si.SymlinkSize)
	if _, err := io.ReadFull(br, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func (r *Reader) FileReader(inode Inode) (*io.SectionReader, error) {
	//log.Printf("Readfile(%v)", inode)
	i, err := r.readInode(inode)
	if err != nil {
		return nil, err
	}
	//log.Printf("i: %+v", i)
	// TODO(compression): read the blocksizes to read compressed blocks
	switch ri := i.(type) {
	case regInodeHeader:
		off := int64(ri.StartBlock) + int64(ri.Offset)
		return io.NewSectionReader(r.r, off, int64(ri.FileSize)), nil
	case lregInodeHeader:
		off := int64(ri.StartBlock) + int64(ri.Offset)
		return io.NewSectionReader(r.r, off, int64(ri.FileSize)), nil
	default:
		return nil, fmt.Errorf("BUG: non-file inode type")
	}
}

type FileNotFoundError struct {
	path string
}

func (e *FileNotFoundError) Error() string {
	return fmt.Sprintf("%q not found", e.path)
}

func (r *Reader) lookupComponent(parent Inode, component string) (Inode, error) {
	rfis, err := r.readdir(parent, false)
	if err != nil {
		return 0, err
	}
	for _, rfi := range rfis {
		if rfi.Name() == component {
			return rfi.Sys().(*FileInfo).Inode, nil
		}
	}
	return 0, &FileNotFoundError{path: component}
}

func (r *Reader) lookupPath(path string, followSymlink bool) (Inode, error) {
	inode := r.RootInode()
	parts := strings.Split(path, "/")
	for idx, part := range parts {
		var err error
		inode, err = r.lookupComponent(inode, part)
		if err != nil {
			if _, ok := err.(*FileNotFoundError); ok {
				return 0, &FileNotFoundError{path: path}
			}
			return 0, err
		}
		if !followSymlink {
			continue
		}
		i, err := r.readInode(inode)
		if err != nil {
			return 0, xerrors.Errorf("Stat(%d): %v", inode, err)
		}
		if _, ok := i.(symlinkInodeHeader); ok {
			target, err := r.ReadLink(inode)
			if err != nil {
				return 0, err
			}
			//log.Printf("component %q (full: %q) resolved to %q", part, parts[:idx+1], target)
			target = filepath.Clean(filepath.Join(append(parts[:idx] /* parent */, target)...))
			//log.Printf("-> %s", target)
			i, err := r.LookupPath(target)
			if err != nil {
				return 0, err
			}
			inode = i
		}
	}
	return inode, nil
}

func (r *Reader) LookupPath(path string) (Inode, error) {
	return r.lookupPath(path, true)
}

// LlookupPath is like LookupPath, but does not follow symbolic links, i.e. will
// instead return the inode of the link itself.
func (r *Reader) LlookupPath(path string) (Inode, error) {
	return r.lookupPath(path, false)
}

func (r *Reader) Readdir(dirInode Inode) ([]os.FileInfo, error) {
	return r.readdir(dirInode, true)
}

// Like Readdir, but does not call Stat on each file. The returned FileInfo
// structs will still have a filled in Name, partly filled in Mode, and filled
// in Inode.
func (r *Reader) ReaddirNoStat(dirInode Inode) ([]os.FileInfo, error) {
	return r.readdir(dirInode, false)
}

var nameBufPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func (r *Reader) readdir(dirInode Inode, stat bool) ([]os.FileInfo, error) {
	//log.Printf("Readdir(%v (%x))", dirInode, dirInode)
	i, err := r.readInode(dirInode)
	if err != nil {
		return nil, err
	}
	var (
		startBlock int64
		fileSize   int64
		offset     int64
	)
	switch x := i.(type) {
	case dirInodeHeader:
		startBlock = int64(x.StartBlock)
		fileSize = int64(x.FileSize)
		offset = int64(x.Offset)

	case ldirInodeHeader:
		startBlock = int64(x.StartBlock)
		fileSize = int64(x.FileSize)
		offset = int64(x.Offset)

	default:
		return nil, fmt.Errorf("unknown directory inode type %T", i)
	}

	br, err := r.blockReader(r.super.DirectoryTableStart+startBlock, offset)
	if err != nil {
		return nil, err
	}
	defer br.Close()

	// See also https://elixir.bootlin.com/linux/v4.18.9/source/fs/squashfs/dir.c#L63
	limit := fileSize - int64(len(".")) - int64(len(".."))
	br = ioutil.NopCloser(io.LimitReader(br, limit))

	var fis []os.FileInfo
	var dh dirHeader
	var de dirEntry
	var dhBuf [12]byte
	var deBuf [8]byte
	nameBuf := nameBufPool.Get().(*bytes.Buffer)
	defer nameBufPool.Put(nameBuf)
	for {
		if _, err := io.ReadFull(br, dhBuf[:]); err != nil {
			if err == io.EOF {
				return fis, nil
			}
			return nil, fmt.Errorf("failed to read dir header: %v", err)
		}
		dh.Unmarshal(dhBuf[:])
		dh.Count++ // SquashFS stores count-1
		// log.Printf("dh: %+v", dh)

		for i := 0; i < int(dh.Count); i++ {
			if _, err := io.ReadFull(br, deBuf[:]); err != nil {
				return nil, fmt.Errorf("failed to read dir entry: %v", err)
			}
			de.Unmarshal(deBuf[:])
			de.Size++ // SquashFS stores size-1
			// log.Printf("de: %+v", de)
			nameBuf.Reset()
			nameBuf.Grow(int(de.Size))
			nb := nameBuf.Bytes()[:de.Size]
			if _, err := io.ReadFull(br, nb); err != nil {
				return nil, fmt.Errorf("failed to read name: %v", err)
			}
			name := string(nb)
			// log.Printf("name: %q", string(name))

			var fi os.FileInfo
			if stat {
				var err error
				fi, err = r.Stat(name, Inode(int64(dh.StartBlock)<<16|int64(de.Offset)))
				if err != nil {
					return nil, err
				}
			} else {
				ffi := &FileInfo{
					name:  name,
					Inode: Inode(int64(dh.StartBlock)<<16 | int64(de.Offset)),
				}
				switch de.EntryType {
				case dirType, ldirType:
					ffi.mode |= os.ModeDir
				case symlinkType, lsymlinkType:
					ffi.mode |= os.ModeSymlink
				}
				fi = ffi
			}
			fis = append(fis, fi)
		}
	}
}

type FileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	Inode   Inode
}

func (fi *FileInfo) Name() string       { return fi.name }
func (fi *FileInfo) Size() int64        { return fi.size }
func (fi *FileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *FileInfo) IsDir() bool        { return fi.mode.IsDir() }
func (fi *FileInfo) ModTime() time.Time { return fi.modTime }
func (fi *FileInfo) Sys() interface{}   { return fi }

func (r *Reader) readXattr(tableHeader xattrTableHeader, id xattrID) (*Xattr, error) {
	blockoffset, offset := r.inode(Inode(id.Xattr))
	br, err := r.blockReader(int64(tableHeader.XattrTableStart)+blockoffset, offset)
	if err != nil {
		return nil, err
	}
	defer br.Close()
	var typ, nameSize uint16
	if err := binary.Read(br, binary.LittleEndian, &typ); err != nil {
		return nil, err
	}
	if err := binary.Read(br, binary.LittleEndian, &nameSize); err != nil {
		return nil, err
	}
	// log.Printf("type = %v, nameSize = %v", typ, nameSize)
	name := make([]byte, nameSize)
	if _, err := io.ReadFull(br, name); err != nil {
		return nil, err
	}
	// log.Printf("name = %v", string(name))
	var valSize uint32
	if err := binary.Read(br, binary.LittleEndian, &valSize); err != nil {
		return nil, err
	}
	val := make([]byte, valSize)
	if _, err := io.ReadFull(br, val); err != nil {
		return nil, err
	}
	// log.Printf("val = %x", val)
	return &Xattr{
		Type:     typ,
		FullName: xattrPrefix[int(typ)] + string(name),
		Value:    val,
	}, nil
}

func (r *Reader) ReadXattrs(inode Inode) ([]Xattr, error) {
	//log.Printf("Readdir(%v (%x))", dirInode, dirInode)
	i, err := r.readInode(inode)
	if err != nil {
		return nil, err
	}
	var xid uint32
	switch x := i.(type) {
	case regInodeHeader,
		dirInodeHeader,
		ldirInodeHeader,
		symlinkInodeHeader:
		return nil, nil // no extended attributes

	case lregInodeHeader:
		if x.Xattr == invalidXattr {
			return nil, nil // file has no extended attributes
		}
		xid = x.Xattr

	default:
		return nil, fmt.Errorf("unknown inode type %T", i)
	}

	const idEntriesPerBlock = 512 // = 8192 / 16 /* sizeof(xattrId) */
	block := xid / idEntriesPerBlock
	offset := (xid % idEntriesPerBlock) * 16
	// log.Printf("xattr id %d, block %d, offset %d", xid, block, offset)
	// log.Printf("r.super.XattrIdTableStart = 0x%x, r.super.XattrIdTableStart = %v", r.super.XattrIDTableStart, r.super.XattrIDTableStart)
	br := ioutil.NopCloser(io.NewSectionReader(r.r, r.super.XattrIDTableStart, int64(16 /* sizeof(xattrTableHeader) */ +(block+1)*4 /* sizeof(uint32) */)))
	var tableHeader xattrTableHeader
	if err := binary.Read(br, binary.LittleEndian, &tableHeader); err != nil {
		return nil, err
	}
	// index starts here
	if _, err := io.CopyN(ioutil.Discard, br, int64(block*4 /* sizeof(uint32) */)); err != nil {
		return nil, err
	}
	var blockOffset uint32
	if err := binary.Read(br, binary.LittleEndian, &blockOffset); err != nil {
		return nil, err
	}
	// log.Printf("blockOffset = 0x%x (%d)", blockOffset, blockOffset)
	br, err = r.blockReader(int64(blockOffset), int64(offset))
	if err != nil {
		return nil, err
	}
	defer br.Close()
	var id xattrID
	if err := binary.Read(br, binary.LittleEndian, &id); err != nil {
		return nil, err
	}
	// log.Printf("id: %+v", id)
	// log.Printf("tableHeader: %+v (start 0x%x)", tableHeader, tableHeader.XattrTableStart)

	var xattrs []Xattr
	for i := 0; i < int(id.Count); i++ {
		xattr, err := r.readXattr(tableHeader, id)
		if err != nil {
			return nil, err
		}
		xattrs = append(xattrs, *xattr)
	}

	return xattrs, nil
}
