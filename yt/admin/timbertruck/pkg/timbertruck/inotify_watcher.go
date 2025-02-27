package timbertruck

// This file is based on the fsnotify package: github.com/fsnotify/fsnotify/blob/v1.7.0/backend_inotify.go

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Common errors that can be reported.
var (
	ErrNonExistentWatch = errors.New("inotify watcher: can't remove non-existent watch")
	ErrEventOverflow    = errors.New("inotify watcher: queue or buffer overflow")
	ErrClosed           = errors.New("inotify watcher: watcher already closed")
)

type InotifyWatcher struct {
	// Events sends the filesystem change events.
	Events chan FsEvent

	// Errors sends any errors.
	//
	// ErrEventOverflow is used to indicate there are too many queued events (fs.inotify.max_queued_events sysctl).
	Errors chan error

	// Store fd here as os.File.Read() will no longer return on close after
	// calling Fd(). See: https://github.com/golang/go/issues/26439
	fd          int
	inotifyFile *os.File
	watches     *watches
	done        chan struct{} // Channel for sending a "quit message" to the reader goroutine
	closeMu     sync.Mutex
	doneResp    chan struct{} // Channel to respond to Close
}

// FsEvent represents a file system notification.
type FsEvent struct {
	// Path to the file or directory.
	Name string

	// File operation that triggered the event.
	Op FileOp
}

func (e FsEvent) Has(op FileOp) bool { return e.Op.Has(op) }

func (e FsEvent) String() string {
	return fmt.Sprintf("%-13s %q", e.Op.String(), e.Name)
}

// FileOp describes a set of file operations.
type FileOp uint32

const (
	// A new pathname was created.
	FileOpCreate FileOp = 1 << iota

	// The path was removed; any watches on it will be removed.
	FileOpRemove

	// The path was renamed to something else; any watched on it will be
	// removed.
	FileOpRename
)

func (o FileOp) Flags() uint32 {
	var flags uint32
	if o.Has(FileOpCreate) {
		flags |= unix.IN_CREATE | unix.IN_MOVED_TO
	}
	if o.Has(FileOpRemove) {
		flags |= unix.IN_DELETE_SELF | unix.IN_DELETE
	}
	if o.Has(FileOpRename) {
		flags |= unix.IN_MOVE_SELF | unix.IN_MOVED_FROM
	}
	return flags
}

func (o FileOp) String() string {
	var b strings.Builder
	if o.Has(FileOpCreate) {
		b.WriteString("|CREATE")
	}
	if o.Has(FileOpRemove) {
		b.WriteString("|REMOVE")
	}
	if o.Has(FileOpRename) {
		b.WriteString("|RENAME")
	}
	if b.Len() == 0 {
		return "[no events]"
	}
	return b.String()[1:]
}

func (o FileOp) Has(h FileOp) bool { return o&h != 0 }

type (
	addOpt   func(opt *withOpts)
	withOpts struct {
		fileOp FileOp
	}
)

var defaultOpts = withOpts{
	fileOp: FileOpCreate | FileOpRemove | FileOpRename,
}

func getOptions(opts ...addOpt) withOpts {
	with := defaultOpts
	for _, o := range opts {
		o(&with)
	}
	return with
}

func WithFileOps(op FileOp) addOpt {
	return func(opt *withOpts) { opt.fileOp = op }
}

// NewInotifyWatcher creates a new InotifyWatcher.
func NewInotifyWatcher() (*InotifyWatcher, error) {
	// Need to set nonblocking mode for SetDeadline to work, otherwise blocking
	// I/O operations won't terminate on close.
	fd, errno := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
	if fd == -1 {
		return nil, errno
	}

	w := &InotifyWatcher{
		fd:          fd,
		inotifyFile: os.NewFile(uintptr(fd), ""),
		watches:     newWatches(),
		Events:      make(chan FsEvent),
		Errors:      make(chan error),
		done:        make(chan struct{}),
		doneResp:    make(chan struct{}),
	}

	go w.readEvents()
	return w, nil
}

// Add starts monitoring the path for changes.
func (w *InotifyWatcher) Add(name string) error { return w.AddWith(name) }

// AddWith is like [InotifyWatcher.Add], but allows adding options. When using Add()
// the defaults described below are used.
//
// Possible options are:
//
//   - [WithFileOps] sets which operations to listen for. The default is [FileOpCreate], [FileOpRemove], [FileOpRename].
func (w *InotifyWatcher) AddWith(name string, opts ...addOpt) error {
	if w.isClosed() {
		return ErrClosed
	}

	name = filepath.Clean(name)
	options := getOptions(opts...)

	var flags uint32 = options.fileOp.Flags()

	return w.watches.updatePath(name, func(existing *watch) (*watch, error) {
		if existing != nil {
			flags |= existing.flags | unix.IN_MASK_ADD
		}

		wd, err := unix.InotifyAddWatch(w.fd, name, flags)
		if wd == -1 {
			return nil, err
		}

		if existing == nil {
			return &watch{
				wd:    uint32(wd),
				path:  name,
				flags: flags,
			}, nil
		}

		existing.wd = uint32(wd)
		existing.flags = flags
		return existing, nil
	})
}

// Close removes all watches and closes the Events channel.
func (w *InotifyWatcher) Close() error {
	w.closeMu.Lock()
	if w.isClosed() {
		w.closeMu.Unlock()
		return nil
	}
	close(w.done)
	w.closeMu.Unlock()

	// Causes any blocking reads to return with an error, provided the file
	// still supports deadline operations.
	err := w.inotifyFile.Close()
	if err != nil {
		return err
	}

	// Wait for goroutine to close
	<-w.doneResp

	return nil
}

// Returns true if the event was sent, or false if watcher is closed.
func (w *InotifyWatcher) sendEvent(e FsEvent) bool {
	select {
	case w.Events <- e:
		return true
	case <-w.done:
		return false
	}
}

// Returns true if the error was sent, or false if watcher is closed.
func (w *InotifyWatcher) sendError(err error) bool {
	select {
	case w.Errors <- err:
		return true
	case <-w.done:
		return false
	}
}

func (w *InotifyWatcher) isClosed() bool {
	select {
	case <-w.done:
		return true
	default:
		return false
	}
}

func (w *InotifyWatcher) remove(name string) error {
	wd, ok := w.watches.removePath(name)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNonExistentWatch, name)
	}

	success, errno := unix.InotifyRmWatch(w.fd, wd)
	if success == -1 {
		return errno
	}
	return nil
}

// readEvents reads from the inotify file descriptor, converts the
// received events into Event objects and sends them via the Events channel
func (w *InotifyWatcher) readEvents() {
	defer func() {
		close(w.doneResp)
		close(w.Errors)
		close(w.Events)
	}()

	var (
		buf   [unix.SizeofInotifyEvent * 4096]byte // Buffer for a maximum of 4096 raw events
		errno error                                // Syscall errno
	)
	for {
		// See if we have been closed.
		if w.isClosed() {
			return
		}

		n, err := w.inotifyFile.Read(buf[:])
		switch {
		case errors.Unwrap(err) == os.ErrClosed:
			return
		case err != nil:
			if !w.sendError(err) {
				return
			}
			continue
		}

		if n < unix.SizeofInotifyEvent {
			var err error
			if n == 0 {
				err = io.EOF // If EOF is received. This should really never happen.
			} else if n < 0 {
				err = errno // If an error occurred while reading.
			} else {
				err = errors.New("notify: short read in readEvents()") // Read was too short.
			}
			if !w.sendError(err) {
				return
			}
			continue
		}

		var offset uint32
		// We don't know how many events we just read into the buffer
		// While the offset points to at least one whole event...
		for offset <= uint32(n-unix.SizeofInotifyEvent) {
			var (
				// Point "raw" to the event in the buffer
				raw     = (*unix.InotifyEvent)(unsafe.Pointer(&buf[offset]))
				mask    = uint32(raw.Mask)
				nameLen = uint32(raw.Len)
			)

			if mask&unix.IN_Q_OVERFLOW != 0 {
				if !w.sendError(ErrEventOverflow) {
					return
				}
			}

			// If the event happened to the watched directory or the watched file, the kernel
			// doesn't append the filename to the event, but we would like to always fill the
			// the "Name" field with a valid filename. We retrieve the path of the watch from
			// the "paths" map.
			watch := w.watches.byWd(uint32(raw.Wd))

			// inotify will automatically remove the watch on deletes; just need
			// to clean our state here.
			if watch != nil && mask&unix.IN_DELETE_SELF == unix.IN_DELETE_SELF {
				w.watches.remove(watch.wd)
			}
			// We can't really update the state when a watched path is moved;
			// only IN_MOVE_SELF is sent and not IN_MOVED_{FROM,TO}. So remove
			// the watch.
			if watch != nil && mask&unix.IN_MOVE_SELF == unix.IN_MOVE_SELF {
				err := w.remove(watch.path)
				if err != nil && !errors.Is(err, ErrNonExistentWatch) {
					if !w.sendError(err) {
						return
					}
				}
			}

			var name string
			if watch != nil {
				name = watch.path
			}
			if nameLen > 0 {
				// Point "bytes" at the first byte of the filename
				bytes := (*[unix.PathMax]byte)(unsafe.Pointer(&buf[offset+unix.SizeofInotifyEvent]))[:nameLen:nameLen]
				// The filename is padded with NULL bytes. TrimRight() gets rid of those.
				name += "/" + strings.TrimRight(string(bytes[0:nameLen]), "\000")
			}

			event := w.newEvent(name, mask)

			// Send the events that are not ignored on the events channel
			if mask&unix.IN_IGNORED == 0 {
				if !w.sendEvent(event) {
					return
				}
			}

			// Move to the next event in the buffer
			offset += unix.SizeofInotifyEvent + nameLen
		}
	}
}

func (w *InotifyWatcher) newEvent(name string, mask uint32) FsEvent {
	e := FsEvent{Name: name}
	if mask&unix.IN_CREATE == unix.IN_CREATE || mask&unix.IN_MOVED_TO == unix.IN_MOVED_TO {
		e.Op |= FileOpCreate
	}
	if mask&unix.IN_DELETE_SELF == unix.IN_DELETE_SELF || mask&unix.IN_DELETE == unix.IN_DELETE {
		e.Op |= FileOpRemove
	}
	if mask&unix.IN_MOVE_SELF == unix.IN_MOVE_SELF || mask&unix.IN_MOVED_FROM == unix.IN_MOVED_FROM {
		e.Op |= FileOpRename
	}
	return e
}

type (
	watches struct {
		mu   sync.RWMutex
		wd   map[uint32]*watch // wd → watch
		path map[string]uint32 // pathname → wd
	}
	watch struct {
		wd    uint32 // Watch descriptor (as returned by the inotify_add_watch() syscall)
		flags uint32 // inotify flags of this watch (see inotify(7) for the list of valid flags)
		path  string // Watch path.
	}
)

func newWatches() *watches {
	return &watches{
		wd:   make(map[uint32]*watch),
		path: make(map[string]uint32),
	}
}

func (w *watches) remove(wd uint32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.path, w.wd[wd].path)
	delete(w.wd, wd)
}

func (w *watches) removePath(path string) (uint32, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	wd, ok := w.path[path]
	if !ok {
		return 0, false
	}

	delete(w.path, path)
	delete(w.wd, wd)

	return wd, true
}

func (w *watches) byWd(wd uint32) *watch {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.wd[wd]
}

func (w *watches) updatePath(path string, f func(*watch) (*watch, error)) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var existing *watch
	wd, ok := w.path[path]
	if ok {
		existing = w.wd[wd]
	}

	upd, err := f(existing)
	if err != nil {
		return err
	}
	if upd != nil {
		w.wd[upd.wd] = upd
		w.path[upd.path] = upd.wd

		if upd.wd != wd {
			delete(w.wd, wd)
		}
	}

	return nil
}
