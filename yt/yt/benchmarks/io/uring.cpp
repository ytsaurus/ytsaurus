#include "uring.h"

#include <yt/yt/core/misc/error.h>

#include <util/system/error.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

int UringQueueInit(unsigned entries, TUring* ring, unsigned flags)
{
    return io_uring_queue_init(entries, ring, flags);
}

int UringRegisterFiles(TUring* ring, const std::vector<int>& files)
{
    return io_uring_register_files(ring, &files[0], static_cast<int>(files.size()));
}

int UringRegisterBuffers(TUring* ring, const std::vector<TIovec>& buffers)
{
    return io_uring_register_buffers(ring, &buffers[0], static_cast<int>(buffers.size()));
}

void UringPrepareReadv(TUringSqe* sqe, int fd, TIovec* iovecs, unsigned nr_vecs, off_t offset)
{
    io_uring_prep_readv(sqe, fd, iovecs, nr_vecs, offset);
}

void UringPrepareWritev(TUringSqe* sqe, int fd, TIovec* iovecs, unsigned nr_vecs, off_t offset)
{
    io_uring_prep_writev(sqe, fd, iovecs, nr_vecs, offset);
}

void UringPrepareReadFixed(TUringSqe* sqe, int fd, void *buf, unsigned nbytes, off_t offset, int buf_index)
{
    io_uring_prep_read_fixed(sqe, fd, buf, nbytes, offset, buf_index);
}

void UringPrepareWriteFixed(TUringSqe* sqe, int fd, void *buf, unsigned nbytes, off_t offset, int buf_index)
{
    io_uring_prep_write_fixed(sqe, fd, buf, nbytes, offset, buf_index);
}

TUringSqe* UringGetSqe(TUring* ring)
{
    return io_uring_get_sqe(ring);
}

void UringSqeSetFlags(TUringSqe* sqe, unsigned flags)
{
    io_uring_sqe_set_flags(sqe, flags);
}

void UringSqeSetData(TUringSqe* sqe, void* data)
{
    io_uring_sqe_set_data(sqe, data);
}

void* UringCqeGetData(TUringCqe* cqe)
{
    return io_uring_cqe_get_data(cqe);
}

int UringSubmit(TUring* ring)
{
    return io_uring_submit(ring);
}

int UringPeekCqe(TUring* ring, TUringCqe** cqe_ptr)
{
    return io_uring_peek_cqe(ring, cqe_ptr);
}

int UringWaitCqe(TUring* ring, TUringCqe** cqe_ptr)
{
    return io_uring_wait_cqe(ring, cqe_ptr);
}

int UringWaitCqes(TUring* ring, TUringCqe** cqe_ptr, unsigned wait_nr, struct __kernel_timespec *ts, sigset_t *sigmask)
{
    return io_uring_wait_cqes(ring, cqe_ptr, wait_nr, ts, sigmask);
}

void UringCqeSeen(TUring* ring, TUringCqe* cqe)
{
    io_uring_cqe_seen(ring, cqe);
}

void UringExit(TUring* ring)
{
    io_uring_queue_exit(ring);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
