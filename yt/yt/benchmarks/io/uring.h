#pragma once

#include <contrib/libs/liburing/src/include/liburing.h>

#include <vector>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

using TUring = struct io_uring;
using TIovec = struct iovec;
using TUringSqe = struct io_uring_sqe;
using TUringCqe = struct io_uring_cqe;

int UringQueueInit(unsigned entries, TUring* ring, unsigned flags);
int UringRegisterFiles(TUring* ring, const std::vector<int>& files);
int UringRegisterBuffers(TUring* ring, const std::vector<TIovec>& buffers);
void UringPrepareReadv(TUringSqe* sqe, int fd, TIovec* iovecs, unsigned nr_vecs, off_t offset);
void UringPrepareWritev(TUringSqe* sqe, int fd, TIovec* iovecs, unsigned nr_vecs, off_t offset);
void UringPrepareReadFixed(TUringSqe* sqe, int fd, void *buf, unsigned nbytes, off_t offset, int buf_index);
void UringPrepareWriteFixed(TUringSqe* sqe, int fd, void *buf, unsigned nbytes, off_t offset, int buf_index);
TUringSqe* UringGetSqe(TUring* ring);
void UringSqeSetFlags(TUringSqe* sqe, unsigned flags);
void UringSqeSetData(TUringSqe* sqe, void* data);
void* UringCqeGetData(TUringCqe* cqe);
int UringSubmit(TUring* ring);
int UringPeekCqe(TUring* ring, TUringCqe** cqe_ptr);
int UringWaitCqe(TUring* ring, TUringCqe** cqe_ptr);
int UringWaitCqes(TUring* ring, TUringCqe** cqe_ptr, unsigned wait_nr, struct __kernel_timespec *ts, sigset_t *sigmask);
void UringCqeSeen(TUring* ring, TUringCqe* cqe);
void UringExit(TUring* ring);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
