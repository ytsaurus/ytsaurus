#include "pread.h"

#include <linux/fs.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

#ifndef RWF_HIPRI
#define RWF_HIPRI       (0x00000001)
#endif
#ifndef RWF_DSYNC
#define RWF_DSYNC       (0x00000002)
#endif
#ifndef RWF_SYNC
#define RWF_SYNC        (0x00000004)
#endif
#ifndef RWF_NOWAIT
#define RWF_NOWAIT      (0x00000008)
#endif

#ifndef __NR_preadv2
#define __NR_preadv2 327
#endif
#ifndef __NR_pwritev2
#define __NR_pwritev2 328
#endif

#ifndef LO_HI_LONG
#define LO_HI_LONG(off) (off), 0
#endif

ssize_t preadv2(int fd, const struct iovec *iov, int iovcnt, off_t offset, int flags)
{
    return syscall(__NR_preadv2, fd, iov, iovcnt, LO_HI_LONG(offset), flags);
}

ssize_t pwritev2(int fd, const struct iovec *iov, int iovcnt, off_t offset, int flags)
{
    return syscall(__NR_pwritev2, fd, iov, iovcnt, LO_HI_LONG(offset), flags);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

int GetFlags(EPreadv2Flags flags)
{
    int fl = 0;
    if (Any(flags & EPreadv2Flags::HighPriority)) {
        fl |= RWF_HIPRI;
    }
    if (Any(flags & EPreadv2Flags::Sync)) {
        fl |= RWF_SYNC;
    }
    if (Any(flags & EPreadv2Flags::DataSync)) {
        fl |= RWF_DSYNC;
    }
    if (Any(flags & EPreadv2Flags::Nowait)) {
        fl |= RWF_NOWAIT;
    }
    return fl;
}

ssize_t Preadv2(::TFile file, const TIovec* iov, int iovcnt, off_t offset, EPreadv2Flags flags)
{
    return NDetail::preadv2(file.GetHandle(), iov, iovcnt, offset, GetFlags(flags));
}

ssize_t Pwritev2(::TFile file, const TIovec* iov, int iovcnt, off_t offset, EPreadv2Flags flags)
{
    return NDetail::pwritev2(file.GetHandle(), iov, iovcnt, offset, GetFlags(flags));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
