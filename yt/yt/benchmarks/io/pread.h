#pragma once

#include <library/cpp/yt/misc/enum.h>

#include <util/system/file.h>

#include <sys/uio.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

DEFINE_BIT_ENUM(EPreadv2Flags,
    ((HighPriority)  (0x01))
    ((Sync)          (0x02))
    ((DataSync)      (0x04))
    ((Nowait)        (0x08))
);

using TIovec = struct iovec;

ssize_t Preadv2(::TFile file, const TIovec* iov, int iovcnt, off_t offset, EPreadv2Flags flags);

ssize_t Pwritev2(::TFile file, const TIovec* iov, int iovcnt, off_t offset, EPreadv2Flags flags);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
