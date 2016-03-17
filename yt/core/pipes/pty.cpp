#include "pty.h"
#include "async_reader.h"
#include "async_writer.h"

#include <yt/core/misc/common.h>
#include <yt/core/misc/proc.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

TPty::TPty(int height, int width)
{
    SafeOpenPty(&MasterFD_, &SlaveFD_, height, width);
}

TPty::~TPty()
{
    if (MasterFD_ != InvalidFD) {
        YCHECK(TryClose(MasterFD_, false));
    }

    if (SlaveFD_ != InvalidFD) {
        YCHECK(TryClose(SlaveFD_, false));
    }
}

TAsyncWriterPtr TPty::CreateMasterAsyncWriter()
{
    YCHECK(MasterFD_ != InvalidFD);
    int fd = SafeDup(MasterFD_);
    SafeSetCloexec(fd);
    SafeMakeNonblocking(fd);
    return New<TAsyncWriter>(fd);
}

TAsyncReaderPtr TPty::CreateMasterAsyncReader()
{
    YCHECK(MasterFD_ != InvalidFD);
    int fd = SafeDup(MasterFD_);
    SafeSetCloexec(fd);
    SafeMakeNonblocking(fd);
    return New<TAsyncReader>(fd);
}

int TPty::GetMasterFD() const
{
    YCHECK(MasterFD_ != InvalidFD);
    return MasterFD_;
}

int TPty::GetSlaveFD() const
{
    YCHECK(SlaveFD_ != InvalidFD);
    return SlaveFD_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
