#include "pty.h"

#include "io_dispatcher.h"

#include <yt/core/misc/common.h>
#include <yt/core/misc/proc.h>

#include <yt/core/net/connection.h>

namespace NYT::NPipes {

using namespace NNet;

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

IConnectionWriterPtr TPty::CreateMasterAsyncWriter()
{
    YCHECK(MasterFD_ != InvalidFD);
    int fd = SafeDup(MasterFD_);
    SafeSetCloexec(fd);
    SafeMakeNonblocking(fd);
    return CreateConnectionFromFD(fd, {}, {}, TIODispatcher::Get()->GetPoller());
}

IConnectionReaderPtr TPty::CreateMasterAsyncReader()
{
    YCHECK(MasterFD_ != InvalidFD);
    int fd = SafeDup(MasterFD_);
    SafeSetCloexec(fd);
    SafeMakeNonblocking(fd);
    return CreateConnectionFromFD(fd, {}, {}, TIODispatcher::Get()->GetPoller());
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

} // namespace NYT::NPipes
