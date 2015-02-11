#include "stdafx.h"

#include "pipe.h"
#include "proc.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const int TPipe::InvalidFd = -1;

////////////////////////////////////////////////////////////////////////////////

TPipeFactory::TPipeFactory(int minFD)
    : MinFD_(minFD)
{ }

TPipeFactory::~TPipeFactory()
{
    for (int fd : ReservedFDs_) {
        TryClose(fd);
    }
}

TPipe TPipeFactory::Create()
{
    while (true) {
        int fd[2];
        SafePipe(fd);
        if (fd[0] >= MinFD_ && fd[1] >= MinFD_) {
            TPipe pipe(fd);
            return pipe;
        } else {
            ReservedFDs_.push_back(fd[0]);
            ReservedFDs_.push_back(fd[1]);
        }
    }
}

void TPipeFactory::Clear()
{
    for (int& fd : ReservedFDs_) {
        SafeClose(fd);
        fd = TPipe::InvalidFd;
    }
    ReservedFDs_.clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
