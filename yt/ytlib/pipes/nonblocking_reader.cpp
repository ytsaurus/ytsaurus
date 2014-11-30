#include "stdafx.h"
#include "nonblocking_reader.h"

#include "private.h"

namespace NYT {
namespace NPipes {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TNonblockingReader::TNonblockingReader(int fd)
    : FD_(fd)
    , Closed_(false)
    , Logger(PipesLogger)
{
    Logger.AddTag("FD: %v", fd);
}

TNonblockingReader::~TNonblockingReader()
{
    Close();
}

TErrorOr<size_t> TNonblockingReader::Read(void* buf, size_t len)
{
    ssize_t size = -1;
    do {
        size = ::read(FD_, buf, len);
    } while (size == -1 && errno == EINTR);

    if (size == -1) {
        auto error = TError("Failed to read from pipe") << TError::FromSystem();
        LOG_DEBUG(error);
        return error;
    }

    return size;
}

TError TNonblockingReader::Close()
{
    if (!Closed_) {
        int errCode = ::close(FD_);
        Closed_ = true;
        if (errCode == -1 && errno != EAGAIN) {
            // Please read
            // http://lkml.indiana.edu/hypermail/linux/kernel/0509.1/0877.html and
            // http://rb.yandex-team.ru/arc/r/44030/
            // before editing.
            return TError("Failed to close pipe") << TError::FromSystem();
        }
    }
    return TError();
}

bool TNonblockingReader::IsClosed() const
{
    return Closed_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NPipes
} // namespace NYT
