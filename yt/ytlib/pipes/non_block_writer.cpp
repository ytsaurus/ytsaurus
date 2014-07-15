#include "stdafx.h"
#include "non_block_writer.h"

#include "private.h"

namespace NYT {
namespace NPipes {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

static const size_t WriteBufferSize = 64 * 1024;

////////////////////////////////////////////////////////////////////////////////

TNonblockingWriter::TNonblockingWriter(int fd)
    : FD_(fd)
    , Closed_(false)
    , Logger(PipesLogger)
{
    Logger.AddTag("FD: %v", fd);
}

TNonblockingWriter::~TNonblockingWriter()
{ }

TError TNonblockingWriter::Close()
{
    if (!Closed_) {
        int errCode = ::close(FD_);
        Closed_ = true;
        if (errCode == -1 && errno != EAGAIN) {
            // please, read
            // http://lkml.indiana.edu/hypermail/linux/kernel/0509.1/0877.html and
            // http://rb.yandex-team.ru/arc/r/44030/
            // before editing
            return TError("Failed to close") << TError::FromSystem();
        }
    }
    return TError();
}

TErrorOr<size_t> TNonblockingWriter::Write(const char* data, size_t size)
{
    int errCode;
    do {
        errCode = ::write(FD_, data, size);
    } while (errCode == -1 && errno == EINTR);

    if (errCode == -1) {
        if (errno != EWOULDBLOCK && errno != EAGAIN) {
            return TError("Failed to write to pipe") << TError::FromSystem();
        }
        return 0;
    } else {
        size_t bytesWritten = errCode;
        YCHECK(bytesWritten <= size);
        return bytesWritten;
    }
}

bool TNonblockingWriter::IsClosed() const
{
    return Closed_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NPipes
} // namespace NYT
