#include "non_block_writer.h"

#include "private.h"

namespace NYT {
namespace NPipes {
namespace NDetail {

static const size_t WriteBufferSize = 64 * 1024;

////////////////////////////////////////////////////////////////////////////////

TNonblockingWriter::TNonblockingWriter(int fd)
    : FD_(fd)
    , BytesWrittenTotal_(0)
    , Closed_(false)
    , LastSystemError_(0)
    , Logger(WriterLogger)
{
    Logger.AddTag(Sprintf("FD: %s", ~ToString(fd)));
}

TNonblockingWriter::~TNonblockingWriter()
{ }

void TNonblockingWriter::WriteFromBuffer()
{
    YCHECK(WriteBuffer_.Size() >= BytesWrittenTotal_);
    const size_t size = WriteBuffer_.Size() - BytesWrittenTotal_;
    const char* data = WriteBuffer_.Begin() + BytesWrittenTotal_;
    const size_t bytesWritten = TryWrite(data, size);

    if (LastSystemError_ == 0) {
        BytesWrittenTotal_ += bytesWritten;
        TryCleanBuffer();
    }
}

void TNonblockingWriter::Close()
{
    if (!Closed_) {
        int errCode = close(FD_);
        if (errCode == -1 && errno != EAGAIN) {
            // please, read
            // http://lkml.indiana.edu/hypermail/linux/kernel/0509.1/0877.html and
            // http://rb.yandex-team.ru/arc/r/44030/
            // before editing
            LOG_DEBUG(TError::FromSystem(), "Failed to close");

            LastSystemError_ = errno;
        }

        Closed_ = true;
    }
}

void TNonblockingWriter::WriteToBuffer(const char* data, size_t size)
{
    size_t bytesWritten = 0;

    WriteBuffer_.Append(data + bytesWritten, size - bytesWritten);
}

size_t TNonblockingWriter::TryWrite(const char* data, size_t size)
{
    int errCode;
    do {
        errCode = ::write(FD_, data, size);
    } while (errCode == -1 && errno == EINTR);

    if (errCode == -1) {
        if (errno != EWOULDBLOCK && errno != EAGAIN) {
            LOG_DEBUG(TError::FromSystem(), "Failed to write");

            LastSystemError_ = errno;
        }
        return 0;
    } else {
        size_t bytesWritten = errCode;

        YCHECK(bytesWritten <= size);
        return bytesWritten;
    }
}

void TNonblockingWriter::TryCleanBuffer()
{
    if (BytesWrittenTotal_ == WriteBuffer_.Size()) {
        WriteBuffer_.Clear();
        BytesWrittenTotal_ = 0;
    }
}

bool TNonblockingWriter::IsBufferFull() const
{
    return WriteBuffer_.Size() >= WriteBufferSize;
}

bool TNonblockingWriter::IsBufferEmpty() const
{
    return WriteBuffer_.Size() == 0;
}

bool TNonblockingWriter::IsFailed() const
{
    return LastSystemError_ != 0;
}

int TNonblockingWriter::GetLastSystemError() const
{
    return LastSystemError_;
}

bool TNonblockingWriter::IsClosed() const
{
    return Closed_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NPipes
} // namespace NYT
