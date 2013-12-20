#include "non_block_writer.h"

#include "private.h"

namespace NYT {
namespace NPipes {
namespace NDetail {

static const size_t WriteBufferSize = 64 * 1024;

TNonBlockWriter::TNonBlockWriter(int fd)
    : FD(fd)
    , BytesWrittenTotal(0)
    , Closed(false)
    , LastSystemError(0)
    , Logger(WriterLogger)
{
    Logger.AddTag(Sprintf("FD: %s", ~ToString(fd)));
}

TNonBlockWriter::~TNonBlockWriter()
{
}

void TNonBlockWriter::TryWriteFromBuffer()
{
    YCHECK(WriteBuffer.Size() >= BytesWrittenTotal);
    const size_t size = WriteBuffer.Size() - BytesWrittenTotal;
    const char* data = WriteBuffer.Begin() + BytesWrittenTotal;
    const size_t bytesWritten = TryWrite(data, size);

    if (LastSystemError == 0) {
        BytesWrittenTotal += bytesWritten;
        TryCleanBuffer();
    }
}

void TNonBlockWriter::Close()
{
    if (!Closed) {
        int errCode = close(FD);
        if (errCode == -1) {
            // please, read
            // http://lkml.indiana.edu/hypermail/linux/kernel/0509.1/0877.html and
            // http://rb.yandex-team.ru/arc/r/44030/
            // before editing
            if (errno != EAGAIN) {
                LOG_DEBUG(TError::FromSystem(), "Error closing");

                LastSystemError = errno;
            }
        }

        Closed = true;
    }
}

void TNonBlockWriter::WriteToBuffer(const char* data, size_t size)
{
    size_t bytesWritten = 0;

    if (WriteBuffer.Size() == 0) {
        bytesWritten = TryWrite(data, size);
    }

    WriteBuffer.Append(data + bytesWritten, size - bytesWritten);
}

size_t TNonBlockWriter::TryWrite(const char* data, size_t size)
{
    int errCode;
    do {
        errCode = ::write(FD, data, size);
    } while (errCode == -1 && errno == EINTR);

    if (errCode == -1) {
        if (errno != EWOULDBLOCK && errno != EAGAIN) {
            LOG_DEBUG(TError::FromSystem(), "Error writing");

            LastSystemError = errno;
        }
        return 0;
    } else {
        size_t bytesWritten = errCode;
        if (bytesWritten > 0) {
            LOG_DEBUG("Wrote %" PRISZT " bytes", bytesWritten);
        }

        YCHECK(bytesWritten <= size);
        return bytesWritten;
    }
}

void TNonBlockWriter::TryCleanBuffer()
{
    if (BytesWrittenTotal == WriteBuffer.Size()) {
        WriteBuffer.Clear();
        BytesWrittenTotal = 0;
    }
}

bool TNonBlockWriter::IsBufferFull() const
{
    return (WriteBuffer.Size() >= WriteBufferSize);
}

bool TNonBlockWriter::IsBufferEmpty() const
{
    return (WriteBuffer.Size() == 0);
}

bool TNonBlockWriter::InFailedState() const
{
    return (LastSystemError != 0);
}

int TNonBlockWriter::GetLastSystemError() const
{
    return LastSystemError;
}

bool TNonBlockWriter::IsClosed() const
{
    return Closed;
}

}
}
}
