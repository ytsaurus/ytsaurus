#include "non_block_reader.h"

#include "private.h"

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

static const size_t ReadBufferSize = 64 * 1024;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

TNonBlockReader::TNonBlockReader(int fd)
    : FD(fd)
    , ReadBuffer(ReadBufferSize)
    , BytesInBuffer(0)
    , ReachedEOF_(false)
    , Closed(false)
    , LastSystemError(0)
    , Logger(ReaderLogger)
{
    Logger.AddTag(Sprintf("FD: %s", ~ToString(fd)));
}

TNonBlockReader::~TNonBlockReader()
{
    Close();
}

void TNonBlockReader::TryReadInBuffer()
{
    YCHECK(ReadBuffer.Size() >= BytesInBuffer);
    const size_t count = ReadBuffer.Size() - BytesInBuffer;
    if (count > 0) {
        ssize_t size = -1;
        do {
            size = ::read(FD, ReadBuffer.Begin() + BytesInBuffer, count);
        } while (size == -1 && errno == EINTR);

        if (size == -1) {
            LOG_DEBUG("Encounter an error: %" PRId32, errno);

            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                LastSystemError = errno;
            }
        } else {
            LOG_DEBUG("Read %" PRISZT " bytes", size);

            BytesInBuffer += size;
            if (size == 0) {
                ReachedEOF_ = true;
            }
        }
    } else {
        // do I need to log this event?
    }
}

void TNonBlockReader::Close()
{
    if (!Closed) {
        int errCode = close(FD);
        LOG_DEBUG("close syscall returns %" PRId32, errno);

        if (errCode == -1) {
            // please, read
            // http://lkml.indiana.edu/hypermail/linux/kernel/0509.1/0877.html and
            // http://rb.yandex-team.ru/arc/r/44030/
            // before editing
            if (errno != EAGAIN) {
                LastSystemError = errno;
            }
        }
        Closed = true;
    }
}

std::pair<TBlob, bool> TNonBlockReader::GetRead(TBlob&& buffer)
{
    TBlob result(std::move(ReadBuffer));
    result.Resize(BytesInBuffer);

    ReadBuffer = buffer;
    ReadBuffer.Resize(ReadBufferSize);
    BytesInBuffer = 0;

    return std::make_pair(std::move(result), ReachedEOF_);
}

bool TNonBlockReader::IsBufferFull() const
{
    return (BytesInBuffer == ReadBuffer.Size());
}

bool TNonBlockReader::IsBufferEmpty() const
{
    return (BytesInBuffer == 0);
}

bool TNonBlockReader::InFailedState() const
{
    return (LastSystemError != 0);
}

bool TNonBlockReader::ReachedEOF() const
{
    return ReachedEOF_;
}

int TNonBlockReader::GetLastSystemError() const
{
    YCHECK(InFailedState());
    return LastSystemError;
}

bool TNonBlockReader::IsReady() const
{
    if (InFailedState()) {
        return true;
    } else if (ReachedEOF_ || !IsBufferEmpty()) {
        return true;
    }
    return false;
}

} // NDetail

////////////////////////////////////////////////////////////////////////////////

}
}
