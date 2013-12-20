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
    , ReadBuffer_(ReadBufferSize)
    , BytesInBuffer_(0)
    , ReachedEOF_(false)
    , Closed_(false)
    , LastSystemError_(0)
    , Logger(ReaderLogger)
{
    Logger.AddTag(Sprintf("FD: %s", ~ToString(fd)));
}

TNonBlockReader::~TNonBlockReader()
{
    Close();
}

void TNonBlockReader::ReadToBuffer()
{
    YCHECK(ReadBuffer_.Size() >= BytesInBuffer_);
    const size_t count = ReadBuffer_.Size() - BytesInBuffer_;
    if (count > 0) {
        ssize_t size = -1;
        do {
            size = ::read(FD, ReadBuffer_.Begin() + BytesInBuffer_, count);
        } while (size == -1 && errno == EINTR);

        if (size == -1) {
            LOG_DEBUG(TError::FromSystem(), "Failed to read");

            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                LastSystemError_ = errno;
            }
        } else {
            BytesInBuffer_ += size;
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
    if (!Closed_) {
        int errCode = close(FD);

        if (errCode == -1 && errno != EAGAIN) {
            LOG_DEBUG(TError::FromSystem(), "Failed to close");

            // please, read
            // http://lkml.indiana.edu/hypermail/linux/kernel/0509.1/0877.html and
            // http://rb.yandex-team.ru/arc/r/44030/
            // before editing
            LastSystemError_ = errno;
        }
        Closed_ = true;
    }
}

std::pair<TBlob, bool> TNonBlockReader::GetRead(TBlob&& buffer)
{
    TBlob result(std::move(ReadBuffer_));
    result.Resize(BytesInBuffer_);

    ReadBuffer_ = std::move(buffer);
    ReadBuffer_.Resize(ReadBufferSize);
    BytesInBuffer_ = 0;

    return std::make_pair(std::move(result), ReachedEOF_);
}

bool TNonBlockReader::IsBufferFull() const
{
    return BytesInBuffer_ == ReadBuffer_.Size();
}

bool TNonBlockReader::IsBufferEmpty() const
{
    return BytesInBuffer_ == 0;
}

bool TNonBlockReader::InFailedState() const
{
    return LastSystemError_ != 0;
}

bool TNonBlockReader::IsClosed() const
{
    return Closed_;
}

bool TNonBlockReader::ReachedEOF() const
{
    return ReachedEOF_;
}

int TNonBlockReader::GetLastSystemError() const
{
    YCHECK(InFailedState());
    return LastSystemError_;
}

bool TNonBlockReader::IsReady() const
{
    return InFailedState() || ReachedEOF() || !IsBufferEmpty();
}

} // NDetail

////////////////////////////////////////////////////////////////////////////////

}
}
