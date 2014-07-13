#include "stdafx.h"
#include "non_block_reader.h"

#include "private.h"

namespace NYT {
namespace NPipes {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

static const size_t ReadBufferSize = 64 * 1024;

////////////////////////////////////////////////////////////////////////////////

TNonblockingReader::TNonblockingReader(int fd)
    : FD_(fd)
    , ReadBuffer_(ReadBufferSize)
    , BytesInBuffer_(0)
    , ReachedEOF_(false)
    , Closed_(false)
    , LastSystemError_(0)
    , Logger(PipesLogger)
{
    Logger.AddTag("FD: %v", fd);
}

TNonblockingReader::~TNonblockingReader()
{
    Close();
}

void TNonblockingReader::ReadToBuffer()
{
    YCHECK(ReadBuffer_.Size() >= BytesInBuffer_);
    const size_t count = ReadBuffer_.Size() - BytesInBuffer_;
    if (count > 0) {
        ssize_t size = -1;
        do {
            size = ::read(FD_, ReadBuffer_.Begin() + BytesInBuffer_, count);
        } while (size == -1 && errno == EINTR);

        if (size == -1) {
            LOG_DEBUG(TError::FromSystem(), "Failed to read from pipe");

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

void TNonblockingReader::Close()
{
    if (!Closed_) {
        int errCode = close(FD_);

        if (errCode == -1 && errno != EAGAIN) {
            LOG_DEBUG(TError::FromSystem(), "Failed to close the pipe reader");

            // Please read
            // http://lkml.indiana.edu/hypermail/linux/kernel/0509.1/0877.html and
            // http://rb.yandex-team.ru/arc/r/44030/
            // before editing.
            LastSystemError_ = errno;
        }
        Closed_ = true;
    }
}

std::pair<TBlob, bool> TNonblockingReader::GetRead(TBlob&& buffer)
{
    TBlob result(std::move(ReadBuffer_));
    result.Resize(BytesInBuffer_);

    ReadBuffer_ = std::move(buffer);
    ReadBuffer_.Resize(ReadBufferSize);
    BytesInBuffer_ = 0;

    return std::make_pair(std::move(result), ReachedEOF_);
}

bool TNonblockingReader::IsBufferFull() const
{
    return BytesInBuffer_ == ReadBuffer_.Size();
}

bool TNonblockingReader::IsBufferEmpty() const
{
    return BytesInBuffer_ == 0;
}

bool TNonblockingReader::InFailedState() const
{
    return LastSystemError_ != 0;
}

bool TNonblockingReader::IsClosed() const
{
    return Closed_;
}

bool TNonblockingReader::ReachedEOF() const
{
    return ReachedEOF_;
}

int TNonblockingReader::GetLastSystemError() const
{
    YCHECK(InFailedState());
    return LastSystemError_;
}

bool TNonblockingReader::IsReady() const
{
    return InFailedState() || ReachedEOF() || !IsBufferEmpty();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NPipes
} // namespace NYT
