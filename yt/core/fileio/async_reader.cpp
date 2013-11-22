#include "async_reader.h"

#include <yt/core/logging/log.h>

namespace NYT {
namespace NFileIO {

////////////////////////////////////////////////////////////////////////////////

static const size_t ReadBufferSize = 64 * 1024;

NLog::TLogger Logger("AsyncReader");

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

TNonBlockReader::TNonBlockReader(int fd)
    : FD(fd)
    , ReadBuffer(ReadBufferSize)
    , BytesInBuffer(0)
    , ReachedEOF_(false)
    , Closed(false)
    , LastSystemError(0)
{ }

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
            size = read(FD, ReadBuffer.Begin() + BytesInBuffer, count);
        } while (size == -1 && errno == EINTR);

        if (size == -1) {
            LOG_TRACE("Encounter an error: %" PRId32, errno);

            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                LastSystemError = errno;
            }
        } else {
            LOG_TRACE("Read %" PRISZT " bytes", size);

            BytesInBuffer += size;
            if (size == 0) {
                ReachedEOF_ = true;
            }
        }
    } else {
        // do I need to log this event?
    }
}

std::pair<TBlob, bool> TNonBlockReader::GetRead()
{
    TBlob result(std::move(ReadBuffer));
    result.Resize(BytesInBuffer);

    ReadBuffer.Resize(ReadBufferSize);
    BytesInBuffer = 0;

    return std::make_pair(std::move(result), ReachedEOF_);
}

bool TNonBlockReader::IsBufferFull()
{
    return (BytesInBuffer == ReadBuffer.Size());
}

bool TNonBlockReader::IsBufferEmpty()
{
    return (BytesInBuffer == 0);
}

bool TNonBlockReader::InFailedState()
{
    return (LastSystemError != 0);
}

bool TNonBlockReader::ReachedEOF()
{
    return ReachedEOF_;
}

int TNonBlockReader::GetLastSystemError()
{
    YCHECK(InFailedState());
    return LastSystemError;
}

bool TNonBlockReader::IsReady()
{
    if (InFailedState()) {
        return true;
    } else if (ReachedEOF_ || !IsBufferEmpty()) {
        return true;
    }
    return false;
}

void TNonBlockReader::Close()
{
    if (!Closed) {
        int errCode = close(FD);
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

} // NDetail

////////////////////////////////////////////////////////////////////////////////

TAsyncReader::TAsyncReader(int fd)
    : Reader(fd)
    , ReadyPromise()
{
    LOG_TRACE("Constructing...");
    FDWatcher.set(fd, ev::READ);
}

void TAsyncReader::Start(ev::dynamic_loop& eventLoop)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TGuard<TSpinLock> guard(ReadLock);

    StartWatcher.set(eventLoop);
    StartWatcher.set<TAsyncReader, &TAsyncReader::OnStart>(this);
    StartWatcher.start();

    FDWatcher.set(eventLoop);
    FDWatcher.set<TAsyncReader, &TAsyncReader::OnRead>(this);
    FDWatcher.start();
}

void TAsyncReader::OnStart(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    FDWatcher.start();
}

void TAsyncReader::OnRead(ev::io&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TGuard<TSpinLock> guard(ReadLock);

    LOG_TRACE("Reading to buffer...");

    if (!Reader.IsBufferFull()) {
        Reader.TryReadInBuffer();

        if (Reader.ReachedEOF()) {
            FDWatcher.stop();
            Reader.Close();
        }

        if (ReadyPromise.HasValue()) {
            if (Reader.IsReady()) {
                ReadyPromise->Set(GetState());
                ReadyPromise.Reset();
            }
        }
    } else {
        // pause for a while
        FDWatcher.stop();
    }
}

std::pair<TBlob, bool> TAsyncReader::Read()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(ReadLock);

    // ev_io_start is not thread-safe
    StartWatcher.send();

    return Reader.GetRead();
}

TAsyncError TAsyncReader::GetReadyEvent()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(ReadLock);

    if (Reader.IsReady()) {
        return MakePromise(GetState());
    }

    LOG_TRACE("Returning a new future");

    ReadyPromise.Assign(NewPromise<TError>());
    return ReadyPromise->ToFuture();
}

TError TAsyncReader::GetState()
{
    if (Reader.ReachedEOF() || !Reader.IsBufferEmpty()) {
        return TError();
    } else if (Reader.InFailedState()) {
        return TError::FromSystem(Reader.GetLastSystemError());
    } else {
        YCHECK(false);
        return TError();
    }
}


}
}
