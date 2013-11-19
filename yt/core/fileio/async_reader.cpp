#include "async_reader.h"

#include <yt/core/logging/log.h>

namespace NYT {
namespace NFileIO {

static const size_t BufferSize = 64 * 1024;

NLog::TLogger Logger("AsyncReader");

TAsyncReader::TAsyncReader(int fd)
    : ReadStatePromise()
    , ReadBuffer(BufferSize)
    , BytesInBuffer(0)
    , FD(fd)
    , IsClosed(false)
    , LastSystemError(0)
{
    LOG_TRACE("Constructing...");
    FDWatcher.set(fd, ev::READ);
}

void TAsyncReader::Start(ev::dynamic_loop& eventLoop)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    FDWatcher.set(eventLoop);
    FDWatcher.set<TAsyncReader, &TAsyncReader::OnRead>(this);
    FDWatcher.start();
}

void TAsyncReader::OnRead(ev::io&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TGuard<TSpinLock> guard(ReadLock);

    LOG_TRACE("Reading to buffer...");

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
        }
        if (ReadStatePromise.HasValue()) {
            LOG_TRACE("Setting future to %" PRId32, LastSystemError);

            if (LastSystemError == 0) {
                ReadStatePromise->Set(TError());
            } else {
                ReadStatePromise->Set(TError::FromSystem(LastSystemError));
            }
            ReadStatePromise.Reset();
        }

        if (size == 0) {
            IsClosed = true;
            FDWatcher.stop();
        }
    } else {
        // do I need to log this event?
    }
}

std::pair<TBlob, bool> TAsyncReader::Read()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(ReadLock);

    TBlob result(std::move(ReadBuffer));
    result.Resize(BytesInBuffer);

    ReadBuffer.Resize(BufferSize);
    BytesInBuffer = 0;

    return std::make_pair(std::move(result), IsClosed);
}

TAsyncError TAsyncReader::GetReadState()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(ReadLock);

    if (IsClosed || (BytesInBuffer > 0)) {
        return MakePromise<TError>(TError());
    } else if (LastSystemError != 0) {
        return MakePromise<TError>(TError::FromSystem(LastSystemError));
    }

    LOG_TRACE("Returning a new future");

    ReadStatePromise.Assign(NewPromise<TError>());
    return ReadStatePromise->ToFuture();
}

}
}
