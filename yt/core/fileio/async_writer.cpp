#include "async_writer.h"

#include "private.h"

namespace NYT {
namespace NFileIO {

static const size_t WriteBufferSize = 64 * 1024;

TAsyncWriter::TAsyncWriter(int fd)
    : FD(fd)
    , BytesWrittenTotal(0)
    , NeedToClose(false)
    , LastSystemError(0)
    , Logger(WriterLogger)
{
    Logger.AddTag(Sprintf("FD: %s", ~ToString(fd)));

    FDWatcher.set(fd, ev::WRITE);
}

void TAsyncWriter::Start(ev::dynamic_loop& eventLoop)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    FDWatcher.set(eventLoop);
    FDWatcher.set<TAsyncWriter, &TAsyncWriter::OnWrite>(this);
    FDWatcher.start();
}

void TAsyncWriter::OnWrite(ev::io&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TGuard<TSpinLock> guard(WriteLock);

    ssize_t bytesWritten = 0;

    do {
        YCHECK(WriteBuffer.Size() >= BytesWrittenTotal);

        const size_t size = WriteBuffer.Size() - BytesWrittenTotal;
        LOG_DEBUG("Writing %" PRISZT " bytes...", size);

        bytesWritten = ::write(FD, WriteBuffer.Begin() + BytesWrittenTotal, size);
    } while (bytesWritten == -1 && errno == EINTR);
    if (bytesWritten == -1) {
        if (errno != EWOULDBLOCK && errno != EAGAIN) {
            LOG_DEBUG("Encounter an error: %" PRId32, errno);

            LastSystemError = errno;
        }
    } else {
        LOG_DEBUG("Wrote %" PRISZT " bytes", bytesWritten);

        BytesWrittenTotal += bytesWritten;
        TryCleanBuffer();
        if (NeedToClose && WriteBuffer.Size() == 0) {
            LOG_DEBUG("Closing...");

            int errCode = close(FD);
            if (errCode == -1) {
                // please, read
                // http://lkml.indiana.edu/hypermail/linux/kernel/0509.1/0877.html and
                // http://rb.yandex-team.ru/arc/r/44030/
                // before editing
                if (errno != EAGAIN) {
                    LOG_DEBUG("Encounter an error: %" PRId32, errno);

                    LastSystemError = errno;
                }
            }

            NeedToClose = false;
        }
    }

    if (ReadyPromise.HasValue()) {
        if (LastSystemError == 0) {
            ReadyPromise->Set(TError());
        } else {
            ReadyPromise->Set(TError::FromSystem(LastSystemError));
        }
        ReadyPromise.Reset();
    }

    if (LastSystemError != 0) {
        FDWatcher.stop();
    }
}

bool TAsyncWriter::Write(const void* data, size_t size)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(WriteLock);

    ssize_t bytesWritten = 0;

    if (WriteBuffer.Size() == 0) {
        LOG_DEBUG("Internal buffer is empty. Trying to write %" PRISZT " bytes", size);

        int errCode = -1;
        do {
            LOG_DEBUG("Writing %" PRISZT " bytes...", size);

            errCode = ::write(FD, data, size);
        } while (errCode == -1 && errno == EINTR);

        if (errCode == -1) {
            if (errno != EWOULDBLOCK && errno != EAGAIN) {
                LOG_DEBUG("Encounter an error: %" PRId32, errno);

                LastSystemError = errno;
            }
            bytesWritten = 0;
        } else {
            bytesWritten = errCode;
            TryCleanBuffer();
        }
    }

    LOG_DEBUG("Wrote %" PRISZT " bytes", bytesWritten);

    YCHECK(!ReadyPromise.HasValue());

    YCHECK(bytesWritten <= size);
    WriteBuffer.Append(data + bytesWritten, size - bytesWritten);

    return ((LastSystemError != 0) || (WriteBuffer.Size() >= WriteBufferSize));
}

TAsyncError TAsyncWriter::Close()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(WriteLock);

    NeedToClose = true;
    YCHECK(!ReadyPromise.HasValue());

    ReadyPromise.Assign(NewPromise<TError>());
    return ReadyPromise->ToFuture();
}

TAsyncError TAsyncWriter::GetReadyEvent()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(WriteLock);

    if (LastSystemError != 0) {
        return MakePromise<TError>(TError::FromSystem(LastSystemError));
    } else if (WriteBuffer.Size() < WriteBufferSize) {
        return MakePromise<TError>(TError());
    } else {
        ReadyPromise.Assign(NewPromise<TError>());
        return ReadyPromise->ToFuture();
    }
}

void TAsyncWriter::TryCleanBuffer()
{
    if (BytesWrittenTotal == WriteBuffer.Size()) {
        WriteBuffer.Clear();
        BytesWrittenTotal = 0;
    }
}

}
}
