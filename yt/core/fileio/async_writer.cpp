#include "async_writer.h"

namespace NYT {
namespace NFileIO {

static const size_t WriteBufferSize = 64 * 1024;

TAsyncWriter::TAsyncWriter(int fd)
    : FD(fd)
    , BytesWrittenTotal(0)
    , NeedToClose(false)
    , LastSystemError(0)
{
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
        const size_t size = WriteBuffer.Size() - BytesWrittenTotal;
        bytesWritten = write(FD, WriteBuffer.Begin() + BytesWrittenTotal, size);
    } while (bytesWritten == -1 && errno == EINTR);
    if (bytesWritten == -1) {
        if (errno != EWOULDBLOCK && errno != EAGAIN) {
            LastSystemError = errno;
        }
    } else {
        BytesWrittenTotal += bytesWritten;
        TryCleanBuffer();
        if (NeedToClose && WriteBuffer.Size() == 0) {
            close(FD);
            NeedToClose = false;
        }
    }

    if (WriteStatePromise.HasValue()) {
        if (LastSystemError == 0) {
            WriteStatePromise->Set(TError());
        } else {
            WriteStatePromise->Set(TError::FromSystem(LastSystemError));
        }
        WriteStatePromise.Reset();
    }
}

bool TAsyncWriter::Write(const void* data, size_t size)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(WriteLock);

    ssize_t bytesWritten = 0;

    if (WriteBuffer.Size() == 0) {
        int errCode = -1;
        do {
            errCode = write(FD, data, size);
        } while (errCode == -1 && errno == EINTR);

        if (errCode == -1) {
            if (errno != EWOULDBLOCK && errno != EAGAIN) {
                LastSystemError = errno;
            }
            bytesWritten = 0;
        } else {
            bytesWritten = errCode;
            BytesWrittenTotal += bytesWritten;
            TryCleanBuffer();
        }
    }

    YCHECK(!WriteStatePromise.HasValue());

    YCHECK(bytesWritten <= size);
    WriteBuffer.Append(data + bytesWritten, size);

    return ((LastSystemError != 0) && (WriteBuffer.Size() <= WriteBufferSize));
}

TAsyncError TAsyncWriter::Close()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(WriteLock);

    NeedToClose = true;
    YCHECK(!WriteStatePromise.HasValue());

    WriteStatePromise.Assign(NewPromise<TError>());
    return WriteStatePromise->ToFuture();
}

TAsyncError TAsyncWriter::GetWriteState()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(WriteLock);

    if (LastSystemError != 0) {
        return MakePromise<TError>(TError::FromSystem(LastSystemError));
    } else if (WriteBuffer.Size() < WriteBufferSize) {
        return MakePromise<TError>(TError());
    } else {
        WriteStatePromise.Assign(NewPromise<TError>());
        return WriteStatePromise->ToFuture();
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
