#pragma once

#include "file_io_dispatcher_impl.h"

#include <yt/core/misc/blob.h>

#include <util/system/spinlock.h>

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NFileIO {

namespace NDetail {
// Do not use this clas directly
    class TNonBlockReader
    {
    public:
        TNonBlockReader(int fd);

        void TryReadInBuffer();
        std::pair<TBlob, bool> GetRead();

        bool IsBufferFull();
        bool IsBufferEmpty();

        bool InFailedState();
        bool ReachedEOF();
        int GetLastSystemError();

        bool IsReady();
    private:
        int FD;

        TBlob ReadBuffer;
        size_t BytesInBuffer;

        bool ReachedEOF_;
        int LastSystemError;
    };
}


class TAsyncReader : public IFDWatcher
{
public:
    TAsyncReader(int fd);

    std::pair<TBlob, bool> Read();
    TAsyncError GetReadyEvent();

    virtual void Start(ev::dynamic_loop& eventLoop);

private:
    NDetail::TNonBlockReader Reader;
    ev::io FDWatcher;

    TNullable<TAsyncErrorPromise> ReadyPromise;

    TSpinLock ReadLock;

    TError GetState();
    void OnRead(ev::io&, int);

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);
};

}
}
