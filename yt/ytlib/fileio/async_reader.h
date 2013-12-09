#pragma once

#include "public.h"

#include <core/misc/blob.h>
#include <core/logging/tagged_logger.h>
#include <core/concurrency/thread_affinity.h>

#include <util/system/spinlock.h>

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NFileIO {

namespace NDetail {
// Do not use this clas directly
    class TNonBlockReader
    {
    public:
        // It own this fd
        TNonBlockReader(int fd);
        ~TNonBlockReader();

        void TryReadInBuffer();
        std::pair<TBlob, bool> GetRead();

        bool IsBufferFull();
        bool IsBufferEmpty();

        bool InFailedState();
        bool ReachedEOF();
        int GetLastSystemError();

        bool IsReady();

        void Close();
    private:
        int FD;

        TBlob ReadBuffer;
        size_t BytesInBuffer;

        bool ReachedEOF_;
        bool Closed;
        int LastSystemError;

        NLog::TTaggedLogger Logger;
    };
}


class TAsyncReader : public IFDWatcher
{
public:
    TAsyncReader(int fd);
    virtual ~TAsyncReader() {}

    std::pair<TBlob, bool> Read();
    TAsyncError GetReadyEvent();

    virtual void Start(ev::dynamic_loop& eventLoop);
private:
    NDetail::TNonBlockReader Reader;
    ev::io FDWatcher;
    ev::async StartWatcher;

    TNullable<TAsyncErrorPromise> ReadyPromise;

    TSpinLock ReadLock;

    TError GetState();
    void OnRead(ev::io&, int);

    void OnStart(ev::async&, int);

    NLog::TTaggedLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);
};

}
}
