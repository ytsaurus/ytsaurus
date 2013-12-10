#pragma once

#include "public.h"

#include <core/misc/blob.h>
#include <core/logging/tagged_logger.h>
#include <core/concurrency/thread_affinity.h>

#include <util/system/spinlock.h>

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NPipes {

class TAsyncWriter : public IFDWatcher
{
public:
    TAsyncWriter(int fd);
    ~TAsyncWriter() override;

    bool Write(const void* data, size_t size);
    TAsyncError AsyncClose();
    TAsyncError GetReadyEvent();

    virtual void Start(ev::dynamic_loop& eventLoop);

private:
    void TryCleanBuffer();
    size_t TryWrite(const char* data, size_t size);
    void Close();

    ev::io FDWatcher;

    TNullable<TAsyncErrorPromise> ReadyPromise;

    int FD;

    TBlob WriteBuffer;
    size_t BytesWrittenTotal;
    bool NeedToClose;
    bool Closed;
    int LastSystemError;

    TSpinLock WriteLock;

    NLog::TTaggedLogger Logger;

    void OnWrite(ev::io&, int);

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);
};

}
}
