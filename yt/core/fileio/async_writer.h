#pragma once

#include "file_io_dispatcher_impl.h"

#include <yt/core/misc/blob.h>

#include <util/system/spinlock.h>

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NFileIO {

class TAsyncWriter : public IFDWatcher
{
public:
    TAsyncWriter(int fd);

    bool Write(const void* data, size_t size);
    TAsyncError Close();
    TAsyncError GetReadyEvent();

    virtual void Start(ev::dynamic_loop& eventLoop);

private:
    void TryCleanBuffer();

    ev::io FDWatcher;

    TNullable<TAsyncErrorPromise> ReadyPromise;

    int FD;

    TBlob WriteBuffer;
    size_t BytesWrittenTotal;
    bool NeedToClose;
    int LastSystemError;

    TSpinLock WriteLock;

    void OnWrite(ev::io&, int);

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);
};

}
}
