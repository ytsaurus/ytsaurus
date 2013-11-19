#pragma once

#include "file_io_dispatcher_impl.h"

#include <yt/core/misc/blob.h>

#include <util/system/spinlock.h>

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NFileIO {

class TAsyncReader : public IFDWatcher
{
public:
    TAsyncReader(int fd);

    std::pair<TBlob, bool> Read();
    TAsyncError GetReadState();

    virtual void Start(ev::dynamic_loop& eventLoop);

private:
    ev::io FDWatcher;

    TNullable<TAsyncErrorPromise> ReadStatePromise;

    TBlob ReadBuffer;
    size_t BytesInBuffer;

    int FD;
    bool IsClosed;
    int LastSystemError;

    TSpinLock ReadLock;

    void OnRead(ev::io&, int);

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);
};

}
}
