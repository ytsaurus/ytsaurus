#pragma once

#include "public.h"

#include "async_io.h"

#include <core/misc/blob.h>
#include <core/logging/tagged_logger.h>
#include <core/concurrency/thread_affinity.h>

#include <util/system/spinlock.h>

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NPipes {

namespace NDetail {
    class TNonblockingReader;
}

class TAsyncReader
    : public TAsyncIOBase
{
public:
    TAsyncReader(int fd);
    ~TAsyncReader() override;

    std::pair<TBlob, bool> Read(TBlob&& buffer);
    TAsyncError GetReadyEvent();

    TError Abort();
private:
    virtual void DoStart(ev::dynamic_loop& eventLoop) override;
    virtual void DoStop() override;

    std::unique_ptr<NDetail::TNonblockingReader> Reader;
    ev::io FDWatcher;
    ev::async StartWatcher;

    TError RegistrationError;
    TAsyncErrorPromise ReadyPromise;

    TSpinLock Lock;

    bool CanReadSomeMore() const;
    TError GetState() const;

    virtual void OnRegistered(TError status) override;
    virtual void OnUnregister(TError status) override;

    void OnRead(ev::io&, int);
    void OnStart(ev::async&, int);

    NLog::TTaggedLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);
};

}
}
