#pragma once

#include "public.h"

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
    : public IFDWatcher
{
public:
    TAsyncReader(int fd);
    ~TAsyncReader() override;

    std::pair<TBlob, bool> Read(TBlob&& buffer);
    TAsyncError GetReadyEvent();

    TError Abort();

    void Register();
    void Unregister();
private:
    virtual void Start(ev::dynamic_loop& eventLoop) override;
    virtual void Stop() override;

    std::unique_ptr<NDetail::TNonblockingReader> Reader;
    ev::io FDWatcher;
    ev::async StartWatcher;

    TError RegistrationError;
    TAsyncErrorPromise ReadyPromise;

    bool IsAborted_;
    bool IsRegistered_;

    TSpinLock Lock;

    void DoAbort();
    void Close();

    bool IsAborted() const;
    bool IsRegistered() const;
    bool IsStopped() const;

    bool CanReadSomeMore() const;
    TError GetState() const;

    void OnRegistered(TError status);
    void OnUnregister(TError status);

    void OnRead(ev::io&, int);
    void OnStart(ev::async&, int);

    NLog::TTaggedLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);
};

}
}
