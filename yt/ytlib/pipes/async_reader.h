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
    class TNonBlockReader;
}

class TAsyncReader : public IFDWatcher
{
public:
    TAsyncReader(int fd);
    ~TAsyncReader() override;

    std::pair<TBlob, bool> Read();
    TAsyncError GetReadyEvent();

    TError Close();

private:
    virtual void Start(ev::dynamic_loop& eventLoop) override;

    std::unique_ptr<NDetail::TNonBlockReader> Reader;
    ev::io FDWatcher;
    ev::async StartWatcher;

    TAsyncError RegistrationError;
    TNullable<TAsyncErrorPromise> ReadyPromise;

    TSpinLock ReadLock;

    bool CanReadSomeMore() const;
    TError GetState() const;

    void OnRead(ev::io&, int);

    void OnStart(ev::async&, int);

    NLog::TTaggedLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);
};

}
}
