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
    class TNonblockingWriter;
}

class TAsyncWriter : public IFDWatcher
{
public:
    TAsyncWriter(int fd);
    ~TAsyncWriter() override;

    bool Write(const void* data, size_t size);
    TAsyncError AsyncClose();
    TAsyncError GetReadyEvent();

private:
    virtual void Start(ev::dynamic_loop& eventLoop) override;
    virtual void Stop() override;

    std::unique_ptr<NDetail::TNonblockingWriter> Writer;
    ev::io FDWatcher;
    ev::async StartWatcher;

    TAsyncErrorPromise ReadyPromise;
    TAsyncErrorPromise ClosePromise;

    TError RegistrationError;
    bool IsRegistered_;
    bool NeedToClose;

    TSpinLock Lock;

    void Close();
    void RestartWatcher();
    TError GetWriterStatus() const;

    bool IsStopped() const;
    bool IsRegistered() const;
    bool HasJobToDo() const;

    void OnRegistered(TError status);

    void OnWrite(ev::io&, int);
    void OnStart(ev::async&, int);

    NLog::TTaggedLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);
};

}
}
