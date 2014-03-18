#pragma once

#include "public.h"
#include "async_io.h"

#include <core/misc/blob.h>

// TODO(babenko): most of these includes shouldn't be here

#include <core/logging/tagged_logger.h>

#include <core/concurrency/thread_affinity.h>

#include <util/system/spinlock.h>

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): use pimpl and hide it
namespace NDetail {
    class TNonblockingWriter;
}

class TAsyncWriter
    : public TAsyncIOBase
{
public:
    // TODO(babenko): Owns this fd?
    explicit TAsyncWriter(int fd);
    ~TAsyncWriter() override;

    bool Write(const void* data, size_t size);
    TAsyncError AsyncClose();
    TAsyncError GetReadyEvent();

private:
    virtual void DoStart(ev::dynamic_loop& eventLoop) override;
    virtual void DoStop() override;

    // TODO(babenko): Writer -> Writer_ etc
    std::unique_ptr<NDetail::TNonblockingWriter> Writer;
    ev::io FDWatcher;
    ev::async StartWatcher;

    TAsyncErrorPromise ReadyPromise;
    TAsyncErrorPromise ClosePromise;

    TError RegistrationError;
    bool NeedToClose;

    TSpinLock Lock;

    void RestartWatcher();
    TError GetWriterStatus() const;

    bool HasJobToDo() const;

    virtual void OnRegistered(TError status) override;
    virtual void OnUnregister(TError status) override;

    void OnWrite(ev::io&, int);
    void OnStart(ev::async&, int);

    NLog::TTaggedLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
