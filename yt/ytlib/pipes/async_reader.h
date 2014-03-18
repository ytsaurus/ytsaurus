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
    class TNonblockingReader;
}

class TAsyncReader
    : public TAsyncIOBase
{
public:
    // TODO(babenko): Owns this fd?
    explicit TAsyncReader(int fd);
    virtual ~TAsyncReader() override;

    // TODO(babenko): can't understand the meaning of this signature
    // WTF is pair<blob, bool> anyway?
    // need to change it or provide a comment at least
    std::pair<TBlob, bool> Read(TBlob&& buffer);
    TAsyncError GetReadyEvent();

    TError Abort();

private:
    virtual void DoStart(ev::dynamic_loop& eventLoop) override;
    virtual void DoStop() override;

    // TODO(babenko): Reader -> Reader_ etc
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
