#pragma once

#include "io_dispatcher.h"
#include "public.h"

#include <core/misc/common.h>
#include <core/misc/error.h>

#include <core/actions/future.h>

#include <core/concurrency/thread_affinity.h>

#include <util/system/thread.h>

#include <util/thread/lfqueue.h>

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

class TIODispatcher::TImpl
{
public:
    TImpl();
    ~TImpl();

    void Shutdown();

    TAsyncError AsyncRegister(IFDWatcherPtr watcher);
    TAsyncError AsyncUnregister(IFDWatcherPtr watcher);

private:
    // TODO(babenko): Thread -> Thread_ etc
    TThread Thread;
    ev::dynamic_loop EventLoop;

    bool Stopped;
    ev::async StopWatcher;

    struct TRegistryEntry
    {
        TRegistryEntry()
        { }

        explicit TRegistryEntry(IFDWatcherPtr&& watcher)
            : Watcher(std::move(watcher))
            , Promise(NewPromise<TError>())
        { }

        IFDWatcherPtr Watcher;
        TPromise<TError> Promise;
    };

    TLockFreeQueue<TRegistryEntry> RegisterQueue;
    ev::async RegisterWatcher;

    struct TUnregistryEntry
    {
        TUnregistryEntry()
        { }

        explicit TUnregistryEntry(IFDWatcherPtr&& watcher)
            : Watcher(std::move(watcher))
            , Promise(NewPromise<TError>())
        { }

        IFDWatcherPtr Watcher;
        TPromise<TError> Promise;
    };

    TLockFreeQueue<TUnregistryEntry> UnregisterQueue;
    ev::async UnregisterWatcher;

    void OnRegister(ev::async&, int);
    void OnUnregister(ev::async&, int);
    void OnStop(ev::async&, int);

    static void* ThreadFunc(void* param);
    void ThreadMain();

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
