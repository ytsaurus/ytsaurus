#pragma once

#include "file_io_dispatcher.h"

#include <yt/core/misc/common.h>
#include <yt/core/misc/error.h>
#include <yt/core/actions/future.h>

#include <util/system/thread.h>
#include <yt/core/concurrency/thread_affinity.h>
#include <util/thread/lfqueue.h>

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NFileIO {

struct IFDWatcher
    : public virtual TRefCounted
{
    virtual void Start(ev::dynamic_loop& eventLoop) = 0;
};

typedef TIntrusivePtr<IFDWatcher> IFDWatcherPtr;

class TFileIODispatcher::TImpl
{
public:
    TImpl();
    ~TImpl();

    void Shutdown();

    TAsyncError AsyncRegister(IFDWatcherPtr watcher);
private:
    TThread Thread;
    ev::dynamic_loop EventLoop;

    bool Stopped;
    ev::async StopWatcher;

    struct TRegisterEntry
    {
        TRegisterEntry()
        {}

        explicit TRegisterEntry(IFDWatcherPtr watcher)
            : Watcher(std::move(watcher))
            , Promise(NewPromise<TError>())
        {}

        IFDWatcherPtr Watcher;
        TPromise<TError> Promise;
    };

    TLockFreeQueue<TRegisterEntry> RegisterQueue;
    ev::async RegisterWatcher;

    void OnRegister(ev::async&, int);
    void OnStop(ev::async&, int);

    static void* ThreadFunc(void* param);
    void ThreadMain();

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);
};

}
}
