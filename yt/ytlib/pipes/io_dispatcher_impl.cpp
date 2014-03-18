#include "stdafx.h"
#include "io_dispatcher_impl.h"

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

TIODispatcher::TImpl::TImpl()
    : Thread(ThreadFunc, this)
    , Stopped(false)
    , StopWatcher(EventLoop)
    , RegisterWatcher(EventLoop)
    , UnregisterWatcher(EventLoop)
{
    StopWatcher.set<TImpl, &TImpl::OnStop>(this);
    RegisterWatcher.set<TImpl, &TImpl::OnRegister>(this);
    UnregisterWatcher.set<TImpl, &TImpl::OnUnregister>(this);

    StopWatcher.start();
    RegisterWatcher.start();
    UnregisterWatcher.start();

    Thread.Start();
}

TIODispatcher::TImpl::~TImpl()
{
    Shutdown();
}

void TIODispatcher::TImpl::Shutdown()
{
    StopWatcher.send();
    Thread.Join();

    Stopped = true;
}

TAsyncError TIODispatcher::TImpl::AsyncRegister(IFDWatcherPtr watcher)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TRegistryEntry entry(std::move(watcher));
    RegisterQueue.Enqueue(entry);

    YCHECK(RegisterWatcher.is_active());
    RegisterWatcher.send();

    return entry.Promise.ToFuture();
}

TAsyncError TIODispatcher::TImpl::AsyncUnregister(IFDWatcherPtr watcher)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TUnregistryEntry entry(std::move(watcher));
    UnregisterQueue.Enqueue(entry);

    YCHECK(UnregisterWatcher.is_active());
    UnregisterWatcher.send();

    return entry.Promise.ToFuture();
}

void TIODispatcher::TImpl::OnStop(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    EventLoop.break_loop();
}

void TIODispatcher::TImpl::OnRegister(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TRegistryEntry entry;
    while (RegisterQueue.Dequeue(&entry)) {
        try {
            entry.Watcher->Start(EventLoop);
            entry.Promise.Set(TError());
        } catch (const std::exception& ex) {
            entry.Promise.Set(ex);
        }
    }
}

void TIODispatcher::TImpl::OnUnregister(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TUnregistryEntry entry;
    while (UnregisterQueue.Dequeue(&entry)) {
        try {
            entry.Watcher->Stop();
            entry.Promise.Set(TError());
        } catch (const std::exception& ex) {
            entry.Promise.Set(ex);
        }
    }
}

void* TIODispatcher::TImpl::ThreadFunc(void* param)
{
    auto* self = reinterpret_cast<TImpl*>(param);
    self->ThreadMain();
    return nullptr;
}

void TIODispatcher::TImpl::ThreadMain()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    NConcurrency::SetCurrentThreadName("PipesIO");
    EventLoop.run(0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
