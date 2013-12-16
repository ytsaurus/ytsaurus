#include "io_dispatcher_impl.h"

namespace NYT {
namespace NPipes {

TIODispatcher::TImpl::TImpl()
    : Thread(ThreadFunc, this)
    , Stopped(false)
    , StopWatcher(EventLoop)
    , RegisterWatcher(EventLoop)
{
    StopWatcher.set<TImpl, &TImpl::OnStop>(this);
    RegisterWatcher.set<TImpl, &TImpl::OnRegister>(this);

    StopWatcher.start();
    RegisterWatcher.start();

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

    TRegisterEntry entry(std::move(watcher));
    RegisterQueue.Enqueue(entry);
    RegisterWatcher.send();

    return entry.Promise;
}

void TIODispatcher::TImpl::OnStop(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    EventLoop.break_loop();
}

void TIODispatcher::TImpl::OnRegister(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TRegisterEntry entry;
    while (RegisterQueue.Dequeue(&entry)) {
        try {
            entry.Watcher->Start(EventLoop);
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
    return NULL;
}

void TIODispatcher::TImpl::ThreadMain()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    NConcurrency::SetCurrentThreadName("PipesIO");
    EventLoop.run(0);
}

}
}
