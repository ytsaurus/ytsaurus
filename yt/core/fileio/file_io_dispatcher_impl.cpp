#include "file_io_dispatcher_impl.h"

namespace NYT {
namespace NFileIO {

TFileIODispatcher::TImpl::TImpl()
    : Thread(ThreadFunc, this),
      Stopped(false),
      StopWatcher(EventLoop),
      RegisterWatcher(EventLoop)
{
    StopWatcher.set<TImpl, &TImpl::OnStop>(this);
    RegisterWatcher.set<TImpl, &TImpl::OnRegister>(this);

    StopWatcher.start();
    RegisterWatcher.start();

    Thread.Start();
}

TFileIODispatcher::TImpl::~TImpl()
{
    Shutdown();
}

void TFileIODispatcher::TImpl::Shutdown()
{
    StopWatcher.send();
    Thread.Join();

    Stopped = true;
}

TAsyncError TFileIODispatcher::TImpl::AsyncRegister(IFDWatcherPtr watcher)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TRegisterEntry entry(watcher);
    RegisterQueue.Enqueue(entry);
    RegisterWatcher.send();

    return entry.Promise;
}

void TFileIODispatcher::TImpl::OnStop(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    EventLoop.break_loop();
}

void TFileIODispatcher::TImpl::OnRegister(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TRegisterEntry entry;
    while (RegisterQueue.Dequeue(&entry)) {
        try {
            entry.Watcher->Start(EventLoop);
            entry.Promise.Set(TError());
        } catch (std::exception& ex) {
            entry.Promise.Set(ex);
        }
    }
}

void* TFileIODispatcher::TImpl::ThreadFunc(void* param)
{
    auto* self = reinterpret_cast<TImpl*>(param);
    self->ThreadMain();
    return NULL;
}

void TFileIODispatcher::TImpl::ThreadMain()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    NConcurrency::SetCurrentThreadName("FileIO");
    EventLoop.run(0);
}

}
}
