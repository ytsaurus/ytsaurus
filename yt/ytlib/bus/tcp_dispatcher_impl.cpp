#include "stdafx.h"
#include "tcp_dispatcher_impl.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = BusLogger;
static NProfiling::TProfiler& Profiler = BusProfiler;

////////////////////////////////////////////////////////////////////////////////

TTcpDispatcher::TImpl::TImpl()
    : Thread(ThreadFunc, (void*) this)
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

TTcpDispatcher::TImpl::~TImpl()
{
    Shutdown();
}

void TTcpDispatcher::TImpl::Shutdown()
{
    if (Stopped) {
        return;
    }

    StopWatcher.send();
    Thread.Join();
    Stopped = true;
}

const ev::loop_ref& TTcpDispatcher::TImpl::GetEventLoop() const
{
    return EventLoop;
}

void* TTcpDispatcher::TImpl::ThreadFunc(void* param)
{
    auto* self = reinterpret_cast<TImpl*>(param);
    self->ThreadMain();
    return NULL;
}

void TTcpDispatcher::TImpl::ThreadMain()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    NThread::SetCurrentThreadName("Bus");

    LOG_INFO("TCP bus dispatcher started");

    EventLoop.run(0);

    LOG_INFO("TCP bus dispatcher stopped");
}

void TTcpDispatcher::TImpl::OnStop(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    LOG_INFO("Stopping TCP bus dispatcher");
    EventLoop.break_loop();
}

TAsyncError TTcpDispatcher::TImpl::AsyncRegister(IEventLoopObjectPtr object)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TQueueEntry entry(object);
    RegisterQueue.Enqueue(entry);
    RegisterWatcher.send();

    LOG_DEBUG("Object registration enqueued (%s)", ~object->GetLoggingId());

    return entry.Promise;
}

TAsyncError TTcpDispatcher::TImpl::AsyncUnregister(IEventLoopObjectPtr object)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TQueueEntry entry(object);
    UnregisterQueue.Enqueue(entry);
    UnregisterWatcher.send();

    LOG_DEBUG("Object unregistration enqueued (%s)", ~object->GetLoggingId());

    return entry.Promise;
}

void TTcpDispatcher::TImpl::OnRegister(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TQueueEntry entry;
    while (RegisterQueue.Dequeue(&entry)) {
        try {
            Register(entry.Object);
            entry.Promise.Set(TError());
        } catch (const std::exception& ex) {
            entry.Promise.Set(TError(ex.what()));
        }
    }
}

void TTcpDispatcher::TImpl::OnUnregister(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TQueueEntry entry;
    while (UnregisterQueue.Dequeue(&entry)) {
        try {
            Unregister(entry.Object);
            entry.Promise.Set(TError());
        } catch (const std::exception& ex) {
            entry.Promise.Set(TError(ex.what()));
        }
    }
}

void TTcpDispatcher::TImpl::Register(IEventLoopObjectPtr object)
{
    LOG_DEBUG("Object registered (%s)", ~object->GetLoggingId());

    object->SyncInitialize();
    YVERIFY(Objects.insert(object).second);
}

void TTcpDispatcher::TImpl::Unregister(IEventLoopObjectPtr object)
{
    LOG_DEBUG("Object unregistered (%s)", ~object->GetLoggingId());

    YVERIFY(Objects.erase(object) == 1);
    object->SyncFinalize();
}

TTcpDispatcher::TImpl* TTcpDispatcher::TImpl::Get()
{
    return ~TTcpDispatcher::Get()->Impl;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
