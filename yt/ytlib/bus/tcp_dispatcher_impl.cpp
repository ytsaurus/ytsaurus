#include "stdafx.h"
#include "tcp_dispatcher_impl.h"
#include "config.h"

#ifndef _win_
    #include <sys/socket.h>
    #include <sys/un.h>
#endif

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = BusLogger;
static NProfiling::TProfiler& Profiler = BusProfiler;

////////////////////////////////////////////////////////////////////////////////

Stroka GetLocalBusPath(int port)
{
    return Sprintf("/tmp/yt-local-bus-%d", port);
}

TNetworkAddress GetLocalBusAddress(int port)
{
#ifdef _win_
    ythrow yexception() << "Local bus transport is not supported under this platform";
#else
    sockaddr_un sockAddr;
    memset(&sockAddr, 0, sizeof(sockAddr));
    sockAddr.sun_family = AF_UNIX;
    strncpy(sockAddr.sun_path, ~GetLocalBusPath(port), 100);
    return TNetworkAddress(*reinterpret_cast<sockaddr*>(&sockAddr));
#endif
}

////////////////////////////////////////////////////////////////////////////////

TTcpDispatcher::TImpl::TImpl()
    : Stopped(false)
{
    for (int id = 0; id < ThreadCount; ++id) {
        ThreadContexts.push_back(New<TThreadContext>(id));
    }
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

    FOREACH (auto context, ThreadContexts) {
        context->StopWatcher.send();
        context->Thread.Join();
    }

    Stopped = true;
}

const ev::loop_ref& TTcpDispatcher::TImpl::GetEventLoop(IEventLoopObjectPtr object) const
{
    return ThreadContexts[GetThreadId(object)]->EventLoop;
}

TAsyncError TTcpDispatcher::TImpl::AsyncRegister(IEventLoopObjectPtr object)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TQueueEntry entry(object);
    int threadId = GetThreadId(object);
    auto& context = ThreadContexts[threadId];
    context->RegisterQueue.Enqueue(entry);
    context->RegisterWatcher.send();

    LOG_DEBUG("Object registration enqueued (%s, ThreadId: %d)",
        ~object->GetLoggingId(),
        threadId);

    return entry.Promise;
}

TAsyncError TTcpDispatcher::TImpl::AsyncUnregister(IEventLoopObjectPtr object)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TQueueEntry entry(object);
    int threadId = GetThreadId(object);
    auto& context = ThreadContexts[threadId];
    context->UnregisterQueue.Enqueue(entry);
    context->UnregisterWatcher.send();

    LOG_DEBUG("Object unregistration enqueued (%s, ThreadId: %d)",
        ~object->GetLoggingId(),
        threadId);

    return entry.Promise;
}

int TTcpDispatcher::TImpl::GetThreadId(IEventLoopObjectPtr object) const
{
    return object->GetHash() % ThreadCount;
}

TTcpDispatcher::TImpl* TTcpDispatcher::TImpl::Get()
{
    return ~TTcpDispatcher::Get()->Impl;
}


////////////////////////////////////////////////////////////////////////////////

TTcpDispatcher::TImpl::TThreadContext::TThreadContext(int id)
    : Id(id)
    , Thread(ThreadFunc, (void*) this)
    , StopWatcher(EventLoop)
    , RegisterWatcher(EventLoop)
    , UnregisterWatcher(EventLoop)
{
    StopWatcher.set<TThreadContext, &TThreadContext::OnStop>(this);
    RegisterWatcher.set<TThreadContext, &TThreadContext::OnRegister>(this);
    UnregisterWatcher.set<TThreadContext, &TThreadContext::OnUnregister>(this);

    StopWatcher.start();
    RegisterWatcher.start();
    UnregisterWatcher.start();

    Thread.Start();
}

void* TTcpDispatcher::TImpl::TThreadContext::ThreadFunc(void* param)
{
    auto* self = reinterpret_cast<TThreadContext*>(param);
    self->ThreadMain();
    return NULL;
}

void TTcpDispatcher::TImpl::TThreadContext::ThreadMain()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    NThread::SetCurrentThreadName(~Sprintf("Bus:%d", Id));

    LOG_INFO("TCP bus dispatcher thread %d started", Id);

    EventLoop.run(0);

    LOG_INFO("TCP bus dispatcher thread %d stopped", Id);
}

void TTcpDispatcher::TImpl::TThreadContext::OnStop(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    LOG_INFO("Stopping TCP bus dispatcher thread %d", Id);
    EventLoop.break_loop();
}

void TTcpDispatcher::TImpl::TThreadContext::OnRegister(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TQueueEntry entry;
    while (RegisterQueue.Dequeue(&entry)) {
        try {
            SyncRegister(entry.Object);
            entry.Promise.Set(TError());
        } catch (const std::exception& ex) {
            entry.Promise.Set(TError(ex.what()));
        }
    }
}

void TTcpDispatcher::TImpl::TThreadContext::OnUnregister(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TQueueEntry entry;
    while (UnregisterQueue.Dequeue(&entry)) {
        try {
            SyncUnregister(entry.Object);
            entry.Promise.Set(TError());
        } catch (const std::exception& ex) {
            entry.Promise.Set(TError(ex.what()));
        }
    }
}

void TTcpDispatcher::TImpl::TThreadContext::SyncRegister(IEventLoopObjectPtr object)
{
    LOG_DEBUG("Object registered (%s)", ~object->GetLoggingId());

    object->SyncInitialize();
    YCHECK(Objects.insert(object).second);
}

void TTcpDispatcher::TImpl::TThreadContext::SyncUnregister(IEventLoopObjectPtr object)
{
    LOG_DEBUG("Object unregistered (%s)", ~object->GetLoggingId());

    YCHECK(Objects.erase(object) == 1);
    object->SyncFinalize();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
