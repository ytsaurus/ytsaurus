#include "stdafx.h"
#include "tcp_dispatcher_impl.h"
#include "config.h"
#include "tcp_connection.h"

#include <ytlib/misc/thread.h>

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

TNetworkAddress GetLocalBusAddress(int port)
{
#ifdef _win_
    UNUSED(port);
    THROW_ERROR_EXCEPTION("Local bus transport is not supported under this platform");
#else
    auto name = Sprintf("yt-local-bus-%d", port);
    sockaddr_un sockAddr;
    memset(&sockAddr, 0, sizeof(sockAddr));
    sockAddr.sun_family = AF_UNIX;
    strncpy(sockAddr.sun_path + 1, ~name, name.length());
    return TNetworkAddress(
        *reinterpret_cast<sockaddr*>(&sockAddr),
        sizeof (sockAddr.sun_family) +
        sizeof (char) +
        name.length());
#endif
}

////////////////////////////////////////////////////////////////////////////////

TTcpDispatcher::TImpl::TImpl()
    : Thread(ThreadFunc, (void*) this)
    , ThreadStarted(NewPromise<void>())
    , Stopped(false)
    , StopWatcher(EventLoop)
    , RegisterWatcher(EventLoop)
    , UnregisterWatcher(EventLoop)
    , EventWatcher(EventLoop)
{
    StopWatcher.set<TImpl, &TImpl::OnStop>(this);
    RegisterWatcher.set<TImpl, &TImpl::OnRegister>(this);
    UnregisterWatcher.set<TImpl, &TImpl::OnUnregister>(this);
    EventWatcher.set<TImpl, &TImpl::OnEvent>(this);

    StopWatcher.start();
    RegisterWatcher.start();
    UnregisterWatcher.start();
    EventWatcher.start();

    Thread.Start();
}

TTcpDispatcher::TImpl::~TImpl()
{
    Shutdown();
}

void TTcpDispatcher::TImpl::Initialize()
{
    ThreadStarted.Get();
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

    // NB: never ever use logging or any other YT subsystems here.
    // Bus is always started first to get advantange of the root privileges.

    NThread::SetCurrentThreadName("Bus");

    NThread::RaiseCurrentThreadPriority();

    ThreadStarted.Set();

    EventLoop.run(0);
}

void TTcpDispatcher::TImpl::OnStop(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    // NB: No logging here: logging thread may be inactive (e.g. when running with --version).
    EventLoop.break_loop();
}

TAsyncError TTcpDispatcher::TImpl::AsyncRegister(IEventLoopObjectPtr object)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TRegisterEntry entry(object);
    RegisterQueue.Enqueue(entry);
    RegisterWatcher.send();

    LOG_DEBUG("Object registration enqueued (%s)", ~object->GetLoggingId());

    return entry.Promise;
}

TAsyncError TTcpDispatcher::TImpl::AsyncUnregister(IEventLoopObjectPtr object)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TRegisterEntry entry(object);
    UnregisterQueue.Enqueue(entry);
    UnregisterWatcher.send();

    LOG_DEBUG("Object unregistration enqueued (%s)", ~object->GetLoggingId());

    return entry.Promise;
}

void TTcpDispatcher::TImpl::AsyncPostEvent(TTcpConnectionPtr connection, EConnectionEvent event)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TEventEntry entry(std::move(connection), event);
    EventQueue.Enqueue(entry);
    EventWatcher.send();
}

void TTcpDispatcher::TImpl::OnRegister(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TRegisterEntry entry;
    while (RegisterQueue.Dequeue(&entry)) {
        try {
            LOG_DEBUG("Object registered (%s)", ~entry.Object->GetLoggingId());
            entry.Object->SyncInitialize();
            YCHECK(Objects.insert(entry.Object).second);
            entry.Promise.Set(TError());
        } catch (const std::exception& ex) {
            entry.Promise.Set(ex);
        }
    }
}

void TTcpDispatcher::TImpl::OnUnregister(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TRegisterEntry entry;
    while (UnregisterQueue.Dequeue(&entry)) {
        try {
            LOG_DEBUG("Object unregistered (%s)", ~entry.Object->GetLoggingId());
            YCHECK(Objects.erase(entry.Object) == 1);
            entry.Object->SyncFinalize();
            entry.Promise.Set(TError());
        } catch (const std::exception& ex) {
            entry.Promise.Set(ex);
        }
    }
}

void TTcpDispatcher::TImpl::OnEvent(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TEventEntry entry;
    while (EventQueue.Dequeue(&entry)) {
        entry.Connection->SyncProcessEvent(entry.Event);
    }
}

TTcpDispatcher::TImpl* TTcpDispatcher::TImpl::Get()
{
    return ~TTcpDispatcher::Get()->Impl;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
