#pragma once

#include "private.h"
#include "tcp_dispatcher.h"

#include <ytlib/misc/error.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/address.h>

#include <util/thread/lfqueue.h>

#include <contrib/libuv/src/unix/ev/ev++.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

TNetworkAddress GetLocalBusAddress(int port);

////////////////////////////////////////////////////////////////////////////////

struct IEventLoopObject
    : public virtual TRefCounted
{
    virtual void SyncInitialize() = 0;
    virtual void SyncFinalize() = 0;
    virtual Stroka GetLoggingId() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcher::TImpl
{
public:
    TImpl();
    ~TImpl();

    static TImpl* Get();

    void Shutdown();

    const ev::loop_ref& GetEventLoop() const;

    TAsyncError AsyncRegister(IEventLoopObjectPtr object);
    TAsyncError AsyncUnregister(IEventLoopObjectPtr object);

    void AsyncPostEvent(TTcpConnectionPtr connection, EConnectionEvent event);

    DEFINE_BYREF_RW_PROPERTY(TTcpDispatcherStatistics, Statistics);

private:
    TThread Thread;
    ev::dynamic_loop EventLoop;

    bool Stopped;
    ev::async StopWatcher;

    struct TRegisterEntry
    {
        TRegisterEntry()
        { }

        explicit TRegisterEntry(IEventLoopObjectPtr object)
            : Object(std::move(object))
            , Promise(NewPromise<TError>())
        { }

        IEventLoopObjectPtr Object;
        TPromise<TError> Promise;
    };

    typedef TRegisterEntry TUnregisterEntry;

    TLockFreeQueue<TRegisterEntry> RegisterQueue;
    ev::async RegisterWatcher;

    TLockFreeQueue<TUnregisterEntry> UnregisterQueue;
    ev::async UnregisterWatcher;

    struct TEventEntry
    {
        TEventEntry()
        { }

        TEventEntry(TTcpConnectionPtr connection, EConnectionEvent event)
            : Connection(std::move(connection))
            , Event(event)
        { }

        TTcpConnectionPtr Connection;
        EConnectionEvent Event;
    };

    TLockFreeQueue<TEventEntry> EventQueue;
    ev::async EventWatcher;

    yhash_set<IEventLoopObjectPtr> Objects;

    static void* ThreadFunc(void* param);
    void ThreadMain();

    void OnStop(ev::async&, int);
    void OnRegister(ev::async&, int);
    void OnUnregister(ev::async&, int);
    void OnEvent(ev::async&, int);

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
