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

Stroka GetLocalBusPath(int port);
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

    DEFINE_BYREF_RW_PROPERTY(TTcpDispatcherStatistics, Statistics);

private:
    TThread Thread;
    ev::dynamic_loop EventLoop;

    bool Stopped;
    ev::async StopWatcher;

    struct TQueueEntry
    {
        TQueueEntry()
            : Promise(Null)
        { }

        explicit TQueueEntry(IEventLoopObjectPtr object)
            : Object(object)
            , Promise(NewPromise<TError>())
        { }

        IEventLoopObjectPtr Object;
        TPromise<TError> Promise;
    };

    TLockFreeQueue<TQueueEntry> RegisterQueue;
    ev::async RegisterWatcher;

    TLockFreeQueue<TQueueEntry> UnregisterQueue;
    ev::async UnregisterWatcher;

    yhash_set<IEventLoopObjectPtr> Objects;

    static void* ThreadFunc(void* param);
    void ThreadMain();

    void OnStop(ev::async&, int);

    void Register(IEventLoopObjectPtr object);
    void Unregister(IEventLoopObjectPtr object);

    void OnRegister(ev::async&, int);
    void OnUnregister(ev::async&, int);

    DECLARE_THREAD_AFFINITY_SLOT(EventLoop);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
