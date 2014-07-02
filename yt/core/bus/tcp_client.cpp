#include "stdafx.h"
#include "tcp_client.h"
#include "private.h"
#include "client.h"
#include "bus.h"
#include "config.h"
#include "tcp_connection.h"

#include <core/misc/error.h>
#include <core/misc/address.h>

#include <core/concurrency/thread_affinity.h>

#include <core/rpc/public.h>

#include <errno.h>

#ifndef _WIN32
    #include <netinet/tcp.h>
    #include <sys/socket.h>
#endif

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = BusLogger;
static auto& Profiler = BusProfiler;

////////////////////////////////////////////////////////////////////////////////

//! A lightweight proxy controlling the lifetime of client #TTcpConnection.
/*!
 *  When the last strong reference vanishes, it calls IBus::Terminate
 *  for the underlying connection.
 */
class TTcpClientBusProxy
    : public IBus
{
public:
    TTcpClientBusProxy(
        TTcpBusClientConfigPtr config,
        IMessageHandlerPtr handler)
        : Config(std::move(config))
        , Handler(std::move(handler))
        , DispatcherThread(TTcpDispatcher::TImpl::Get()->AllocateThread())
        , Id(TConnectionId::Create())
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(Config);
        YCHECK(Handler);
    }

    ~TTcpClientBusProxy()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        if (Connection) {
            Connection->Terminate(TError(NRpc::EErrorCode::TransportError, "Bus terminated"));
        }
    }

    void Open()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto interfaceType = GetInterfaceType(Config->Address);

        LOG_DEBUG("Connecting to %s (ConnectionId: %s, InterfaceType: %s)",
            ~Config->Address,
            ~ToString(Id),
            ~ToString(interfaceType));

        Connection = New<TTcpConnection>(
            Config,
            DispatcherThread,
            EConnectionType::Client,
            interfaceType, 
            Id,
            INVALID_SOCKET,
            Config->Address,
            Config->Priority,
            Handler);
        DispatcherThread->AsyncRegister(Connection);
    }

    virtual TAsyncError Send(TSharedRefArray message, EDeliveryTrackingLevel level) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection->Send(std::move(message), level);
    }

    virtual void Terminate(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Connection->Terminate(error);
    }

    virtual void SubscribeTerminated(const TCallback<void(TError)>& callback) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Connection->SubscribeTerminated(callback);
    }

    virtual void UnsubscribeTerminated(const TCallback<void(TError)>& callback) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Connection->UnsubscribeTerminated(callback);
    }

private:
    TTcpBusClientConfigPtr Config;
    IMessageHandlerPtr Handler;
    TTcpDispatcherThreadPtr DispatcherThread;
    TConnectionId Id;

    TTcpConnectionPtr Connection;

    static ETcpInterfaceType GetInterfaceType(const Stroka& address)
    {
        return
            IsLocalServiceAddress(address)
            ? ETcpInterfaceType::Local
            : ETcpInterfaceType::Remote;
    }

};

////////////////////////////////////////////////////////////////////////////////

class TTcpBusClient
    : public IBusClient
{
public:
    explicit TTcpBusClient(TTcpBusClientConfigPtr config)
        : Config(config)
    { }

    virtual IBusPtr CreateBus(IMessageHandlerPtr handler) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto proxy = New<TTcpClientBusProxy>(
            Config,
            std::move(handler));
        proxy->Open();
        return proxy;
    }

private:
    TTcpBusClientConfigPtr Config;

};

////////////////////////////////////////////////////////////////////////////////

IBusClientPtr CreateTcpBusClient(TTcpBusClientConfigPtr config)
{
    return New<TTcpBusClient>(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
