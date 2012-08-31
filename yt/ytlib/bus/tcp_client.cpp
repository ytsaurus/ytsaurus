#include "stdafx.h"
#include "tcp_client.h"
#include "private.h"
#include "client.h"
#include "bus.h"
#include "config.h"
#include "tcp_connection.h"

#include <ytlib/misc/thread.h>
#include <ytlib/misc/error.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/address.h>

#include <ytlib/rpc/error.h>

#include <errno.h>

#ifndef _WIN32
    #include <netinet/tcp.h>
    #include <sys/socket.h>
#endif

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = BusLogger;
static NProfiling::TProfiler& Profiler = BusProfiler;

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
    TTcpClientBusProxy(TTcpBusClientConfigPtr config, IMessageHandlerPtr handler)
        : Config(config)
        , Handler(handler)
        , Id(TConnectionId::Create())
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YASSERT(config);
        YASSERT(handler);
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
    
        LOG_INFO("Connecting to %s (ConnectionId: %s)",
            ~Config->Address,
            ~Id.ToString());
    
        Connection = New<TTcpConnection>(
            EConnectionType::Client,
            Id,
            INVALID_SOCKET,
            Config->Address,
            Config->Priority,
            Handler);
        TTcpDispatcher::TImpl::Get()->AsyncRegister(Connection);
    }

    virtual TAsyncError Send(IMessagePtr message)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection->Send(message);
    }

    virtual void Terminate(const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Connection->Terminate(error);
    }

    virtual void SubscribeTerminated(const TCallback<void(TError)>& callback)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Connection->SubscribeTerminated(callback);
    }

    virtual void UnsubscribeTerminated(const TCallback<void(TError)>& callback)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Connection->UnsubscribeTerminated(callback);
    }

private:
    TTcpBusClientConfigPtr Config;
    IMessageHandlerPtr Handler;
    TTcpConnectionPtr Connection;

    TConnectionId Id;

};

////////////////////////////////////////////////////////////////////////////////

class TTcpBusClient
    : public IBusClient
{
public:
    explicit TTcpBusClient(TTcpBusClientConfigPtr config)
        : Config(config)
    { }

    virtual IBusPtr CreateBus(IMessageHandlerPtr handler)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto proxy = New<TTcpClientBusProxy>(Config, handler);
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
