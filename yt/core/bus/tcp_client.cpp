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

#include <core/ytree/convert.h>

#include <errno.h>

#ifndef _win_
    #include <netinet/tcp.h>
    #include <sys/socket.h>
#endif

namespace NYT {
namespace NBus {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = BusLogger;

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
    explicit TTcpClientBusProxy(TTcpConnectionPtr connection)
        : Connection_(std::move(connection))
    { }

    ~TTcpClientBusProxy()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Connection_->Terminate(TError(NRpc::EErrorCode::TransportError, "Bus terminated"));
    }

    virtual Stroka GetEndpointTextDescription() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->GetEndpointTextDescription();
    }

    virtual TYsonString GetEndpointYsonDescription() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->GetEndpointYsonDescription();
    }

    virtual TFuture<void> Send(TSharedRefArray message, EDeliveryTrackingLevel level) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->Send(std::move(message), level);
    }

    virtual void Terminate(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Connection_->Terminate(error);
    }

    virtual void SubscribeTerminated(const TCallback<void(const TError&)>& callback) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Connection_->SubscribeTerminated(callback);
    }

    virtual void UnsubscribeTerminated(const TCallback<void(const TError&)>& callback) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Connection_->UnsubscribeTerminated(callback);
    }

private:
    const TTcpConnectionPtr Connection_;

};

static ETcpInterfaceType GetInterfaceType(const Stroka& address)
{
    return
        IsLocalServiceAddress(address)
        ? ETcpInterfaceType::Local
        : ETcpInterfaceType::Remote;
}

////////////////////////////////////////////////////////////////////////////////

class TTcpBusClient
    : public IBusClient
{
public:
    explicit TTcpBusClient(TTcpBusClientConfigPtr config)
        : Config_(config)
    { }

    virtual Stroka GetEndpointTextDescription() const override
    {
        return Config_->Address
            ? *Config_->Address
            : "unix://" + *Config_->UnixDomainName;
    }

    virtual TYsonString GetEndpointYsonDescription() const override
    {
        return ConvertToYsonString(GetEndpointTextDescription());
    }

    virtual IBusPtr CreateBus(IMessageHandlerPtr handler) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto id = TConnectionId::Create();
        auto dispatcherThread = TTcpDispatcher::TImpl::Get()->GetClientThread();

        auto interfaceType = Config_->UnixDomainName
             ? ETcpInterfaceType::Remote
             : GetInterfaceType(Config_->Address.Get());

        LOG_DEBUG("Connecting to %v (ConnectionId: %v, InterfaceType: %v)",
            Config_->Address,
            id,
            interfaceType);

        auto connection = New<TTcpConnection>(
            Config_,
            dispatcherThread,
            EConnectionType::Client,
            interfaceType,
            id,
            INVALID_SOCKET,
            Config_->UnixDomainName.HasValue() ? Config_->UnixDomainName.Get() : Config_->Address.Get(),
            Config_->UnixDomainName.HasValue(),
            Config_->Priority,
            handler);

        dispatcherThread->AsyncRegister(connection);

        return New<TTcpClientBusProxy>(connection);
    }

private:
    const TTcpBusClientConfigPtr Config_;

};

IBusClientPtr CreateTcpBusClient(TTcpBusClientConfigPtr config)
{
    return New<TTcpBusClient>(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
