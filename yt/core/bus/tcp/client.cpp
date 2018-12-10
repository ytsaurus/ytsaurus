#include "client.h"
#include "private.h"
#include "client.h"
#include "config.h"
#include "connection.h"

#include <yt/core/bus/bus.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/error.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

#include <errno.h>

#ifdef _unix_
    #include <netinet/tcp.h>
    #include <sys/socket.h>
#endif

namespace NYT::NBus {

using namespace NNet;
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
        Connection_->Terminate(TError(NBus::EErrorCode::TransportError, "Bus terminated"));
    }

    virtual const TString& GetEndpointDescription() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->GetEndpointDescription();
    }

    virtual const IAttributeDictionary& GetEndpointAttributes() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->GetEndpointAttributes();
    }

    virtual const NNet::TNetworkAddress& GetEndpointAddress() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->GetEndpointAddress();
    }

    virtual TTcpDispatcherStatistics GetStatistics() const
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->GetStatistics();
    }

    virtual TFuture<void> Send(TSharedRefArray message, const TSendOptions& options) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->Send(std::move(message), options);
    }

    virtual void SetTosLevel(TTosLevel tosLevel) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection_->SetTosLevel(tosLevel);
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

////////////////////////////////////////////////////////////////////////////////

class TTcpBusClient
    : public IBusClient
{
public:
    explicit TTcpBusClient(TTcpBusClientConfigPtr config)
        : Config_(config)
        , EndpointDescription_(Config_->Address
            ? *Config_->Address
            : Format("unix://%v", *Config_->UnixDomainName))
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("address").Value(EndpointDescription_)
            .EndMap()))
    { }

    virtual const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    virtual const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    virtual IBusPtr CreateBus(IMessageHandlerPtr handler) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto id = TConnectionId::Create();

        LOG_DEBUG("Connecting to server (Address: %v, ConnectionId: %v)",
            EndpointDescription_,
            id);

        auto endpointAttributes = ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Items(*EndpointAttributes_)
                .Item("connection_id").Value(id)
            .EndMap());

        auto connection = New<TTcpConnection>(
            Config_,
            EConnectionType::Client,
            DefaultNetworkName,
            id,
            INVALID_SOCKET,
            EndpointDescription_,
            *endpointAttributes,
            TNetworkAddress{},
            Config_->Address,
            Config_->UnixDomainName,
            handler,
            TTcpDispatcher::TImpl::Get()->GetXferPoller());
        connection->Start();

        return New<TTcpClientBusProxy>(connection);
    }

private:
    const TTcpBusClientConfigPtr Config_;
    const TString EndpointDescription_;
    const std::unique_ptr<IAttributeDictionary> EndpointAttributes_;

};

IBusClientPtr CreateTcpBusClient(TTcpBusClientConfigPtr config)
{
    return New<TTcpBusClient>(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
