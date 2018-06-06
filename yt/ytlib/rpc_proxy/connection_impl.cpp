#include "admin.h"
#include "discovery_service_proxy.h"
#include "connection_impl.h"
#include "client_impl.h"
#include "config.h"
#include "credentials_injecting_channel.h"
#include "helpers.h"
#include "private.h"

#include <yt/ytlib/api/admin.h>

#include <yt/core/net/local_address.h>
#include <yt/core/net/address.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/bus/tcp/dispatcher.h>

#include <yt/core/http/client.h>
#include <yt/core/http/http.h>
#include <yt/core/http/helpers.h>

#include <yt/core/rpc/bus/channel.h>
#include <yt/core/rpc/roaming_channel.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NBus;
using namespace NRpc;
using namespace NNet;
using namespace NHttp;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

std::vector<TString> GetProxyListFromHttp(
    const NHttp::TClientConfigPtr& config,
    const TString& proxyUrl,
    const TString& oauthToken)
{
    auto client = CreateClient(config, TTcpDispatcher::Get()->GetXferPoller());
    auto headers = New<THeaders>();
    headers->Add("Authorization", "OAuth " + oauthToken);
    headers->Add("X-YT-Header-Format", "<format=text>yson");
    headers->Add("X-YT-Parameters", "{path=\"//sys/rpc_proxies\"; output_format=<format=text>yson; read_from=cache}");

    auto rsp = WaitFor(client->Get(proxyUrl + "/api/v3/list", headers))
        .ValueOrThrow();
    if (rsp->GetStatusCode() != EStatusCode::Ok) {
        THROW_ERROR_EXCEPTION("Http proxy discovery failed")
            << ParseYTError(rsp);
    }
    return ConvertTo<std::vector<TString>>(TYsonString{ToString(rsp->ReadBody())});
}

////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreateCredentialsInjectingChannel(
    IChannelPtr underlying,
    const TClientOptions& options,
    const TString& domain,
    const TString& localAddress)
{
    if (options.Token) {
        return CreateTokenInjectingChannel(
            underlying,
            options.User,
            *options.Token,
            localAddress);
    } else if (options.SessionId || options.SslSessionId) {
        return CreateCookieInjectingChannel(
            underlying,
            options.User,
            domain,
            options.SessionId.Get(TString()),
            options.SslSessionId.Get(TString()),
            localAddress);
    } else {
        return CreateUserInjectingChannel(underlying, options.User);
    }
}

////////////////////////////////////////////////////////////////////////////////

TConnection::TConnection(TConnectionConfigPtr config)
    : Config_(std::move(config))
    , ActionQueue_(New<TActionQueue>("RpcProxyConn"))
    , ChannelFactory_(NRpc::NBus::CreateBusChannelFactory(Config_->BusClient))
    , ChannelPool_(New<TDynamicChannelPool>(ChannelFactory_))
    , Logger(NLogging::TLogger(RpcProxyClientLogger)
        .AddTag("ConnectionId: %v", TGuid::Create()))
    , UpdateProxyListExecutor_(New<TPeriodicExecutor>(
        ActionQueue_->GetInvoker(),
        BIND(&TConnection::OnProxyListUpdate, MakeWeak(this)),
        Config_->ProxyListUpdatePeriod))
{
    ResetAddresses();
    if (!Config_->Addresses.empty()) {
        UpdateProxyListExecutor_->Start();        
    }
}

NObjectClient::TCellTag TConnection::GetCellTag()
{
    Y_UNIMPLEMENTED();
}

IInvokerPtr TConnection::GetInvoker()
{
    return ActionQueue_->GetInvoker();
}

IAdminPtr TConnection::CreateAdmin(const TAdminOptions&)
{
    // This client is used only in tests
    return New<TAdmin>(CreateDynamicChannel(ChannelPool_));
}

NApi::IClientPtr TConnection::CreateClient(const TClientOptions& options)
{
    auto localAddress = GetLocalAddress();
    LOG_DEBUG("Creating client (LocalAddress: %v)", localAddress);

    if (Config_->ClusterUrl) {
        auto guard = Guard(HttpDiscoveryLock_);
        if (!HttpCredentials_) {
            HttpCredentials_ = options;
            UpdateProxyListExecutor_->Start();
        }
    }

    auto channel = CreateDynamicChannel(ChannelPool_);
    auto authenticatedChannel = CreateCredentialsInjectingChannel(
        std::move(channel),
        options,
        Config_->Domain,
        localAddress);
        
    return New<TClient>(this, std::move(authenticatedChannel));
}

NHiveClient::ITransactionParticipantPtr TConnection::CreateTransactionParticipant(
    const NHiveClient::TCellId&,
    const TTransactionParticipantOptions&)
{
    Y_UNIMPLEMENTED();
}

void TConnection::ClearMetadataCaches()
{ }

void TConnection::Terminate()
{
    ChannelPool_->Terminate();
    UpdateProxyListExecutor_->Stop();
}

TString TConnection::GetLocalAddress()
{
    // TODO(sandello): Extract this to a new TAddressResolver method.
    auto localHostname = GetLocalHostName();
    auto localAddress = TAddressResolver::Get()->Resolve(localHostname).Get().ValueOrThrow();

    auto localAddressString = ToString(localAddress);
    YCHECK(localAddressString.StartsWith("tcp://"));
    localAddressString = localAddressString.substr(6);
    {
        auto index = localAddressString.rfind(':');
        if (index != TString::npos) {
            localAddressString = localAddressString.substr(0, index);
        }
    }
    if (localAddressString.StartsWith("[") && localAddressString.EndsWith("]")) {
        localAddressString = localAddressString.substr(1, localAddressString.length() - 2);
    }

    return localAddressString;
}

const TConnectionConfigPtr& TConnection::GetConfig()
{
    return Config_;
}

std::vector<TString> TConnection::DiscoverProxiesByRpc(const IChannelPtr& channel)
{
    TDiscoveryServiceProxy proxy(channel);

    auto req = proxy.DiscoverProxies();
    req->SetTimeout(Config_->RpcTimeout);

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    std::vector<TString> proxies;
    for (const auto& address : rsp->addresses()) {
        proxies.push_back(address);
    }
    return proxies;
}

std::vector<TString> TConnection::DiscoverProxiesByHttp(const TClientOptions& options)
{
    try {
        return GetProxyListFromHttp(
            Config_->HttpClient,
            *Config_->ClusterUrl,
            options.Token.Get({}));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error discovering proxies from HTTP")
            << TError(ex);
    }
}

void TConnection::ResetAddresses()
{
    if (!Config_->Addresses.empty()) {
        LOG_INFO("Proxy address list reset (Addresses: %v)",
            Config_->Addresses);
        ChannelPool_->SetAddressList(MakeFuture(Config_->Addresses));
    } else if (Config_->ClusterUrl) {
        LOG_INFO("Fetching proxy address list from HTTP (ClusterUrl: %v)",
            Config_->ClusterUrl);
        HttpDiscoveryPromise_ = NewPromise<std::vector<TString>>();
        ChannelPool_->SetAddressList(HttpDiscoveryPromise_.ToFuture());        
    } else {
        THROW_ERROR_EXCEPTION("Either \"cluster_url\" or \"addresses\" must be specified");
    }
}

void TConnection::OnProxyListUpdate()
{
    try {
        if (HttpDiscoveryPromise_ && !HttpDiscoveryPromise_.IsSet()) {
            LOG_DEBUG("Updating proxy list from HTTP");
            try {
                YCHECK(HttpCredentials_);
                auto proxies = DiscoverProxiesByHttp(*HttpCredentials_);
                if (proxies.empty()) {
                    LOG_ERROR("Empty proxy list returned, skipping proxy list update");
                    return;
                }

                HttpDiscoveryPromise_.Set(proxies);
            } catch (const std::exception& ex) {
                HttpDiscoveryPromise_.Set(TError(ex));
                HttpDiscoveryPromise_ = NewPromise<std::vector<TString>>();
                ChannelPool_->SetAddressList(HttpDiscoveryPromise_.ToFuture());
                throw;
            }
        } else {
            LOG_DEBUG("Updating proxy list from RPC");
            if (!DiscoveryChannel_) {
                DiscoveryChannel_ = ChannelPool_->TryCreateChannel().ValueOrThrow();
            }
    
            auto proxies = DiscoverProxiesByRpc(DiscoveryChannel_);
            if (proxies.empty()) {
                LOG_ERROR("Empty proxy list returned, skipping proxy list update");
                return;
            }

            ChannelPool_->SetAddressList(MakeFuture(proxies));
        }

        ProxyListUpdatesFailedAttempts_ = 0;
    } catch (const std::exception& ex) {
        LOG_WARNING(ex, "Error updating proxy list");
        DiscoveryChannel_.Reset();
        ++ProxyListUpdatesFailedAttempts_;
        if (ProxyListUpdatesFailedAttempts_ == Config_->MaxProxyListUpdateAttempts) {
            ProxyListUpdatesFailedAttempts_ = 0;
            ResetAddresses();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
