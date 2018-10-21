#include "admin.h"
#include "discovery_service_proxy.h"
#include "connection_impl.h"
#include "client_impl.h"
#include "config.h"
#include "helpers.h"
#include "private.h"

#include <yt/client/api/admin.h>

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

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NApi {
namespace NRpcProxy {

using namespace NBus;
using namespace NRpc;
using namespace NNet;
using namespace NHttp;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

TString NormalizeHttpProxyUrl(TString url)
{
    const auto CanonicalPrefix = AsStringBuf("http://");
    const auto CanonicalSuffix = AsStringBuf(".yt.yandex.net");

    if (url.find('.') == TString::npos &&
        url.find(':') == TString::npos &&
        url.find("localhost") == TString::npos)
    {
        url.append(CanonicalSuffix);
    }

    if (!url.StartsWith(CanonicalPrefix)) {
        url.prepend(CanonicalPrefix);
    }

    return url;
}

std::vector<TString> GetRpcProxiesFromHttp(
    const NHttp::TClientConfigPtr& config,
    const TString& proxyUrl,
    const TNullable<TString>& oauthToken,
    const TNullable<TString>& role)
{
    auto client = CreateClient(config, TTcpDispatcher::Get()->GetXferPoller());
    auto headers = New<THeaders>();
    if (oauthToken) {
        headers->Add("Authorization", "OAuth " + *oauthToken);
    }
    headers->Add("X-YT-Header-Format", "<format=text>yson");

    headers->Add(
        "X-YT-Parameters", BuildYsonStringFluently(EYsonFormat::Text)
            .BeginMap()
                .Item("output_format")
            .BeginAttributes()
                .Item("format").Value("text")
            .EndAttributes()
            .Value("yson")
            .DoIf(
                role.HasValue(), [&](auto fluent) {
                    fluent.Item("role").Value(*role);
                })
            .EndMap().GetData());

    auto path = proxyUrl + "/api/v4/discover_proxies";
    auto rsp = WaitFor(client->Get(path, headers))
        .ValueOrThrow();
    if (rsp->GetStatusCode() != EStatusCode::OK) {
        THROW_ERROR_EXCEPTION("HTTP proxy discovery request returned an error")
            << TErrorAttribute("status_code", rsp->GetStatusCode())
            << ParseYTError(rsp);
    }

    auto node = ConvertTo<INodePtr>(TYsonString{ToString(rsp->ReadBody())});
    node = node->AsMap()->FindChild("proxies");
    return ConvertTo<std::vector<TString>>(node);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TConnection::TConnection(TConnectionConfigPtr config)
    : Config_(std::move(config))
    , ActionQueue_(New<TActionQueue>("RpcProxyConn"))
    , ChannelFactory_(NRpc::NBus::CreateBusChannelFactory(Config_->BusClient))
    , ChannelPool_(New<TDynamicChannelPool>(ChannelFactory_, Config_))
    , Logger(NLogging::TLogger(RpcProxyClientLogger)
        .AddTag("ConnectionId: %v", TGuid::Create()))
    , UpdateProxyListExecutor_(New<TPeriodicExecutor>(
        ActionQueue_->GetInvoker(),
        BIND(&TConnection::OnProxyListUpdate, MakeWeak(this)),
        Config_->ProxyListUpdatePeriod))
{
    Config_->Postprocess();

    if (!Config_->EnableProxyDiscovery) {
        ChannelPool_->SetAddressList(Config_->Addresses);
    } else if (!Config_->Addresses.empty()) {
        UpdateProxyListExecutor_->Start();
    }
}

TConnection::~TConnection()
{
    Terminate();
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
    if (Config_->ClusterUrl) {
        auto guard = Guard(HttpDiscoveryLock_);
        if (!HttpCredentials_) {
            HttpCredentials_ = options;
            UpdateProxyListExecutor_->Start();
        }
    }

    return New<TClient>(this, ChannelPool_, options);
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
    LOG_DEBUG("Terminating connection");
    ChannelPool_->Terminate();
    UpdateProxyListExecutor_->Stop();
}

const TConnectionConfigPtr& TConnection::GetConfig()
{
    return Config_;
}

std::vector<TString> TConnection::DiscoverProxiesByRpc(const IChannelPtr& channel)
{
    TDiscoveryServiceProxy proxy(channel);

    auto req = proxy.DiscoverProxies();
    if (Config_->ProxyRole) {
        req->set_role(*Config_->ProxyRole);
    }
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
        return GetRpcProxiesFromHttp(
            Config_->HttpClient,
            NormalizeHttpProxyUrl(*Config_->ClusterUrl),
            options.Token,
            Config_->ProxyRole);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error discovering proxies from HTTP")
            << ex;
    }
}

void TConnection::OnProxyListUpdate()
{
    auto backoff = Config_->ProxyListRetryPeriod;
    for (int attempt = 0;; ++attempt) {
        try {
            std::vector<TString> proxies;
            if (Config_->ClusterUrl) {
                LOG_DEBUG("Updating proxy list from HTTP");
                YCHECK(HttpCredentials_);
                proxies = DiscoverProxiesByHttp(*HttpCredentials_);
            } else {
                LOG_DEBUG("Updating proxy list from RPC");
                if (!DiscoveryChannel_) {
                    auto address = Config_->Addresses[RandomNumber(Config_->Addresses.size())];
                    DiscoveryChannel_ = ChannelFactory_->CreateChannel(address);
                }

                try {
                    proxies = DiscoverProxiesByRpc(DiscoveryChannel_);
                } catch (const std::exception& ) {
                    DiscoveryChannel_.Reset();
                    throw;
                }
            }

            if (proxies.empty()) {
                THROW_ERROR_EXCEPTION("Proxy list is empty");
            }

            ChannelPool_->SetAddressList(proxies);

            break;
        } catch (const std::exception& ex) {
            if (attempt > Config_->MaxProxyListUpdateAttempts) {
                ChannelPool_->SetAddressList(TError(ex));
            }

            LOG_ERROR(ex, "Error updating proxy list (Attempt: %d, Backoff: %d)", attempt, backoff);
            TDelayedExecutor::WaitForDuration(backoff);

            if (backoff < Config_->MaxProxyListRetryPeriod) {
                backoff *= 1.2;
            }

            if (attempt > Config_->MaxProxyListUpdateAttempts) {
                attempt = 0;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NApi
} // namespace NYT
