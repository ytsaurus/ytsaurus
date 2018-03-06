#include "discovery_service_proxy.h"
#include "connection_impl.h"
#include "client_impl.h"
#include "timestamp_provider.h"
#include "config.h"
#include "credentials_injecting_channel.h"
#include "private.h"

#include <yt/ytlib/transaction_client/remote_timestamp_provider.h>

#include <yt/core/net/local_address.h>
#include <yt/core/net/address.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/rpc/bus_channel.h>
#include <yt/core/rpc/roaming_channel.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NBus;
using namespace NRpc;
using namespace NNet;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TConnection::TConnection(TConnectionConfigPtr config)
    : Config_(std::move(config))
    , ActionQueue_(New<TActionQueue>("RpcProxyConn"))
    , ChannelFactory_(CreateBusChannelFactory(Config_->BusClient))
    , Logger(NLogging::TLogger(RpcProxyClientLogger)
        .AddTag("ConnectionId: %v", TGuid::Create()))
    , UpdateProxyListExecutor_(New<TPeriodicExecutor>(
        ActionQueue_->GetInvoker(),
        BIND(&TConnection::OnProxyListUpdated, MakeWeak(this)),
        Config_->ProxyListUpdatePeriod))
{
    ResetAddresses();
    UpdateProxyListExecutor_->Start();
}

NObjectClient::TCellTag TConnection::GetCellTag()
{
    Y_UNIMPLEMENTED();
}

const NTabletClient::ITableMountCachePtr& TConnection::GetTableMountCache()
{
    Y_UNIMPLEMENTED();
}

const NTransactionClient::ITimestampProviderPtr& TConnection::GetTimestampProvider()
{
    if (!TimestampProviderInitialized_.load()) {
        auto guard = Guard(TimestampProviderSpinLock_);
        if (!TimestampProvider_) {
            TimestampProvider_ = NTransactionClient::CreateBatchingTimestampProvider(
                CreateTimestampProvider(MakeWeak(this), Config_->RpcTimeout),
                Config_->TimestampProviderUpdatePeriod);
        }
        TimestampProviderInitialized_.store(true);
    }
    return TimestampProvider_;
}

const IInvokerPtr& TConnection::GetInvoker()
{
    return ActionQueue_->GetInvoker();
}

IAdminPtr TConnection::CreateAdmin(const TAdminOptions&)
{
    Y_UNIMPLEMENTED();
}

NApi::IClientPtr TConnection::CreateClient(const TClientOptions& options)
{
    return New<TClient>(this, options);
}

NHiveClient::ITransactionParticipantPtr TConnection::CreateTransactionParticipant(
    const NHiveClient::TCellId&,
    const TTransactionParticipantOptions&)
{
    Y_UNIMPLEMENTED();
}

void TConnection::ClearMetadataCaches()
{
    Y_UNIMPLEMENTED();
}

void TConnection::Terminate()
{
    Y_UNIMPLEMENTED();
}

IChannelPtr TConnection::GetRandomPeerChannel(IRoamingChannelProvider* provider)
{
    TString address;
    {
        auto guard = Guard(AddressSpinLock_);
        YCHECK(!Addresses_.empty());
        address = Addresses_[RandomNumber(Addresses_.size())];
        if (provider) {
            AddressToProviders_[address].insert(provider);
            ProviderToAddress_[provider] = address;
        }
    }
    return ChannelFactory_->CreateChannel(address);
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

TFuture<std::vector<TProxyInfo>> TConnection::DiscoverProxies(const TDiscoverProxyOptions& options)
{
    return DiscoverProxies(GetRandomPeerChannel(), options);
}

const TConnectionConfigPtr& TConnection::GetConfig()
{
    return Config_;
}

IChannelPtr TConnection::CreateChannelAndRegisterProvider(
    const NApi::TClientOptions& options,
    IRoamingChannelProvider* provider)
{
    auto localAddress = GetLocalAddress();

    LOG_DEBUG("Creating channel (LocalAddress: %v)",
        localAddress);

    auto channel = GetRandomPeerChannel(provider);

    if (options.Token) {
        channel = CreateTokenInjectingChannel(
            channel,
            options.User,
            *options.Token,
            localAddress);
    } else if (options.SessionId || options.SslSessionId) {
        channel = CreateCookieInjectingChannel(
            channel,
            options.User,
            Config_->Domain,
            options.SessionId.Get(TString()),
            options.SslSessionId.Get(TString()),
            localAddress);
    }
    return channel;
}

void TConnection::UnregisterProvider(IRoamingChannelProvider* provider)
{
    auto guard = Guard(AddressSpinLock_);
    auto it1 = ProviderToAddress_.find(provider);
    YCHECK(it1 != ProviderToAddress_.end());
    auto it2 = AddressToProviders_.find(it1->second);
    YCHECK(it2 != AddressToProviders_.end());

    LOG_DEBUG("Channel provider unregistered (Address: %v)",
        it1->second);

    // Cleanup.
    ProviderToAddress_.erase(it1);
    YCHECK(it2->second.erase(provider) == 1);
    if (it2->second.empty()) {
        AddressToProviders_.erase(it2);
    }
}

TFuture<std::vector<TProxyInfo>> TConnection::DiscoverProxies(const IChannelPtr& channel, const TDiscoverProxyOptions& /*options*/)
{
    TDiscoveryServiceProxy proxy(channel);

    auto req = proxy.DiscoverProxies();
    req->SetTimeout(Config_->RpcTimeout);

    return req->Invoke().Apply(BIND([] (const TDiscoveryServiceProxy::TRspDiscoverProxiesPtr& rsp) {
        std::vector<TProxyInfo> proxies;
        for (const auto& address : rsp->addresses()) {
            proxies.push_back({address});
        }
        return proxies;
    }));
}

void TConnection::ResetAddresses()
{
    LOG_INFO("Proxy address list reset (Addresses: %v)",
        Config_->Addresses);

    try {
        SetProxyList(Config_->Addresses);
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error resetting proxy list");
    }
}

void TConnection::OnProxyListUpdated()
{
    try {
        if (!DiscoveryChannel_) {
            DiscoveryChannel_ = GetRandomPeerChannel();
        }
        auto asyncProxies = DiscoverProxies(DiscoveryChannel_);
        auto proxies = WaitFor(asyncProxies).ValueOrThrow();
        if (proxies.empty()) {
            LOG_DEBUG("Empty proxy list returned, skipping proxy list update");
            return;
        }

        ProxyListUpdatesFailedAttempts_ = 0;

        std::vector<TString> addresses;
        addresses.clear();
        for (const auto& proxy : proxies) {
            addresses.push_back(proxy.Address);
        }

        SetProxyList(addresses);
    } catch (const std::exception& ex) {
        LOG_WARNING(ex, "Error updating proxy list");
        DiscoveryChannel_.Reset();
        ++ProxyListUpdatesFailedAttempts_;
        if (ProxyListUpdatesFailedAttempts_ == Config_->MaxProxyListUpdateAttempts) {
            ResetAddresses();
        }
    }
}

void TConnection::SetProxyList(std::vector<TString> addresses)
{
    std::vector<TString> diff;
    std::vector<TFuture<void>> terminated;

    std::sort(addresses.begin(), addresses.end());

    {
        auto guard = Guard(AddressSpinLock_);
        std::set_difference(
            Addresses_.begin(),
            Addresses_.end(),
            addresses.begin(),
            addresses.end(),
            std::back_inserter(diff));

        LOG_DEBUG("Proxy list updated (UnavailableAddresses: %v)",
            diff);

        Addresses_ = std::move(addresses);
        for (const auto& unavailableAddress : diff) {
            auto it = AddressToProviders_.find(unavailableAddress);
            if (it == AddressToProviders_.end()) {
                continue;
            }

            LOG_DEBUG("Terminating operable channels (Address: %v)",
                unavailableAddress);

            for (auto* operable : it->second) {
                terminated.push_back(operable->Terminate(TError(
                        NRpc::EErrorCode::Unavailable,
                        "Channel is not unavailable")));
                }
            }
    }

    WaitFor(CombineAll(terminated))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
