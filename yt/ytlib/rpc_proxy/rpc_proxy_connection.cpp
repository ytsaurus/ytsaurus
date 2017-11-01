#include "discovery_service_proxy.h"
#include "rpc_proxy_connection.h"
#include "rpc_proxy_client.h"
#include "rpc_proxy_transaction.h"
#include "rpc_proxy_timestamp_provider.h"
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

TRpcProxyConnection::TRpcProxyConnection(
    TRpcProxyConnectionConfigPtr config,
    NConcurrency::TActionQueuePtr actionQueue)
    : Config_(std::move(config))
    , ActionQueue_(std::move(actionQueue))
    , ChannelFactory_(CreateBusChannelFactory(Config_->BusClient))
    , Logger(NLogging::TLogger(RpcProxyClientLogger)
        .AddTag("ConnectionId: %v", TGuid::Create()))
    , UpdateProxyListExecutor_(New<TPeriodicExecutor>(
        ActionQueue_->GetInvoker(),
        BIND(&TRpcProxyConnection::OnProxyListUpdated, MakeWeak(this)),
        Config_->UpdatePeriod))
{
    ResetAddresses();
    UpdateProxyListExecutor_->Start();
}

TRpcProxyConnection::~TRpcProxyConnection() = default;

NObjectClient::TCellTag TRpcProxyConnection::GetCellTag()
{
    Y_UNIMPLEMENTED();
}

const NTabletClient::ITableMountCachePtr& TRpcProxyConnection::GetTableMountCache()
{
    Y_UNIMPLEMENTED();
}

const NTransactionClient::ITimestampProviderPtr& TRpcProxyConnection::GetTimestampProvider()
{
    if (!TimestampProvider_) {
        auto guard = Guard(SpinLock_);
        if (!TimestampProvider_) {
            TimestampProvider_ = NTransactionClient::CreateBatchingTimestampProvider(
                New<TRpcProxyTimestampProvider>(MakeWeak(this), Config_->TimestampProviderRpcTimeout),
                Config_->TimestampProviderUpdatePeriod
            );
        }
    }
    return TimestampProvider_;
}

const IInvokerPtr& TRpcProxyConnection::GetInvoker()
{
    return ActionQueue_->GetInvoker();
}

IAdminPtr TRpcProxyConnection::CreateAdmin(const TAdminOptions&)
{
    Y_UNIMPLEMENTED();
}

NApi::IClientPtr TRpcProxyConnection::CreateClient(const TClientOptions& options)
{
    return New<TRpcProxyClient>(MakeStrong(this), options);
}

NHiveClient::ITransactionParticipantPtr TRpcProxyConnection::CreateTransactionParticipant(
    const NHiveClient::TCellId&,
    const TTransactionParticipantOptions&)
{
    Y_UNIMPLEMENTED();
}

void TRpcProxyConnection::ClearMetadataCaches()
{
    Y_UNIMPLEMENTED();
}

void TRpcProxyConnection::Terminate()
{
    Y_UNIMPLEMENTED();
}

IChannelPtr TRpcProxyConnection::GetRandomPeerChannel(IRoamingChannelProvider* provider)
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

void TRpcProxyConnection::RegisterTransaction(TRpcProxyTransaction* transaction)
{
    auto guard = Guard(SpinLock_);
    YCHECK(Transactions_.insert(transaction).second);

    if (!PingExecutor_) {
        PingExecutor_ = New<TPeriodicExecutor>(
            ActionQueue_->GetInvoker(),
            BIND(&TRpcProxyConnection::OnPing, MakeWeak(this)),
            Config_->PingPeriod);
        PingExecutor_->Start();
    }
}

void TRpcProxyConnection::UnregisterTransaction(TRpcProxyTransaction* transaction)
{
    auto guard = Guard(SpinLock_);
    Transactions_.erase(transaction);

    if (Transactions_.empty() && PingExecutor_) {
        PingExecutor_->Stop();
        PingExecutor_.Reset();
    }
}

void TRpcProxyConnection::OnPing()
{
    std::vector<TRpcProxyTransactionPtr> activeTransactions;

    {
        auto guard = Guard(SpinLock_);
        activeTransactions.reserve(Transactions_.size());
        for (auto* rawTransaction : Transactions_) {
            auto transaction = TRpcProxyTransaction::DangerousGetPtr(rawTransaction);
            if (transaction) {
                activeTransactions.push_back(std::move(transaction));
            }
        }
    }

    std::vector<TFuture<void>> pingResults;
    pingResults.reserve(activeTransactions.size());
    for (const auto& activeTransaction : activeTransactions) {
        pingResults.push_back(activeTransaction->Ping());
    }

    CombineAll(pingResults)
        .Subscribe(BIND(&TRpcProxyConnection::OnPingCompleted, MakeWeak(this)));
}

void TRpcProxyConnection::OnPingCompleted(const TErrorOr<std::vector<TError>>& pingResults)
{
    if (pingResults.IsOK()) {
        LOG_DEBUG("Transactions pinged (Count: %v)",
            pingResults.Value().size());
    }
}

TFuture<std::vector<TProxyInfo>> TRpcProxyConnection::DiscoverProxies(const TDiscoverProxyOptions& /*options*/)
{
    TDiscoveryServiceProxy proxy(GetRandomPeerChannel());

    auto req = proxy.DiscoverProxies();

    return req->Invoke().Apply(BIND([] (const TDiscoveryServiceProxy::TRspDiscoverProxiesPtr& rsp) {
        std::vector<TProxyInfo> proxies;
        for (auto&& address : rsp->addresses()) {
            proxies.push_back({address});
        }
        return proxies;
    }));
}

IChannelPtr TRpcProxyConnection::CreateChannelAndRegister(const NApi::TClientOptions& options, IRoamingChannelProvider* provider)
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

    LOG_DEBUG("Originating address is %v", localAddressString);

    auto channel = GetRandomPeerChannel(provider);

    if (options.Token) {
        channel = CreateTokenInjectingChannel(
            channel,
            options.User,
            *options.Token,
            localAddressString);
    } else if (options.SessionId || options.SslSessionId) {
        channel = CreateCookieInjectingChannel(
            channel,
            options.User,
            "yt.yandex-team.ru", // TODO(sandello): where to get this?
            options.SessionId.Get(TString()),
            options.SslSessionId.Get(TString()),
            localAddressString);
    }
    return channel;
}

void TRpcProxyConnection::Unregister(IRoamingChannelProvider* provider)
{
    auto guard = Guard(AddressSpinLock_);
    auto it1 = ProviderToAddress_.find(provider);
    YCHECK(it1 != ProviderToAddress_.end());
    auto it2 = AddressToProviders_.find(it1->second);
    YCHECK(it2 != AddressToProviders_.end());

    LOG_DEBUG("Unregistering channel provider (Address: %v)", it1->second);

    // Cleanup.
    ProviderToAddress_.erase(it1);
    YCHECK(it2->second.erase(provider) == 1);
    if (it2->second.empty()) {
        AddressToProviders_.erase(it2);
    }
}

TFuture<std::vector<TProxyInfo>> TRpcProxyConnection::DiscoverProxies(const IChannelPtr& channel, const TDiscoverProxyOptions& /*options*/)
{
    TDiscoveryServiceProxy proxy(channel);

    auto req = proxy.DiscoverProxies();

    return req->Invoke().Apply(BIND([] (const TDiscoveryServiceProxy::TRspDiscoverProxiesPtr& rsp) {
        std::vector<TProxyInfo> proxies;
        for (auto&& address : rsp->addresses()) {
            proxies.push_back({address});
        }
        return proxies;
    }));
}

void TRpcProxyConnection::ResetAddresses()
{
    LOG_INFO("Reset addresses to configured list (Addresses: %v)", Config_->Addresses);
    auto guard = Guard(AddressSpinLock_);
    Addresses_ = Config_->Addresses;
    std::sort(Addresses_.begin(), Addresses_.end());
}

void TRpcProxyConnection::OnProxyListUpdated()
{
    try {
        if (!DiscoveryChannel_) {
            DiscoveryChannel_ = GetRandomPeerChannel();
        }
        auto asyncProxies = DiscoverProxies(DiscoveryChannel_);
        auto proxies = WaitFor(asyncProxies).ValueOrThrow();

        FailedAttempts_ = 0;

        std::vector<TString> addresses;
        addresses.clear();
        for (const auto& proxy : proxies) {
            addresses.push_back(proxy.Address);
        }
        std::sort(addresses.begin(), addresses.end());

        std::vector<TString> diff;
        std::vector<TFuture<void>> terminated;
        {
            auto guard = Guard(AddressSpinLock_);
            std::set_difference(
                Addresses_.begin(),
                Addresses_.end(),
                addresses.begin(),
                addresses.end(),
                std::back_inserter(diff));

            LOG_DEBUG("Updated proxy list (Unavailable: %v)", diff);
            Addresses_ = std::move(addresses);
            for (const auto& unavailableAddress : diff) {
                auto it = AddressToProviders_.find(unavailableAddress);
                if (it == AddressToProviders_.end()) {
                    break;
                }
                LOG_DEBUG("Resetting operable channels (Address: %v)", unavailableAddress);
                for (auto* operable : it->second) {
                    terminated.push_back(operable->Terminate(TError(NRpc::EErrorCode::Unavailable, "Channel is not unavailable")));
                }
            }
        }
        WaitFor(CombineAll(terminated)).ThrowOnError();
    } catch (const std::exception& ex) {
        LOG_INFO(ex, "Error while trying update proxy list");
        DiscoveryChannel_ = nullptr;
        ++FailedAttempts_;
        if (FailedAttempts_ == Config_->UpdateAttempts) {
            ResetAddresses();
        }
    }
}

IProxyConnectionPtr CreateRpcProxyConnection(TRpcProxyConnectionConfigPtr config)
{
    auto actionQueue = New<TActionQueue>("RpcConnect");
    return New<TRpcProxyConnection>(std::move(config), std::move(actionQueue));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
