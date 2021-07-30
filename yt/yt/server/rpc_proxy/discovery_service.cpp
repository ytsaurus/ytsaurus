#include "discovery_service.h"
#include "proxy_coordinator.h"
#include "config.h"
#include "public.h"
#include "private.h"
#include "bootstrap.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/api/rpc_proxy/discovery_service_proxy.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/helpers.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <yt/yt/build/build.h>

namespace NYT::NRpcProxy {

using namespace NApi;
using namespace NApi::NRpcProxy;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NRpc;
using namespace NNet;
using namespace NNodeTrackerClient;
using namespace NApi::NNative;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const TString ExpirationTimeAttributeName = "expiration_time";
static const TString VersionAttributeName = "version";
static const TString StartTimeAttributeName = "start_time";

////////////////////////////////////////////////////////////////////////////////

namespace {

const TServiceDescriptor& GetDescriptor()
{
    static const auto descriptor = TServiceDescriptor(DiscoveryServiceName)
        .SetProtocolVersion({0, 0});
    return descriptor;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryService
    : public TServiceBase
{
public:
    TDiscoveryService(
        TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetWorkerInvoker(),
            GetDescriptor(),
            RpcProxyLogger)
        , Bootstrap_(bootstrap)
        , Config_(bootstrap->GetConfig()->DiscoveryService)
        , Coordinator_(bootstrap->GetProxyCoordinator())
        , RootClient_(bootstrap->GetNativeClient())
        , ProxyPath_(RpcProxiesPath + "/" + BuildServiceAddress(
            GetLocalHostName(),
            Bootstrap_->GetConfig()->RpcPort))
        , AliveUpdateExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TDiscoveryService::OnPeriodicEvent, MakeWeak(this), &TDiscoveryService::UpdateLiveness),
            TPeriodicExecutorOptions::WithJitter(Config_->LivenessUpdatePeriod)))
        , ProxyUpdateExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TDiscoveryService::OnPeriodicEvent, MakeWeak(this), &TDiscoveryService::UpdateProxies),
            TPeriodicExecutorOptions::WithJitter(Config_->ProxyUpdatePeriod)))
    {
        Initialize();

        if (Bootstrap_->GetConfig()->GrpcServer) {
            const auto& addresses = Bootstrap_->GetConfig()->GrpcServer->Addresses;
            YT_VERIFY(addresses.size() == 1);

            int port;
            ParseServiceAddress(addresses[0]->Address, nullptr, &port);

            GrpcProxyPath_ = GrpcProxiesPath + "/" + BuildServiceAddress(GetLocalHostName(), port);
        }

        RegisterMethod(RPC_SERVICE_METHOD_DESC(DiscoverProxies));
    }

private:
    TBootstrap* const Bootstrap_;
    const TDiscoveryServiceConfigPtr Config_;
    const IProxyCoordinatorPtr Coordinator_;
    const NNative::IClientPtr RootClient_;
    const TString ProxyPath_;
    const TPeriodicExecutorPtr AliveUpdateExecutor_;
    const TPeriodicExecutorPtr ProxyUpdateExecutor_;

    std::optional<TString> GrpcProxyPath_;

    TInstant LastSuccessTimestamp_ = Now();

    YT_DECLARE_SPINLOCK(TAdaptiveLock, ProxySpinLock_);

    struct TProxy
    {
        TString Address;
        TString Role;
    };

    std::vector<TProxy> AvailableProxies_;

    bool Initialized_ = false;


    void Initialize()
    {
        AliveUpdateExecutor_->Start();
        ProxyUpdateExecutor_->Start();
    }

    std::vector<TString> GetCypressPaths() const
    {
        std::vector<TString> paths = {ProxyPath_};
        if (GrpcProxyPath_) {
            paths.push_back(*GrpcProxyPath_);
        }
        return paths;
    }

    void CreateProxyNode()
    {
        auto channel = RootClient_->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
        TObjectServiceProxy proxy(channel);

        auto batchReq = proxy.ExecuteBatch();

        for (const auto& path : GetCypressPaths()) {
            {
                auto req = TCypressYPathProxy::Create(path);
                req->set_type(static_cast<int>(EObjectType::MapNode));
                req->set_recursive(true);
                req->set_ignore_existing(true);
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }
            {
                auto req = TYPathProxy::Set(path + "/@" + VersionAttributeName);
                req->set_value(ConvertToYsonString(GetVersion()).ToString());
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }
            {
                auto req = TYPathProxy::Set(path + "/@" + StartTimeAttributeName);
                req->set_value(ConvertToYsonString(TInstant::Now().ToString()).ToString());
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }
            {
                auto req = TCypressYPathProxy::Set(path + "/@annotations");
                req->set_value(ConvertToYsonString(Bootstrap_->GetConfig()->CypressAnnotations).ToString());
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }
            {
                auto req = TCypressYPathProxy::Create(path + "/orchid");
                req->set_ignore_existing(true);
                req->set_type(static_cast<int>(EObjectType::Orchid));
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("remote_addresses", Bootstrap_->GetLocalAddresses());
                ToProto(req->mutable_node_attributes(), *attributes);
                batchReq->AddRequest(req);
            }
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error creating proxy node %Qv",
            ProxyPath_);

        YT_LOG_INFO("Proxy node created (Path: %v)", ProxyPath_);
    }

    bool IsAvailable() const
    {
        return Now() - LastSuccessTimestamp_ < Config_->AvailabilityPeriod;
    }

    void OnPeriodicEvent(void (TDiscoveryService::*action)())
    {
        TDuration backoffDuration;
        while (true) {
            try {
                (this->*action)();
                return;
            } catch (const std::exception& ex) {
                backoffDuration = Min(backoffDuration + RandomDuration(Max(backoffDuration, Config_->LivenessUpdatePeriod)),
                    Config_->BackoffPeriod);
                YT_LOG_WARNING(ex, "Failed to perform update, backing off (Duration: %v)", backoffDuration);
                if (!IsAvailable() && Coordinator_->SetAvailableState(false)) {
                    Initialized_ = false;
                    YT_LOG_WARNING("Connectivity lost");
                }
                TDelayedExecutor::WaitForDuration(backoffDuration);
            }
        }
    }

    void UpdateLiveness()
    {
        if (!Initialized_) {
            CreateProxyNode();
            Initialized_ = true;
        }

        auto channel = RootClient_->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
        TObjectServiceProxy proxy(channel);
        auto batchReq = proxy.ExecuteBatch();

        for (const auto& path : GetCypressPaths()) {
            auto req = TCypressYPathProxy::Create(path + "/" + AliveNodeName);
            req->set_type(static_cast<int>(EObjectType::MapNode));
            auto* attr = req->mutable_node_attributes()->add_attributes();
            attr->set_key(ExpirationTimeAttributeName);
            attr->set_value(ConvertToYsonString(TInstant::Now() + Config_->AvailabilityPeriod).ToString());
            req->set_force(true);
            batchReq->AddRequest(req);
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error updating proxy liveness");

        LastSuccessTimestamp_ = Now();
        if (Coordinator_->SetAvailableState(true)) {
            YT_LOG_INFO("Connectivity restored");
        }
    }

    void UpdateProxies()
    {
        TMasterReadOptions options{
            .ReadFrom = EMasterChannelKind::LocalCache,
            .ExpireAfterSuccessfulUpdateTime = Config_->ProxyUpdatePeriod,
            .ExpireAfterFailedUpdateTime = Config_->ProxyUpdatePeriod,
            .CacheStickyGroupSize = 1
        };

        auto connection = Bootstrap_->GetNativeConnection();
        auto channel = RootClient_->GetMasterChannelOrThrow(options.ReadFrom);
        TObjectServiceProxy proxy(channel, connection->GetStickyGroupSizeCache());

        auto batchReq = proxy.ExecuteBatch();
        SetBalancingHeader(batchReq, connection->GetConfig(), options);

        {
            auto req = TYPathProxy::Get(ProxyPath_ + "/@");
            ToProto(
                req->mutable_attributes()->mutable_keys(),
                std::vector<TString>{
                    RoleAttributeName,
                    BannedAttributeName,
                    BanMessageAttributeName,
                });
            SetCachingHeader(req, connection->GetConfig(), options);
            batchReq->AddRequest(req, "get_ban");
        }

        {
            auto req = TYPathProxy::Get(RpcProxiesPath);
            ToProto(
                req->mutable_attributes()->mutable_keys(),
                std::vector<TString>{
                    RoleAttributeName,
                    BannedAttributeName,
                });
            SetCachingHeader(req, connection->GetConfig(), options);
            batchReq->AddRequest(req, "get_proxies");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error getting states of proxies");
        const auto& batchRsp = batchRspOrError.Value();

        {
            auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_ban").Value();
            auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
            bool banned = attributes->Get(BannedAttributeName, false);
            bool changed = Coordinator_->SetBannedState(banned);
            if (changed) {
                if (banned) {
                    Coordinator_->SetBanMessage(attributes->Get(BanMessageAttributeName, TString()));
                }
                YT_LOG_INFO("Proxy has been %v (Path: %v)", banned ? "banned" : "unbanned", ProxyPath_);
            }

            auto role = attributes->Find<TString>(RoleAttributeName);
            Coordinator_->SetProxyRole(role);

            if (role) {
                NProfiling::TSolomonRegistry::Get()->SetDynamicTags({NProfiling::TTag{"proxy_role", *role}});
            } else {
                NProfiling::TSolomonRegistry::Get()->SetDynamicTags({});
            }
        }
        {
            std::vector<TProxy> proxies;

            auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_proxies").Value();
            auto nodeResult = ConvertToNode(TYsonString(rsp->value()));

            for (const auto& child : nodeResult->AsMap()->GetChildren()) {
                bool banned = child.second->Attributes().Get(BannedAttributeName, false);
                auto role = child.second->Attributes().Get<TString>(RoleAttributeName, DefaultProxyRole);
                bool alive = static_cast<bool>(child.second->AsMap()->FindChild(AliveNodeName));
                bool available = alive && !banned;
                if (available) {
                    proxies.push_back({child.first, role});
                }
            }
            YT_LOG_DEBUG("Updated proxy list (ProxyCount: %v)", proxies.size());

            {
                auto guard = Guard(ProxySpinLock_);
                AvailableProxies_ = std::move(proxies);
            }
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, DiscoverProxies)
    {
        Coordinator_->ValidateOperable();

        auto roleFilter = request->has_role() ? request->role() : DefaultProxyRole;

        context->SetRequestInfo("Role: %v", roleFilter);

        {
            auto guard = Guard(ProxySpinLock_);
            for (const auto& proxy : AvailableProxies_) {
                if (proxy.Role == roleFilter) {
                    *response->mutable_addresses()->Add() = proxy.Address;
                }
            }
        }

        context->SetResponseInfo("ProxyCount: %v", response->addresses_size());
        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateDiscoveryService(
    TBootstrap* bootstrap)
{
    return New<TDiscoveryService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
