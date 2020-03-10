#include "node_addresses_provider.h"

#include <yt/client/api/connection.h>

#include <yt/client/node_tracker_client/private.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/atomic_object.h>

#include <yt/core/rpc/dispatcher.h>
#include <yt/core/rpc/roaming_channel.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/ytlib/cell_master_client/cell_directory.h>

namespace NYT::NNodeTrackerClient {

using namespace NApi;
using namespace NRpc;
using namespace NYTree;
using namespace NConcurrency;
using namespace NCellMasterClient;
using namespace NObjectClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

std::vector<TString> GetAddresses(ENodeRole nodeRole, const TMasterYPathProxy::TRspGetClusterMetaPtr& rsp)
{
    switch (nodeRole) {
        case ENodeRole::MasterCache:
            return FromProto<std::vector<TString>>(rsp->master_cache_node_addresses());
        case ENodeRole::TimestampProvider:
            return FromProto<std::vector<TString>>(rsp->timestamp_provider_node_addresses());
        default:
            YT_ABORT();
    }
}

EMasterChannelKind GetChannelKind(ENodeRole nodeRole)
{
    switch (nodeRole) {
        case ENodeRole::MasterCache:
            return EMasterChannelKind::SecondLevelCache;
        case ENodeRole::TimestampProvider:
            return EMasterChannelKind::Cache;
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TNodeAddressesProvider
    : public IRoamingChannelProvider
{
public:
    TNodeAddressesProvider(
        TDuration syncPeriod,
        TWeakPtr<TCellDirectory> cellDirectory,
        ENodeRole nodeRole,
        const TCallback<IChannelPtr(const std::vector<TString>&)>& getChannelFromAddresses)
        : CellDirectory_(cellDirectory)
        , NodeRole_(nodeRole)
        , GetChannelFromAddresses_(getChannelFromAddresses)
        , SyncExecutor_(New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetLightInvoker(),
            BIND(&TNodeAddressesProvider::OnSync, MakeWeak(this)),
            syncPeriod))
        , Logger(NYT::NLogging::TLogger(NodeTrackerClientLogger)
            .AddTag("NodeRole: %v", NodeRole_))
        , Channel_(getChannelFromAddresses({}))
    {
        SyncExecutor_->Start();
    }

    virtual const TString& GetEndpointDescription() const override
    {
        return Channel_.Load()->GetEndpointDescription();
    }

    virtual const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return Channel_.Load()->GetEndpointAttributes();
    }

    virtual TNetworkId GetNetworkId() const override
    {
        return Channel_.Load()->GetNetworkId();
    }

    virtual TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /* request */) override
    {
        return MakeFuture(Channel_.Load());
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        return Channel_.Load()->Terminate(error);
    }

private:
    const TWeakPtr<TCellDirectory> CellDirectory_;
    const ENodeRole NodeRole_;
    const TCallback<IChannelPtr(const std::vector<TString>&)> GetChannelFromAddresses_;
    const TPeriodicExecutorPtr SyncExecutor_;
    const NYT::NLogging::TLogger Logger;

    TAtomicObject<IChannelPtr> Channel_;
    std::vector<TString> Addresses_;

    void OnSync()
    {
        try {
            auto cellDirectory = CellDirectory_.Lock();
            if (!cellDirectory) {
                return;
            }

            YT_LOG_DEBUG("Started updating node list");

            auto channel = cellDirectory->GetMasterChannelOrThrow(GetChannelKind(NodeRole_), cellDirectory->GetPrimaryMasterCellId());
            TObjectServiceProxy proxy(channel);

            auto req = TMasterYPathProxy::GetClusterMeta();
            switch (NodeRole_) {
                case ENodeRole::MasterCache: {
                    req->set_populate_master_cache_node_addresses(true);
                    break;
                }
                case ENodeRole::TimestampProvider: {
                    req->set_populate_timestamp_provider_node_addresses(true);
                    break;
                }
                default:
                    YT_ABORT();
            }

            // TODO(aleksandra-zh): think of a better way
            TGetClusterMetaOptions options;
            auto* cachingHeaderExt = req->Header().MutableExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext);
            cachingHeaderExt->set_success_expiration_time(ToProto<i64>(options.ExpireAfterSuccessfulUpdateTime));
            cachingHeaderExt->set_failure_expiration_time(ToProto<i64>(options.ExpireAfterFailedUpdateTime));

            auto rsp = WaitFor(proxy.Execute(req)).ValueOrThrow();
            auto newAddresses = GetAddresses(NodeRole_, rsp);

            YT_LOG_DEBUG("Received node list (Addresses: %v)", newAddresses);

            if (Addresses_ == newAddresses) {
                YT_LOG_DEBUG("Node list has not changed");
                return;
            }

            YT_LOG_INFO("Node list has been changed (OldAddresses: %v, NewAddresses: %v)",
                Addresses_,
                newAddresses);

            Addresses_ = std::move(newAddresses);
            Channel_.Store(GetChannelFromAddresses_(Addresses_));

            YT_LOG_DEBUG("Finished updating node list");
        } catch (const std::exception& ex) {
            YT_LOG_INFO(ex, "Failed updating node list");
        }
    }
};

IChannelPtr CreateNodeAddressesChannel(
    TDuration syncPeriod,
    TWeakPtr<TCellDirectory> cellDirectory,
    ENodeRole nodeRole,
    const TCallback<IChannelPtr(const std::vector<TString>&)>& getChannelFromAddresses)
{
    return CreateRoamingChannel(New<TNodeAddressesProvider>(
        syncPeriod,
        cellDirectory,
        nodeRole,
        getChannelFromAddresses));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
