#pragma once

#include "public.h"

#include <yt/yt/server/node/exec_agent/public.h>

#include <yt/yt/server/node/cellar_node/public.h>

#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/server/node/job_agent/public.h>

#include <yt/yt/server/node/query_agent/public.h>

#include <yt/yt/server/node/tablet_node/public.h>

#include <yt/yt/server/node/chaos_node/public.h>

#include <yt/yt/server/lib/cellar_agent/public.h>

#include <yt/yt/server/lib/containers/public.h>

#include <yt/yt/server/lib/job_proxy/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/misc/public.h>

#include <yt/yt/library/profiling/solomon/exporter.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/ytlib/query_client/public.h>

#include <yt/yt/ytlib/monitoring/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

//! Common base for all node bootstraps.
struct IBootstrapBase
{
    virtual ~IBootstrapBase() = default;

    // Resourse management.
    virtual const TNodeMemoryTrackerPtr& GetMemoryUsageTracker() const = 0;
    virtual const TNodeResourceManagerPtr& GetNodeResourceManager() const = 0;

    // Total throttlers.
    virtual const NConcurrency::IThroughputThrottlerPtr& GetTotalInThrottler() const = 0;
    virtual const NConcurrency::IThroughputThrottlerPtr& GetTotalOutThrottler() const = 0;
    virtual const NConcurrency::IThroughputThrottlerPtr& GetReadRpsOutThrottler() const = 0;

    // Config stuff.
    virtual const TClusterNodeConfigPtr& GetConfig() const = 0;
    virtual const NClusterNode::TClusterNodeDynamicConfigManagerPtr& GetDynamicConfigManager() const = 0;

    // Common invokers.
    virtual const IInvokerPtr& GetControlInvoker() const = 0;
    virtual const IInvokerPtr& GetJobInvoker() const = 0;
    virtual const IInvokerPtr& GetMasterConnectionInvoker() const = 0;
    virtual const IInvokerPtr& GetStorageLightInvoker() const = 0;
    virtual const IPrioritizedInvokerPtr& GetStorageHeavyInvoker() const = 0;

    // Master connection stuff.
    virtual const NApi::NNative::IClientPtr& GetMasterClient() const = 0;
    virtual const NApi::NNative::IConnectionPtr& GetMasterConnection() const = 0;
    virtual NRpc::IChannelPtr GetMasterChannel(NObjectClient::TCellTag cellTag) = 0;

    virtual NNodeTrackerClient::TNodeDescriptor GetLocalDescriptor() const = 0;

    virtual NObjectClient::TCellId GetCellId() const = 0;
    virtual NObjectClient::TCellId GetCellId(NObjectClient::TCellTag cellTag) const = 0;
    virtual const NObjectClient::TCellTagList& GetMasterCellTags() const = 0;
    virtual std::vector<TString> GetMasterAddressesOrThrow(NObjectClient::TCellTag cellTag) const = 0;

    virtual const NDataNode::TLegacyMasterConnectorPtr& GetLegacyMasterConnector() const = 0;
    virtual bool UseNewHeartbeats() const = 0;

    virtual void ResetAndRegisterAtMaster() = 0;

    virtual bool IsConnected() const = 0;
    virtual NNodeTrackerClient::TNodeId GetNodeId() const = 0;

    DECLARE_INTERFACE_SIGNAL(void(NNodeTrackerClient::TNodeId nodeId), MasterConnected);
    DECLARE_INTERFACE_SIGNAL(void(), MasterDisconnected);

    // Node directory.
    virtual const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() const = 0;

    // Network stuff.
    virtual NNodeTrackerClient::TNetworkPreferenceList GetLocalNetworks() const = 0;
    virtual std::optional<TString> GetDefaultNetworkName() const = 0;
    virtual TString GetDefaultLocalAddressOrThrow() const = 0;

    // Servers.
    virtual const NHttp::IServerPtr& GetHttpServer() const = 0;
    virtual const NRpc::IServerPtr& GetRpcServer() const = 0;

    // Common node caches.
    virtual const NChunkClient::IBlockCachePtr& GetBlockCache() const = 0;
    virtual const NChunkClient::IClientBlockCachePtr& GetClientBlockCache() const = 0;
    virtual const NDataNode::IChunkMetaManagerPtr& GetChunkMetaManager() const = 0;
    virtual const NTabletNode::IVersionedChunkMetaManagerPtr& GetVersionedChunkMetaManager() const = 0;

    // Orchid.
    virtual const NYTree::IMapNodePtr& GetOrchidRoot() const = 0;

    // Global node state.
    virtual bool IsReadOnly() const = 0;
    virtual bool Decommissioned() const = 0;

    // Alerts.
    virtual void RegisterStaticAlert(const TError& alert) = 0;

    DECLARE_INTERFACE_SIGNAL(void(std::vector<TError>* alerts), PopulateAlerts);

    // Network statistics.
    virtual NDataNode::TNetworkStatistics& GetNetworkStatistics() const = 0;

    // Global chunk management.
    virtual const NDataNode::IChunkRegistryPtr& GetChunkRegistry() const = 0;

    virtual const NDataNode::IBlobReaderCachePtr& GetBlobReaderCache() const = 0;

    // Job controller.
    virtual const NJobAgent::TJobControllerPtr& GetJobController() const = 0;

    // Job environment.
    virtual NExecAgent::EJobEnvironmentType GetJobEnvironmentType() const = 0;

    // Node flavors accessors.
    virtual const THashSet<NNodeTrackerClient::ENodeFlavor>& GetFlavors() const = 0;

    virtual bool IsDataNode() const = 0;
    virtual bool IsExecNode() const = 0;
    virtual bool IsCellarNode() const = 0;
    virtual bool IsTabletNode() const = 0;
    virtual bool IsChaosNode() const = 0;

    // Per-flavor node bootstraps.
    virtual NCellarNode::IBootstrap* GetCellarNodeBootstrap() const = 0;
    virtual NDataNode::IBootstrap* GetDataNodeBootstrap() const = 0;
    virtual NExecAgent::IBootstrap* GetExecNodeBootstrap() const = 0;
    virtual NChaosNode::IBootstrap* GetChaosNodeBootstrap() const = 0;
    virtual NTabletNode::IBootstrap* GetTabletNodeBootstrap() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
    : public IBootstrapBase
{
    virtual void Initialize() = 0;
    virtual void Run() = 0;

    virtual void ValidateSnapshot(const TString& fileName) = 0;

    virtual const IMasterConnectorPtr& GetMasterConnector() const = 0;

    virtual NConcurrency::TRelativeThroughputThrottlerConfigPtr PatchRelativeNetworkThrottlerConfig(
        const NConcurrency::TRelativeThroughputThrottlerConfigPtr& config) const = 0;

    virtual void SetDecommissioned(bool decommissioned) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(TClusterNodeConfigPtr config, NYTree::INodePtr configNode);

////////////////////////////////////////////////////////////////////////////////

class TBootstrapBase
    : public virtual IBootstrapBase
{
public:
    DEFINE_SIGNAL(void(NNodeTrackerClient::TNodeId nodeId), MasterConnected);
    DEFINE_SIGNAL(void(), MasterDisconnected);
    DEFINE_SIGNAL(void(std::vector<TError>* alerts), PopulateAlerts);

public:
    explicit TBootstrapBase(IBootstrapBase* bootstrap);

    virtual const TNodeMemoryTrackerPtr& GetMemoryUsageTracker() const override;
    virtual const TNodeResourceManagerPtr& GetNodeResourceManager() const override;

    virtual const NConcurrency::IThroughputThrottlerPtr& GetTotalInThrottler() const override;
    virtual const NConcurrency::IThroughputThrottlerPtr& GetTotalOutThrottler() const override;
    virtual const NConcurrency::IThroughputThrottlerPtr& GetReadRpsOutThrottler() const override;

    virtual const TClusterNodeConfigPtr& GetConfig() const override;
    virtual const NClusterNode::TClusterNodeDynamicConfigManagerPtr& GetDynamicConfigManager() const override;

    virtual const IInvokerPtr& GetControlInvoker() const override;
    virtual const IInvokerPtr& GetJobInvoker() const override;
    virtual const IInvokerPtr& GetMasterConnectionInvoker() const override;
    virtual const IInvokerPtr& GetStorageLightInvoker() const override;
    virtual const IPrioritizedInvokerPtr& GetStorageHeavyInvoker() const override;

    virtual const NApi::NNative::IClientPtr& GetMasterClient() const override;
    virtual const NApi::NNative::IConnectionPtr& GetMasterConnection() const override;
    virtual NRpc::IChannelPtr GetMasterChannel(NObjectClient::TCellTag cellTag) override;

    virtual NNodeTrackerClient::TNodeDescriptor GetLocalDescriptor() const override;

    virtual NObjectClient::TCellId GetCellId() const override;
    virtual NObjectClient::TCellId GetCellId(NObjectClient::TCellTag cellTag) const override;
    virtual const NObjectClient::TCellTagList& GetMasterCellTags() const override;
    virtual std::vector<TString> GetMasterAddressesOrThrow(NObjectClient::TCellTag cellTag) const override;

    virtual const NDataNode::TLegacyMasterConnectorPtr& GetLegacyMasterConnector() const override;
    virtual bool UseNewHeartbeats() const override;

    virtual void ResetAndRegisterAtMaster() override;

    virtual bool IsConnected() const override;
    virtual NNodeTrackerClient::TNodeId GetNodeId() const override;

    virtual const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() const override;

    virtual NNodeTrackerClient::TNetworkPreferenceList GetLocalNetworks() const override;
    virtual std::optional<TString> GetDefaultNetworkName() const override;
    virtual TString GetDefaultLocalAddressOrThrow() const override;

    virtual const NHttp::IServerPtr& GetHttpServer() const override;
    virtual const NRpc::IServerPtr& GetRpcServer() const override;

    virtual const NChunkClient::IBlockCachePtr& GetBlockCache() const override;
    virtual const NChunkClient::IClientBlockCachePtr& GetClientBlockCache() const override;
    virtual const NDataNode::IChunkMetaManagerPtr& GetChunkMetaManager() const override;
    virtual const NTabletNode::IVersionedChunkMetaManagerPtr& GetVersionedChunkMetaManager() const override;

    virtual const NYTree::IMapNodePtr& GetOrchidRoot() const override;

    virtual bool IsReadOnly() const override;
    virtual bool Decommissioned() const override;

    virtual void RegisterStaticAlert(const TError& alert) override;

    virtual NDataNode::TNetworkStatistics& GetNetworkStatistics() const override;

    virtual const NDataNode::IChunkRegistryPtr& GetChunkRegistry() const override;

    virtual const NDataNode::IBlobReaderCachePtr& GetBlobReaderCache() const override;

    virtual const NJobAgent::TJobControllerPtr& GetJobController() const override;

    virtual NExecAgent::EJobEnvironmentType GetJobEnvironmentType() const override;

    virtual const THashSet<NNodeTrackerClient::ENodeFlavor>& GetFlavors() const override;

    virtual bool IsDataNode() const override;
    virtual bool IsExecNode() const override;
    virtual bool IsCellarNode() const override;
    virtual bool IsTabletNode() const override;
    virtual bool IsChaosNode() const override;

    virtual NCellarNode::IBootstrap* GetCellarNodeBootstrap() const override;
    virtual NDataNode::IBootstrap* GetDataNodeBootstrap() const override;
    virtual NExecAgent::IBootstrap* GetExecNodeBootstrap() const override;
    virtual NChaosNode::IBootstrap* GetChaosNodeBootstrap() const override;
    virtual NTabletNode::IBootstrap* GetTabletNodeBootstrap() const override;

private:
    IBootstrapBase* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
