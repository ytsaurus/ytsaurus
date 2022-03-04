#pragma once

#include "public.h"

#include <yt/yt/server/node/exec_node/public.h>

#include <yt/yt/server/node/cellar_node/public.h>

#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/server/node/job_agent/public.h>

#include <yt/yt/server/node/query_agent/public.h>

#include <yt/yt/server/node/tablet_node/public.h>

#include <yt/yt/server/node/chaos_node/public.h>

#include <yt/yt/server/lib/cellar_agent/public.h>

#include <yt/yt/server/lib/containers/public.h>

#include <yt/yt/server/lib/job_proxy/public.h>

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/misc/public.h>

#include <yt/yt/library/profiling/solomon/public.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/ytlib/query_client/public.h>

#include <yt/yt/ytlib/monitoring/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/concurrency/public.h>

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

    virtual const NConcurrency::IThroughputThrottlerPtr& GetDefaultInThrottler() const = 0;
    virtual const NConcurrency::IThroughputThrottlerPtr& GetDefaultOutThrottler() const = 0;

    virtual const NConcurrency::IThroughputThrottlerPtr& GetReadRpsOutThrottler() const = 0;
    virtual const NConcurrency::IThroughputThrottlerPtr& GetAnnounceChunkReplicaRpsOutThrottler() const = 0;

    virtual const NProfiling::TBufferedProducerPtr& GetBufferedProducer() const = 0;

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
    virtual TString GetLocalHostName() const = 0;
    virtual TMasterEpoch GetMasterEpoch() const = 0;

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
    virtual const NDataNode::TChunkReaderSweeperPtr& GetChunkReaderSweeper() const = 0;

    // Orchid.
    virtual const NYTree::IMapNodePtr& GetOrchidRoot() const = 0;

    // Global node state.
    virtual bool IsReadOnly() const = 0;
    virtual bool IsDecommissioned() const = 0;

    // Alerts.
    DECLARE_INTERFACE_SIGNAL(void(std::vector<TError>* alerts), PopulateAlerts);

    // Network statistics.
    virtual NDataNode::TNetworkStatistics& GetNetworkStatistics() const = 0;

    // Global chunk management.
    virtual const NDataNode::IChunkRegistryPtr& GetChunkRegistry() const = 0;

    virtual const NDataNode::IBlobReaderCachePtr& GetBlobReaderCache() const = 0;

    // Job controller.
    virtual const NJobAgent::TJobControllerPtr& GetJobController() const = 0;

    // Job environment.
    virtual NExecNode::EJobEnvironmentType GetJobEnvironmentType() const = 0;

    // IO tracker.
    virtual const NIO::IIOTrackerPtr& GetIOTracker() const = 0;

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
    virtual NExecNode::IBootstrap* GetExecNodeBootstrap() const = 0;
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

    virtual NConcurrency::IThroughputThrottlerPtr GetInThrottler(const TString& bucket) = 0;
    virtual NConcurrency::IThroughputThrottlerPtr GetOutThrottler(const TString& bucket) = 0;

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
    DEFINE_SIGNAL_OVERRIDE(void(NNodeTrackerClient::TNodeId nodeId), MasterConnected);
    DEFINE_SIGNAL_OVERRIDE(void(), MasterDisconnected);
    DEFINE_SIGNAL_OVERRIDE(void(std::vector<TError>* alerts), PopulateAlerts);

public:
    explicit TBootstrapBase(IBootstrapBase* bootstrap);

    const TNodeMemoryTrackerPtr& GetMemoryUsageTracker() const override;
    const TNodeResourceManagerPtr& GetNodeResourceManager() const override;

    const NConcurrency::IThroughputThrottlerPtr& GetDefaultInThrottler() const override;
    const NConcurrency::IThroughputThrottlerPtr& GetDefaultOutThrottler() const override;

    const NConcurrency::IThroughputThrottlerPtr& GetReadRpsOutThrottler() const override;
    const NConcurrency::IThroughputThrottlerPtr& GetAnnounceChunkReplicaRpsOutThrottler() const override;

    const NProfiling::TBufferedProducerPtr& GetBufferedProducer() const override;

    const TClusterNodeConfigPtr& GetConfig() const override;
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr& GetDynamicConfigManager() const override;

    const IInvokerPtr& GetControlInvoker() const override;
    const IInvokerPtr& GetJobInvoker() const override;
    const IInvokerPtr& GetMasterConnectionInvoker() const override;
    const IInvokerPtr& GetStorageLightInvoker() const override;
    const IPrioritizedInvokerPtr& GetStorageHeavyInvoker() const override;

    const NApi::NNative::IClientPtr& GetMasterClient() const override;
    const NApi::NNative::IConnectionPtr& GetMasterConnection() const override;
    NRpc::IChannelPtr GetMasterChannel(NObjectClient::TCellTag cellTag) override;

    NNodeTrackerClient::TNodeDescriptor GetLocalDescriptor() const override;

    NObjectClient::TCellId GetCellId() const override;
    NObjectClient::TCellId GetCellId(NObjectClient::TCellTag cellTag) const override;
    const NObjectClient::TCellTagList& GetMasterCellTags() const override;
    std::vector<TString> GetMasterAddressesOrThrow(NObjectClient::TCellTag cellTag) const override;

    const NDataNode::TLegacyMasterConnectorPtr& GetLegacyMasterConnector() const override;
    bool UseNewHeartbeats() const override;

    void ResetAndRegisterAtMaster() override;

    bool IsConnected() const override;
    NNodeTrackerClient::TNodeId GetNodeId() const override;
    TString GetLocalHostName() const override;
    TMasterEpoch GetMasterEpoch() const override;

    const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() const override;

    NNodeTrackerClient::TNetworkPreferenceList GetLocalNetworks() const override;
    std::optional<TString> GetDefaultNetworkName() const override;
    TString GetDefaultLocalAddressOrThrow() const override;

    const NHttp::IServerPtr& GetHttpServer() const override;
    const NRpc::IServerPtr& GetRpcServer() const override;

    const NChunkClient::IBlockCachePtr& GetBlockCache() const override;
    const NChunkClient::IClientBlockCachePtr& GetClientBlockCache() const override;
    const NDataNode::IChunkMetaManagerPtr& GetChunkMetaManager() const override;
    const NTabletNode::IVersionedChunkMetaManagerPtr& GetVersionedChunkMetaManager() const override;
    const NDataNode::TChunkReaderSweeperPtr& GetChunkReaderSweeper() const override;

    const NYTree::IMapNodePtr& GetOrchidRoot() const override;

    bool IsReadOnly() const override;
    bool IsDecommissioned() const override;

    NDataNode::TNetworkStatistics& GetNetworkStatistics() const override;

    const NDataNode::IChunkRegistryPtr& GetChunkRegistry() const override;

    const NDataNode::IBlobReaderCachePtr& GetBlobReaderCache() const override;

    const NJobAgent::TJobControllerPtr& GetJobController() const override;

    NExecNode::EJobEnvironmentType GetJobEnvironmentType() const override;

    const NIO::IIOTrackerPtr& GetIOTracker() const override;

    const THashSet<NNodeTrackerClient::ENodeFlavor>& GetFlavors() const override;

    bool IsDataNode() const override;
    bool IsExecNode() const override;
    bool IsCellarNode() const override;
    bool IsTabletNode() const override;
    bool IsChaosNode() const override;

    NCellarNode::IBootstrap* GetCellarNodeBootstrap() const override;
    NDataNode::IBootstrap* GetDataNodeBootstrap() const override;
    NExecNode::IBootstrap* GetExecNodeBootstrap() const override;
    NChaosNode::IBootstrap* GetChaosNodeBootstrap() const override;
    NTabletNode::IBootstrap* GetTabletNodeBootstrap() const override;

private:
    IBootstrapBase* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
