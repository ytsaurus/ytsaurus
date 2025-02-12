#pragma once

#include "public.h"

#include <yt/yt/server/node/exec_node/public.h>

#include <yt/yt/server/node/cellar_node/public.h>

#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/server/node/job_agent/public.h>

#include <yt/yt/server/node/query_agent/public.h>

#include <yt/yt/server/node/tablet_node/public.h>

#include <yt/yt/server/node/chaos_node/public.h>

#include <yt/yt/server/lib/misc/restart_manager.h>

#include <yt/yt/server/lib/cellar_agent/public.h>

#include <yt/yt/server/lib/job_proxy/public.h>

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/server/lib/misc/bootstrap.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/cell_master_client/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/misc/public.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/yt/library/profiling/solomon/public.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/library/monitoring/public.h>

#include <yt/yt/library/fusion/public.h>

#include <yt/yt/library/disk_manager/public.h>

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
    : public virtual TRefCounted
{
    // Resource management.
    virtual const INodeMemoryTrackerPtr& GetNodeMemoryUsageTracker() const = 0;
    virtual const TNodeResourceManagerPtr& GetNodeResourceManager() const = 0;

    virtual const NConcurrency::IThroughputThrottlerPtr& GetDefaultInThrottler() const = 0;
    virtual const NConcurrency::IThroughputThrottlerPtr& GetDefaultOutThrottler() const = 0;

    virtual const NConcurrency::IThroughputThrottlerPtr& GetReadRpsOutThrottler() const = 0;
    virtual const NConcurrency::IThroughputThrottlerPtr& GetAnnounceChunkReplicaRpsOutThrottler() const = 0;

    virtual const NConcurrency::IThroughputThrottlerPtr& GetUserJobContainerCreationThrottler() const = 0;

    virtual const NProfiling::TBufferedProducerPtr& GetBufferedProducer() const = 0;

    // Config stuff.
    virtual const TClusterNodeBootstrapConfigPtr& GetConfig() const = 0;
    virtual const NClusterNode::TClusterNodeDynamicConfigManagerPtr& GetDynamicConfigManager() const = 0;
    virtual const NCellarNode::TBundleDynamicConfigManagerPtr& GetBundleDynamicConfigManager() const = 0;

    // Common invokers.
    virtual const IInvokerPtr& GetControlInvoker() const = 0;
    virtual const IInvokerPtr& GetJobInvoker() const = 0;
    virtual const IInvokerPtr& GetMasterConnectionInvoker() const = 0;
    virtual const IInvokerPtr& GetStorageLightInvoker() const = 0;
    virtual const IPrioritizedInvokerPtr& GetStorageHeavyInvoker() const = 0;

    // Master connection stuff.
    virtual const NApi::NNative::IClientPtr& GetClient() const = 0;
    virtual const NApi::NNative::IConnectionPtr& GetConnection() const = 0;
    virtual NRpc::IChannelPtr GetMasterChannel(NObjectClient::TCellTag cellTag) = 0;
    virtual const NRpc::IAuthenticatorPtr& GetNativeAuthenticator() const = 0;

    virtual NNodeTrackerClient::TNodeDescriptor GetLocalDescriptor() const = 0;

    virtual NObjectClient::TCellId GetCellId() const = 0;
    virtual NObjectClient::TCellId GetCellId(NObjectClient::TCellTag cellTag) const = 0;
    virtual std::vector<std::string> GetMasterAddressesOrThrow(NObjectClient::TCellTag cellTag) const = 0;

    virtual void ResetAndRegisterAtMaster() = 0;

    virtual bool IsConnected() const = 0;
    virtual NNodeTrackerClient::TNodeId GetNodeId() const = 0;
    virtual std::string GetLocalHostName() const = 0;
    virtual TMasterEpoch GetMasterEpoch() const = 0;

    DECLARE_INTERFACE_SIGNAL(void(NNodeTrackerClient::TNodeId nodeId), MasterConnected);
    DECLARE_INTERFACE_SIGNAL(void(), MasterDisconnected);
    DECLARE_INTERFACE_SIGNAL(void(const NCellMasterClient::TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs), SecondaryMasterCellListChanged);
    DECLARE_INTERFACE_SIGNAL(void(const NCellMasterClient::TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs), ReadyToUpdateHeartbeatStream);

    // Node directory.
    virtual const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() const = 0;

    // Network stuff.
    virtual NNodeTrackerClient::TNetworkPreferenceList GetNetworks() const = 0;
    virtual NNodeTrackerClient::TNetworkPreferenceList GetLocalNetworks() const = 0;

    // Servers.
    virtual const NHttp::IServerPtr& GetHttpServer() const = 0;
    virtual const NRpc::IServerPtr& GetRpcServer() const = 0;

    // Block tracker.
    virtual const IMemoryUsageTrackerPtr& GetRpcMemoryUsageTracker() const = 0;
    virtual const IMemoryUsageTrackerPtr& GetReadBlockMemoryUsageTracker() const = 0;
    virtual const IMemoryUsageTrackerPtr& GetSystemJobsMemoryUsageTracker() const = 0;

    // Common node caches.
    virtual const NChunkClient::IBlockCachePtr& GetBlockCache() const = 0;
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

    // Job resource manager.
    virtual const NJobAgent::TJobResourceManagerPtr& GetJobResourceManager() const = 0;

    // Restart manager for hot swap functionality.
    virtual const NServer::TRestartManagerPtr& GetRestartManager() const = 0;

    // Job environment.
    virtual NJobProxy::EJobEnvironmentType GetJobEnvironmentType() const = 0;

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
    virtual const NClusterNode::IBootstrap* GetClusterNodeBootstrap() const = 0;

    // COMPAT(gritukan)
    virtual bool NeedDataNodeBootstrap() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
    : public IBootstrapBase
    , public NServer::IDaemonBootstrap
{
    virtual void Initialize() = 0;

    virtual const IMasterConnectorPtr& GetMasterConnector() const = 0;

    virtual NConcurrency::IThroughputThrottlerPtr GetInThrottler(const TString& bucket) = 0;
    virtual NConcurrency::IThroughputThrottlerPtr GetOutThrottler(const TString& bucket) = 0;

    virtual NDiskManager::IHotswapManagerPtr TryGetHotswapManager() const = 0;

    virtual NConcurrency::TRelativeThroughputThrottlerConfigPtr PatchRelativeNetworkThrottlerConfig(
        const NConcurrency::TRelativeThroughputThrottlerConfigPtr& config) const = 0;

    virtual void SetDecommissioned(bool decommissioned) = 0;

    //! One-shot. Then No-Op.
    virtual void CompleteNodeRegistration() = 0;
};

DEFINE_REFCOUNTED_TYPE(IBootstrap)

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateNodeBootstrap(
    TClusterNodeBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator);

////////////////////////////////////////////////////////////////////////////////

class TBootstrapBase
    : public virtual IBootstrapBase
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(NNodeTrackerClient::TNodeId nodeId), MasterConnected);
    DEFINE_SIGNAL_OVERRIDE(void(), MasterDisconnected);
    DEFINE_SIGNAL_OVERRIDE(void(std::vector<TError>* alerts), PopulateAlerts);
    DEFINE_SIGNAL_OVERRIDE(void(const NCellMasterClient::TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs), SecondaryMasterCellListChanged);
    DEFINE_SIGNAL_OVERRIDE(void(const NCellMasterClient::TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs), ReadyToUpdateHeartbeatStream);

public:
    explicit TBootstrapBase(IBootstrapBase* bootstrap);

    const INodeMemoryTrackerPtr& GetNodeMemoryUsageTracker() const override;
    const TNodeResourceManagerPtr& GetNodeResourceManager() const override;

    const NConcurrency::IThroughputThrottlerPtr& GetDefaultInThrottler() const override;
    const NConcurrency::IThroughputThrottlerPtr& GetDefaultOutThrottler() const override;

    const NConcurrency::IThroughputThrottlerPtr& GetReadRpsOutThrottler() const override;
    const NConcurrency::IThroughputThrottlerPtr& GetAnnounceChunkReplicaRpsOutThrottler() const override;

    const NConcurrency::IThroughputThrottlerPtr& GetUserJobContainerCreationThrottler() const override;

    const NProfiling::TBufferedProducerPtr& GetBufferedProducer() const override;

    const TClusterNodeBootstrapConfigPtr& GetConfig() const override;
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr& GetDynamicConfigManager() const override;
    const NCellarNode::TBundleDynamicConfigManagerPtr& GetBundleDynamicConfigManager() const override;

    const IInvokerPtr& GetControlInvoker() const override;
    const IInvokerPtr& GetJobInvoker() const override;
    const IInvokerPtr& GetMasterConnectionInvoker() const override;
    const IInvokerPtr& GetStorageLightInvoker() const override;
    const IPrioritizedInvokerPtr& GetStorageHeavyInvoker() const override;

    const NApi::NNative::IClientPtr& GetClient() const override;
    const NApi::NNative::IConnectionPtr& GetConnection() const override;
    NRpc::IChannelPtr GetMasterChannel(NObjectClient::TCellTag cellTag) override;
    const NRpc::IAuthenticatorPtr& GetNativeAuthenticator() const override;

    NNodeTrackerClient::TNodeDescriptor GetLocalDescriptor() const override;

    NObjectClient::TCellId GetCellId() const override;
    NObjectClient::TCellId GetCellId(NObjectClient::TCellTag cellTag) const override;
    std::vector<std::string> GetMasterAddressesOrThrow(NObjectClient::TCellTag cellTag) const override;

    void ResetAndRegisterAtMaster() override;

    bool IsConnected() const override;
    NNodeTrackerClient::TNodeId GetNodeId() const override;
    std::string GetLocalHostName() const override;
    TMasterEpoch GetMasterEpoch() const override;

    const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() const override;

    NNodeTrackerClient::TNetworkPreferenceList GetNetworks() const override;
    NNodeTrackerClient::TNetworkPreferenceList GetLocalNetworks() const override;

    const NHttp::IServerPtr& GetHttpServer() const override;
    const NRpc::IServerPtr& GetRpcServer() const override;

    const IMemoryUsageTrackerPtr& GetRpcMemoryUsageTracker() const override;
    const IMemoryUsageTrackerPtr& GetReadBlockMemoryUsageTracker() const override;
    const IMemoryUsageTrackerPtr& GetSystemJobsMemoryUsageTracker() const override;

    const NChunkClient::IBlockCachePtr& GetBlockCache() const override;
    const NDataNode::IChunkMetaManagerPtr& GetChunkMetaManager() const override;
    const NTabletNode::IVersionedChunkMetaManagerPtr& GetVersionedChunkMetaManager() const override;
    const NDataNode::TChunkReaderSweeperPtr& GetChunkReaderSweeper() const override;

    const NYTree::IMapNodePtr& GetOrchidRoot() const override;

    bool IsReadOnly() const override;
    bool IsDecommissioned() const override;

    NDataNode::TNetworkStatistics& GetNetworkStatistics() const override;

    const NDataNode::IChunkRegistryPtr& GetChunkRegistry() const override;

    const NDataNode::IBlobReaderCachePtr& GetBlobReaderCache() const override;

    const NJobAgent::TJobResourceManagerPtr& GetJobResourceManager() const override;

    const NServer::TRestartManagerPtr& GetRestartManager() const override;

    NJobProxy::EJobEnvironmentType GetJobEnvironmentType() const override;

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
    const NClusterNode::IBootstrap* GetClusterNodeBootstrap() const override;

    bool NeedDataNodeBootstrap() const override;

private:
    IBootstrapBase* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
