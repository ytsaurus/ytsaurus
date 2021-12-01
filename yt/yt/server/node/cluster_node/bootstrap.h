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

    // Orchid.
    virtual const NYTree::IMapNodePtr& GetOrchidRoot() const = 0;

    // Global node state.
    virtual bool IsReadOnly() const = 0;
    virtual bool Decommissioned() const = 0;

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

    virtual NConcurrency::TRelativeThroughputThrottlerConfigPtr PatchRelativeNetworkThrottlerConfig(
        const NConcurrency::TRelativeThroughputThrottlerConfigPtr& config) const = 0;

    virtual void SetDecommissioned(bool decommissioned) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(NNodeTrackerClient::TNodeId nodeId), MasterConnected);
    DEFINE_SIGNAL_OVERRIDE(void(), MasterDisconnected);
    DEFINE_SIGNAL_OVERRIDE(void(std::vector<TError>* alerts), PopulateAlerts);

public:
    TBootstrap(TClusterNodeConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    // IBootstrap implementation.
    void Initialize() override;
    void Run() override;
    void ValidateSnapshot(const TString& fileName) override;
    const IMasterConnectorPtr& GetMasterConnector() const override;
    NConcurrency::TRelativeThroughputThrottlerConfigPtr PatchRelativeNetworkThrottlerConfig(
        const NConcurrency::TRelativeThroughputThrottlerConfigPtr& config) const override;
    void SetDecommissioned(bool decommissioned) override;

    // IBootstrapBase implementation.
    const TNodeMemoryTrackerPtr& GetMemoryUsageTracker() const override;
    const TNodeResourceManagerPtr& GetNodeResourceManager() const override;
    const NConcurrency::IThroughputThrottlerPtr& GetTotalInThrottler() const override;
    const NConcurrency::IThroughputThrottlerPtr& GetTotalOutThrottler() const override;
    const NConcurrency::IThroughputThrottlerPtr& GetReadRpsOutThrottler() const override;
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
    const NYTree::IMapNodePtr& GetOrchidRoot() const override;
    bool IsReadOnly() const override;
    bool Decommissioned() const override;
    NDataNode::TNetworkStatistics& GetNetworkStatistics() const override;
    const NDataNode::IChunkRegistryPtr& GetChunkRegistry() const override;
    const NDataNode::IBlobReaderCachePtr& GetBlobReaderCache() const override;
    const NJobAgent::TJobControllerPtr& GetJobController() const override;
    NExecNode::EJobEnvironmentType GetJobEnvironmentType() const override;
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

protected:
    const TClusterNodeConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;

    NConcurrency::TActionQueuePtr ControlActionQueue_;
    NConcurrency::TActionQueuePtr JobActionQueue_;
    NConcurrency::TThreadPoolPtr ConnectionThreadPool_;
    NConcurrency::TThreadPoolPtr StorageLightThreadPool_;
    NConcurrency::TThreadPoolPtr StorageHeavyThreadPool_;
    IPrioritizedInvokerPtr StorageHeavyInvoker_;
    NConcurrency::TActionQueuePtr MasterCacheQueue_;

    ICoreDumperPtr CoreDumper_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;

    NYT::NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;

    NYTree::IMapNodePtr OrchidRoot_;

    TNodeMemoryTrackerPtr MemoryUsageTracker_;
    TNodeResourceManagerPtr NodeResourceManager_;

    NConcurrency::IReconfigurableThroughputThrottlerPtr RawTotalInThrottler_;
    NConcurrency::IThroughputThrottlerPtr TotalInThrottler_;

    NConcurrency::IReconfigurableThroughputThrottlerPtr RawTotalOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr TotalOutThrottler_;

    NConcurrency::IReconfigurableThroughputThrottlerPtr RawReadRpsOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr ReadRpsOutThrottler_;

    NContainers::TInstanceLimitsTrackerPtr InstanceLimitsTracker_;

    TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;

    NApi::NNative::IClientPtr MasterClient_;
    NApi::NNative::IConnectionPtr MasterConnection_;

    NJobAgent::TJobControllerPtr JobController_;

    IMasterConnectorPtr MasterConnector_;
    NDataNode::TLegacyMasterConnectorPtr LegacyMasterConnector_;

    NChunkClient::IBlockCachePtr BlockCache_;
    NChunkClient::IClientBlockCachePtr ClientBlockCache_;

    std::unique_ptr<NDataNode::TNetworkStatistics> NetworkStatistics_;

    NDataNode::IChunkMetaManagerPtr ChunkMetaManager_;
    NTabletNode::IVersionedChunkMetaManagerPtr VersionedChunkMetaManager_;

    NDataNode::IChunkRegistryPtr ChunkRegistry_;
    NDataNode::IBlobReaderCachePtr BlobReaderCache_;

    TError UnrecognizedOptionsAlert_;

    NObjectClient::TObjectServiceCachePtr ObjectServiceCache_;
    std::vector<NObjectClient::ICachingObjectServicePtr> CachingObjectServices_;

    THashSet<NNodeTrackerClient::ENodeFlavor> Flavors_;

    std::unique_ptr<NCellarNode::IBootstrap> CellarNodeBootstrap_;
    std::unique_ptr<NChaosNode::IBootstrap> ChaosNodeBootstrap_;
    std::unique_ptr<NExecNode::IBootstrap> ExecNodeBootstrap_;
    std::unique_ptr<NDataNode::IBootstrap> DataNodeBootstrap_;
    std::unique_ptr<NTabletNode::IBootstrap> TabletNodeBootstrap_;

    bool Decommissioned_ = false;

    void DoInitialize();
    void DoRun();
    void DoValidateConfig();
    void DoValidateSnapshot(const TString& fileName);
    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldConfig*/,
        const TClusterNodeDynamicConfigPtr& newConfig);
    void PopulateAlerts(std::vector<TError>* alerts);
    void OnMasterConnected(NNodeTrackerClient::TNodeId nodeId);
    void OnMasterDisconnected();
};

std::unique_ptr<IBootstrap> CreateBootstrap(TClusterNodeConfigPtr config, NYTree::INodePtr configNode);

////////////////////////////////////////////////////////////////////////////////

class TBootstrapForward
    : public virtual IBootstrapBase
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(NNodeTrackerClient::TNodeId nodeId), MasterConnected);
    DEFINE_SIGNAL_OVERRIDE(void(), MasterDisconnected);
    DEFINE_SIGNAL_OVERRIDE(void(std::vector<TError>* alerts), PopulateAlerts);

public:
    explicit TBootstrapForward(IBootstrapBase* bootstrap);

    const TNodeMemoryTrackerPtr& GetMemoryUsageTracker() const override;
    const TNodeResourceManagerPtr& GetNodeResourceManager() const override;

    const NConcurrency::IThroughputThrottlerPtr& GetTotalInThrottler() const override;
    const NConcurrency::IThroughputThrottlerPtr& GetTotalOutThrottler() const override;
    const NConcurrency::IThroughputThrottlerPtr& GetReadRpsOutThrottler() const override;

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

    const NYTree::IMapNodePtr& GetOrchidRoot() const override;

    bool IsReadOnly() const override;
    bool Decommissioned() const override;

    NDataNode::TNetworkStatistics& GetNetworkStatistics() const override;

    const NDataNode::IChunkRegistryPtr& GetChunkRegistry() const override;

    const NDataNode::IBlobReaderCachePtr& GetBlobReaderCache() const override;

    const NJobAgent::TJobControllerPtr& GetJobController() const override;

    NExecNode::EJobEnvironmentType GetJobEnvironmentType() const override;

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
