#include <gtest/gtest.h>

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/node_resource_manager.h>

#include <yt/yt/server/lib/job_agent/config.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NJobAgent {
namespace {

using namespace NClusterNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

// Minimal mock of NClusterNode::IBootstrap providing what TJobResourceManager
// and TNodeResourceManager need to be constructed and used in tests.
class TJobResourceManagerTestBootstrap
    : public NClusterNode::IBootstrap
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(std::vector<TError>* alerts), PopulateAlerts);
    DEFINE_SIGNAL_OVERRIDE(
        void(const NCellMasterClient::TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs),
        SecondaryMasterCellListChanged);
    DEFINE_SIGNAL_OVERRIDE(
        void(const NCellMasterClient::TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs),
        ReadyToUpdateHeartbeatStream);

    TJobResourceManagerTestBootstrap()
        : ControlActionQueue_(New<TActionQueue>("TestControl"))
        , JobActionQueue_(New<TActionQueue>("TestJob"))
        , ControlInvoker_(ControlActionQueue_->GetInvoker())
        , JobInvoker_(JobActionQueue_->GetInvoker())
    {
        Config_ = New<TClusterNodeBootstrapConfig>();
        Config_->JobResourceManager = New<TJobResourceManagerConfig>();
        Config_->Flavors = {};
        Config_->ResourceLimits = New<NClusterNode::TResourceLimitsConfig>();
        Config_->ResourceLimits->TotalMemory = 1_GB;
        Config_->ResourceLimitsUpdatePeriod = TDuration::Hours(1);
        Config_->NetworkBandwidth = 1_GB;

        MemoryTracker_ = CreateNodeMemoryTracker(
            /*totalLimit*/ 1_GB,
            New<TNodeMemoryTrackerConfig>());

        DynamicConfigManager_ = New<TClusterNodeDynamicConfigManager>(
            New<TClusterNodeDynamicConfig>());

        NodeResourceManager_ = New<TNodeResourceManager>(this);
    }

    // Methods used by TJobResourceManager / TNodeResourceManager.
    const TClusterNodeBootstrapConfigPtr& GetConfig() const override { return Config_; }
    const INodeMemoryTrackerPtr& GetNodeMemoryUsageTracker() const override { return MemoryTracker_; }
    const IInvokerPtr& GetJobInvoker() const override { return JobInvoker_; }
    const IInvokerPtr& GetControlInvoker() const override { return ControlInvoker_; }
    const TClusterNodeDynamicConfigManagerPtr& GetDynamicConfigManager() const override { return DynamicConfigManager_; }
    const TNodeResourceManagerPtr& GetNodeResourceManager() const override { return NodeResourceManager_; }
    bool IsExecNode() const override { return false; }

    // Unused methods — abort if called.
    const NConcurrency::IThroughputThrottlerPtr& GetDefaultInThrottler() const override { YT_ABORT(); }
    const NConcurrency::IThroughputThrottlerPtr& GetDefaultOutThrottler() const override { YT_ABORT(); }
    const NConcurrency::IThroughputThrottlerPtr& GetReadRpsOutThrottler() const override { YT_ABORT(); }
    const NConcurrency::IThroughputThrottlerPtr& GetAnnounceChunkReplicaRpsOutThrottler() const override { YT_ABORT(); }
    const NConcurrency::IThroughputThrottlerPtr& GetUserJobContainerCreationThrottler() const override { YT_ABORT(); }
    const NProfiling::TBufferedProducerPtr& GetBufferedProducer() const override { YT_ABORT(); }
    const TFairShareHierarchicalSchedulerPtr<std::string>& GetFairShareHierarchicalScheduler() const override { YT_ABORT(); }
    const NIO::IHugePageManagerPtr& GetHugePageManager() const override { YT_ABORT(); }
    const NCellarNode::TBundleDynamicConfigManagerPtr& GetBundleDynamicConfigManager() const override { YT_ABORT(); }
    const IInvokerPtr& GetMasterConnectionInvoker() const override { YT_ABORT(); }
    const IInvokerPtr& GetStorageLightInvoker() const override { YT_ABORT(); }
    const IPrioritizedInvokerPtr& GetStorageHeavyInvoker() const override { YT_ABORT(); }
    const NApi::NNative::IClientPtr& GetClient() const override { YT_ABORT(); }
    const NApi::NNative::IConnectionPtr& GetConnection() const override { YT_ABORT(); }
    NRpc::IChannelPtr GetMasterChannel(NObjectClient::TCellTag /*cellTag*/) override { YT_ABORT(); }
    const NRpc::IAuthenticatorPtr& GetNativeAuthenticator() const override { YT_ABORT(); }
    NNodeTrackerClient::TNodeDescriptor GetLocalDescriptor() const override { YT_ABORT(); }
    NObjectClient::TCellId GetCellId() const override { YT_ABORT(); }
    NObjectClient::TCellId GetCellId(NObjectClient::TCellTag /*cellTag*/) const override { YT_ABORT(); }
    std::vector<std::string> GetMasterAddressesOrThrow(NObjectClient::TCellTag /*cellTag*/) const override { YT_ABORT(); }
    void ResetAndRegisterAtMaster(ERegistrationReason /*reason*/) override { YT_ABORT(); }
    bool IsConnected() const override { return false; }
    NNodeTrackerClient::TNodeId GetNodeId() const override { YT_ABORT(); }
    std::string GetLocalHostName() const override { YT_ABORT(); }
    TMasterEpoch GetMasterEpoch() const override { YT_ABORT(); }
    const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() const override { YT_ABORT(); }
    NNodeTrackerClient::TNetworkPreferenceList GetNetworks() const override { YT_ABORT(); }
    NNodeTrackerClient::TNetworkPreferenceList GetLocalNetworks() const override { YT_ABORT(); }
    const NHttp::IServerPtr& GetHttpServer() const override { YT_ABORT(); }
    const NRpc::IServerPtr& GetRpcServer() const override { YT_ABORT(); }
    const IMemoryUsageTrackerPtr& GetRpcMemoryUsageTracker() const override { YT_ABORT(); }
    const IMemoryUsageTrackerPtr& GetReadBlockMemoryUsageTracker() const override { YT_ABORT(); }
    const IMemoryUsageTrackerPtr& GetSystemJobsMemoryUsageTracker() const override { YT_ABORT(); }
    const NChunkClient::IBlockCachePtr& GetBlockCache() const override { YT_ABORT(); }
    const NTabletNode::IVersionedChunkMetaManagerPtr& GetVersionedChunkMetaManager() const override { YT_ABORT(); }
    const NDataNode::TChunkReaderSweeperPtr& GetChunkReaderSweeper() const override { YT_ABORT(); }
    const NYTree::IMapNodePtr& GetOrchidRoot() const override { YT_ABORT(); }
    bool IsReadOnly() const override { return false; }
    bool IsDecommissioned() const override { return false; }
    NDataNode::TNetworkStatistics& GetNetworkStatistics() const override { YT_ABORT(); }
    const NDataNode::IChunkRegistryPtr& GetChunkRegistry() const override { YT_ABORT(); }
    const TJobResourceManagerPtr& GetJobResourceManager() const override { YT_ABORT(); }
    const NServer::TRestartManagerPtr& GetRestartManager() const override { YT_ABORT(); }
    NJobProxy::EJobEnvironmentType GetJobEnvironmentType() const override { YT_ABORT(); }
    const NIO::IIOTrackerPtr& GetIOTracker() const override { YT_ABORT(); }
    const THashSet<NNodeTrackerClient::ENodeFlavor>& GetFlavors() const override { return Flavors_; }
    bool IsDataNode() const override { return false; }
    bool IsCellarNode() const override { return false; }
    bool IsTabletNode() const override { return false; }
    bool IsChaosNode() const override { return false; }
    NCellarNode::IBootstrap* GetCellarNodeBootstrap() const override { return nullptr; }
    NDataNode::IBootstrap* GetDataNodeBootstrap() const override { return nullptr; }
    NExecNode::IBootstrap* GetExecNodeBootstrap() const override { return nullptr; }
    NChaosNode::IBootstrap* GetChaosNodeBootstrap() const override { return nullptr; }
    NTabletNode::IBootstrap* GetTabletNodeBootstrap() const override { return nullptr; }
    const NClusterNode::IBootstrap* GetClusterNodeBootstrap() const override { return nullptr; }
    bool NeedDataNodeBootstrap() const override { return false; }

    // IBootstrap-specific methods (beyond IBootstrapBase).
    void Initialize() override { }
    TFuture<void> Run() override { return MakeFuture<void>(TError()); }
    const NClusterNode::IMasterConnectorPtr& GetMasterConnector() const override { YT_ABORT(); }
    NConcurrency::IThroughputThrottlerPtr CreateInThrottler(const TString& /*bucket*/) override { YT_ABORT(); }
    NConcurrency::IThroughputThrottlerPtr CreateOutThrottler(const TString& /*bucket*/) override { YT_ABORT(); }
    NDiskManager::IHotswapManagerPtr TryGetHotswapManager() const override { return nullptr; }
    NConcurrency::TRelativeThroughputThrottlerConfigPtr PatchRelativeNetworkThrottlerConfig(
        const NConcurrency::TRelativeThroughputThrottlerConfigPtr& config) const override { return config; }
    void SetDecommissioned(bool /*decommissioned*/) override { }
    void CompleteNodeRegistration() override { }

    void RunInJobThread(auto callback)
    {
        BIND(std::move(callback))
            .AsyncVia(JobInvoker_)
            .Run()
            .BlockingGet()
            .ThrowOnError();
    }

private:
    TClusterNodeBootstrapConfigPtr Config_;
    INodeMemoryTrackerPtr MemoryTracker_;
    TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    TNodeResourceManagerPtr NodeResourceManager_;
    TActionQueuePtr ControlActionQueue_;
    TActionQueuePtr JobActionQueue_;
    IInvokerPtr ControlInvoker_;
    IInvokerPtr JobInvoker_;
    THashSet<NNodeTrackerClient::ENodeFlavor> Flavors_;
};

////////////////////////////////////////////////////////////////////////////////

class TJobResourceManagerTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    {
        Bootstrap_ = New<TJobResourceManagerTestBootstrap>();
        ResourceManager_ = TJobResourceManager::CreateJobResourceManager(Bootstrap_.Get());
    }

    TIntrusivePtr<TJobResourceManagerTestBootstrap> Bootstrap_;
    TJobResourceManagerPtr ResourceManager_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TJobResourceManagerTest, UpdateResourceDemandAdjustsPendingResources)
{
    // This test reproduces the bug where UpdateResourceDemand changed holder's
    // BaseResourceUsage_ (and thus the value subtracted from Pending at acquisition)
    // without adjusting ResourceUsages_[Pending] in the JRM.
    // This caused negative pending DiskSpace.

    Bootstrap_->RunInJobThread([&] {
        // Step 1: Create a resource holder with initial demand having DiskSpaceRequest=0.
        TJobResources initialDemand;
        initialDemand.UserSlots = 1;
        initialDemand.UserMemory = 100_MB;
        initialDemand.DiskSpaceRequest = 0;

        auto holder = TResourceHolder::CreateResourceHolder(
            TGuid::Create(),
            ResourceManager_.Get(),
            EResourcesConsumerType::SchedulerAllocation,
            initialDemand);

        // After registration, pending should contain the initial demand (DiskSpaceRequest=0).
        auto pendingAfterRegister = ResourceManager_->GetResourceUsage({EResourcesState::Pending});
        EXPECT_EQ(pendingAfterRegister.DiskSpaceRequest, 0);
        EXPECT_EQ(pendingAfterRegister.UserSlots, 1);

        // Step 2: Update resource demand with DiskSpaceRequest=100MB
        // (this is what PrepareAllocation does via PatchJobResources).
        NScheduler::TAllocationAttributes attributes;
        attributes.DiskRequest.DiskSpace = 100_MB;

        TJobResources updatedDemand = initialDemand;
        updatedDemand.DiskSpaceRequest = 100_MB;

        holder->UpdateResourceDemand(updatedDemand, attributes);

        // After UpdateResourceDemand, pending should reflect the updated demand.
        auto pendingAfterUpdate = ResourceManager_->GetResourceUsage({EResourcesState::Pending});
        EXPECT_EQ(pendingAfterUpdate.DiskSpaceRequest, static_cast<i64>(100_MB));
        EXPECT_EQ(pendingAfterUpdate.UserSlots, 1);

        // Step 3: Simulate multiple holders to show accumulation of negative drift.
        // Create and update 3 more holders the same way.
        std::vector<TResourceHolderPtr> holders;
        for (int i = 0; i < 3; ++i) {
            auto h = TResourceHolder::CreateResourceHolder(
                TGuid::Create(),
                ResourceManager_.Get(),
                EResourcesConsumerType::SchedulerAllocation,
                initialDemand);
            h->UpdateResourceDemand(updatedDemand, attributes);
            holders.push_back(std::move(h));
        }

        // Total pending should be 4 * 100MB (not negative!).
        auto pendingAfterAll = ResourceManager_->GetResourceUsage({EResourcesState::Pending});
        EXPECT_EQ(pendingAfterAll.DiskSpaceRequest, 4 * static_cast<i64>(100_MB));
        EXPECT_EQ(pendingAfterAll.UserSlots, 4);

        // Cleanup: release all holders synchronously.
        holder->PrepareResourcesRelease();
        holder->ReleaseBaseResources();
        holder.Reset();
        for (auto& h : holders) {
            h->PrepareResourcesRelease();
            h->ReleaseBaseResources();
            h.Reset();
        }
    });

    // Drain the job invoker queue to ensure offloaded destructors complete
    // before the test fixture is torn down.
    Bootstrap_->RunInJobThread([] {});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NJobAgent
