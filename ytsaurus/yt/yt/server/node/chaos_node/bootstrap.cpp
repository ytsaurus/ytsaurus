#include "bootstrap.h"

#include "config.h"
#include "slot_manager.h"
#include "shortcut_snapshot_store.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/node/cellar_node/bootstrap.h>

#include <yt/yt/server/lib/cellar_agent/cellar.h>
#include <yt/yt/server/lib/cellar_agent/cellar_manager.h>

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/server/lib/tablet_server/config.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NChaosNode {

using namespace NApi;
using namespace NCellarClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NTabletServer;
using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
    , public TBootstrapBase
{
public:
    explicit TBootstrap(NClusterNode::IBootstrap* bootstrap)
        : TBootstrapBase(bootstrap)
        , ClusterNodeBootstrap_(bootstrap)
    { }

    void Initialize() override
    {
        SnapshotStoreReadPool_ = CreateThreadPool(
            GetConfig()->ChaosNode->SnapshotStoreReadPoolSize,
            "ShortcutRead");

        SlotManager_ = CreateSlotManager(GetConfig()->ChaosNode, this);
        SlotManager_->Initialize();

        ReplicatedTableTrackerConfigFetcher_ = New<TReplicatedTableTrackerConfigFetcher>(
            GetConfig()->ChaosNode->ReplicatedTableTrackerConfigFetcher,
            ClusterNodeBootstrap_);
    }

    void Run() override
    {
        SetNodeByYPath(
            GetOrchidRoot(),
            "/chaos_cells",
            CreateVirtualNode(GetCellarManager()->GetCellar(ECellarType::Chaos)->GetOrchidService()));

        ReplicatedTableTrackerConfigFetcher_->Start();
        SlotManager_->Start();
    }

    const IInvokerPtr& GetTransactionTrackerInvoker() const override
    {
        return GetCellarNodeBootstrap()->GetTransactionTrackerInvoker();
    }

    const NCellarAgent::ICellarManagerPtr& GetCellarManager() const override
    {
        return GetCellarNodeBootstrap()->GetCellarManager();
    }

    const IShortcutSnapshotStorePtr& GetShortcutSnapshotStore() const override
    {
        return ShortcutSnapshotStore_;
    }

    const IInvokerPtr& GetSnapshotStoreReadPoolInvoker() const override
    {
        return SnapshotStoreReadPool_->GetInvoker();
    }

    const NApi::NNative::IConnectionPtr& GetClusterConnection() const override
    {
        return ClusterNodeBootstrap_->GetConnection();
    }

    void SubscribeReplicatedTableTrackerConfigChanged(TReplicatedTableTrackerConfigUpdateCallback callback) const override
    {
        ReplicatedTableTrackerConfigFetcher_->SubscribeConfigChanged(callback);
    }

    NTabletServer::TDynamicReplicatedTableTrackerConfigPtr GetReplicatedTableTrackerConfig() const override
    {
        return ReplicatedTableTrackerConfigFetcher_->GetConfig();
    }

private:
    class TReplicatedTableTrackerConfigFetcher
        : public TDynamicConfigManagerBase<TDynamicReplicatedTableTrackerConfig>
    {
    public:
        TReplicatedTableTrackerConfigFetcher(
            TDynamicConfigManagerConfigPtr config,
            const NClusterNode::IBootstrap* bootstrap)
            : TDynamicConfigManagerBase<TDynamicReplicatedTableTrackerConfig>(
                TDynamicConfigManagerOptions{
                    .ConfigPath = "//sys/@config/tablet_manager/replicated_table_tracker",
                    .Name = "ReplicatedTableTracker",
                    .ConfigIsTagged = false
                },
                std::move(config),
                bootstrap->GetClient(),
                bootstrap->GetControlInvoker())
        { }
    };

    NClusterNode::IBootstrap* const ClusterNodeBootstrap_;
    const IShortcutSnapshotStorePtr ShortcutSnapshotStore_ = CreateShortcutSnapshotStore();

    IThreadPoolPtr SnapshotStoreReadPool_;
    ISlotManagerPtr SlotManager_;
    TIntrusivePtr<TReplicatedTableTrackerConfigFetcher> ReplicatedTableTrackerConfigFetcher_;

    NCellarNode::IBootstrap* GetCellarNodeBootstrap() const override
    {
        return ClusterNodeBootstrap_->GetCellarNodeBootstrap();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(NClusterNode::IBootstrap* bootstrap)
{
    return std::make_unique<TBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
