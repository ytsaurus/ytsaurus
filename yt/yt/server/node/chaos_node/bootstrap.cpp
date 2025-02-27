#include "bootstrap.h"

#include "chaos_slot.h"
#include "config.h"
#include "slot_manager.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

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
using namespace NTransactionSupervisor;
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

        GetDynamicConfigManager()
            ->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TBootstrap::OnDynamicConfigChanged, MakeWeak(this)));
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

    const ITransactionLeaseTrackerThreadPoolPtr& GetTransactionLeaseTrackerThreadPool() const override
    {
        return GetCellarNodeBootstrap()->GetTransactionLeaseTrackerThreadPool();
    }

    const NCellarAgent::ICellarManagerPtr& GetCellarManager() const override
    {
        return GetCellarNodeBootstrap()->GetCellarManager();
    }

    const IInvokerPtr& GetSnapshotStoreReadPoolInvoker() const override
    {
        return SnapshotStoreReadPool_->GetInvoker();
    }

    const NApi::NNative::IConnectionPtr& GetClusterConnection() const override
    {
        return ClusterNodeBootstrap_->GetConnection();
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

    IThreadPoolPtr SnapshotStoreReadPool_;
    ISlotManagerPtr SlotManager_;
    TIntrusivePtr<TReplicatedTableTrackerConfigFetcher> ReplicatedTableTrackerConfigFetcher_;

    NCellarNode::IBootstrap* GetCellarNodeBootstrap() const override
    {
        return ClusterNodeBootstrap_->GetCellarNodeBootstrap();
    }

    void Reconfigure(const TChaosNodeDynamicConfigPtr& config)
    {
        auto cellar = GetCellarManager()->FindCellar(ECellarType::Chaos);
        if (!cellar) {
            return;
        }

        for (const auto& occupant : cellar->Occupants()) {
            if (occupant) {
                auto occupier = occupant->GetTypedOccupier<IChaosSlot>();
                occupant->GetTypedOccupier<IChaosSlot>()->Reconfigure(config);
            }
        }
    }

    static void OnDynamicConfigChanged(
        TWeakPtr<TBootstrap> bootstrap,
        const TClusterNodeDynamicConfigPtr& /*oldConfig*/,
        const TClusterNodeDynamicConfigPtr& newConfig)
    {
        if (auto strongPtr = bootstrap.Lock()) {
            strongPtr->Reconfigure(newConfig->ChaosNode);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateBootstrap(NClusterNode::IBootstrap* bootstrap)
{
    return New<TBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
