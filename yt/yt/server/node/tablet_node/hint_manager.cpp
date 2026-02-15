#include "hint_manager.h"

#include "bootstrap.h"
#include "config.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NTabletNode {

using namespace NDynamicConfig;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

DECLARE_REFCOUNTED_CLASS(TReplicatorHintConfigFetcher)

class TReplicatorHintConfigFetcher
    : public TDynamicConfigManagerBase<TReplicatorHintConfig>
{
public:
    TReplicatorHintConfigFetcher(TDynamicConfigManagerConfigPtr config, IBootstrap* bootstrap)
        : TDynamicConfigManagerBase<TReplicatorHintConfig>(
            TDynamicConfigManagerOptions{
                .ConfigPath = "//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint",
                .Name = "ReplicatorHint",
                .ConfigIsTagged = false,
            },
            std::move(config),
            bootstrap->GetClient(),
            bootstrap->GetControlInvoker())
    { }
};

DEFINE_REFCOUNTED_TYPE(TReplicatorHintConfigFetcher)

}  // namespace

////////////////////////////////////////////////////////////////////////////////

class THintManager
    : public IHintManager
{
public:
    explicit THintManager(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->TabletNode->HintManager)
        , ReplicatorHintConfigFetcher_(New<TReplicatorHintConfigFetcher>(
            Config_->ReplicatorHintConfigFetcher,
            Bootstrap_))
        , OrchidService_(CreateOrchidService())
    {
        ReplicatorHintConfigFetcher_->SubscribeConfigChanged(BIND_NO_PROPAGATE(&THintManager::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void Start() override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        ReplicatorHintConfigFetcher_->Start();
    }

    bool IsReplicaClusterBanned(TStringBuf clusterName) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (!ReplicatorHintConfigFetcher_->IsConfigLoaded()) {
            return false;
        }

        auto guard = ReaderGuard(BannedReplicaClustersSpinLock_);
        return BannedReplicaClusters_.contains(clusterName);
    }

    IYPathServicePtr GetOrchidService() override
    {
        return OrchidService_;
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    IBootstrap* const Bootstrap_;
    const THintManagerConfigPtr Config_;
    const TReplicatorHintConfigFetcherPtr ReplicatorHintConfigFetcher_;
    const IYPathServicePtr OrchidService_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, BannedReplicaClustersSpinLock_);
    THashSet<std::string, THash<TStringBuf>, TEqualTo<TStringBuf>> BannedReplicaClusters_;

    void OnDynamicConfigChanged(
        const TReplicatorHintConfigPtr& /*oldConfig*/,
        const TReplicatorHintConfigPtr& newConfig)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        {
            auto guard = WriterGuard(BannedReplicaClustersSpinLock_);
            BannedReplicaClusters_ = {newConfig->BannedReplicaClusters.begin(), newConfig->BannedReplicaClusters.end()};
        }

        YT_LOG_DEBUG("Updated list of banned replica clusters (BannedReplicaClusters: %v)",
            newConfig->BannedReplicaClusters);
    }

    IYPathServicePtr CreateOrchidService()
    {
        return IYPathService::FromProducer(BIND(&THintManager::BuildOrchid, MakeStrong(this)))
            ->Via(Bootstrap_->GetControlInvoker());
    }

    void BuildOrchid(NYson::IYsonConsumer* consumer) const
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("banned_replica_clusters")
                .BeginList()
                    .DoFor(
                        BannedReplicaClusters_,
                        [=] (TFluentList fluent, const std::string& clusterName) {
                            fluent.Item().Value(clusterName);
                        })
                .EndList()
            .EndMap();
    }
};

IHintManagerPtr CreateHintManager(IBootstrap* bootstrap)
{
    return New<THintManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NTabletNode
