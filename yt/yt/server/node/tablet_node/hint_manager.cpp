#include "hint_manager.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/dynamic_config/dynamic_config_manager.h>

#include <yt/yt/server/lib/tablet_node/config.h>

namespace NYT::NTabletNode {

using namespace NClusterNode;
using namespace NDynamicConfig;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TReplicatorHintConfigFetcher)

class TReplicatorHintConfigFetcher
    : public TDynamicConfigManagerBase<TReplicatorHintConfig>
{
public:
    TReplicatorHintConfigFetcher(TDynamicConfigManagerConfigPtr config, const TBootstrap* bootstrap)
        : TDynamicConfigManagerBase<TReplicatorHintConfig>(
            TDynamicConfigManagerOptions{
                .ConfigPath = "//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint",
                .Name = "ReplicatorHint",
                .ConfigIsTagged = false
            },
            std::move(config),
            bootstrap->GetMasterClient(),
            bootstrap->GetControlInvoker())
    { }
};

DEFINE_REFCOUNTED_TYPE(TReplicatorHintConfigFetcher)

////////////////////////////////////////////////////////////////////////////////

}  // namespace

////////////////////////////////////////////////////////////////////////////////

class THintManager
    : public IHintManager
{
public:
    explicit THintManager(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->TabletNode->HintManager)
        , ReplicatorHintConfigFetcher_(New<TReplicatorHintConfigFetcher>(
            Config_->ReplicatorHintConfigFetcher,
            Bootstrap_))
    {
        ReplicatorHintConfigFetcher_->SubscribeConfigChanged(BIND(&THintManager::OnDynamicConfigChanged, MakeWeak(this)));
    }

    virtual void Start() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ReplicatorHintConfigFetcher_->Start();
    }

    virtual bool IsReplicaClusterBanned(TStringBuf clusterName) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!ReplicatorHintConfigFetcher_->IsConfigLoaded()) {
            return false;
        }

        auto guard = ReaderGuard(SpinLock_);
        return BannedReplicaClusters_.contains(clusterName);
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    NClusterNode::TBootstrap* const Bootstrap_;
    const THintManagerConfigPtr Config_;
    const TReplicatorHintConfigFetcherPtr ReplicatorHintConfigFetcher_;

    YT_DECLARE_SPINLOCK(TReaderWriterSpinLock, SpinLock_);
    THashSet<TString> BannedReplicaClusters_;

    void OnDynamicConfigChanged(
        const TReplicatorHintConfigPtr& /* oldConfig */,
        const TReplicatorHintConfigPtr& newConfig)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        {
            auto guard = WriterGuard(SpinLock_);
            BannedReplicaClusters_ = newConfig->BannedReplicaClusters;
        }

        YT_LOG_DEBUG("Updated list of banned replica clusters (BannedReplicaClusters: %v)",
            newConfig->BannedReplicaClusters);
    }
};

IHintManagerPtr CreateHintManager(NClusterNode::TBootstrap* bootstrap)
{
    return New<THintManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NTabletNode
