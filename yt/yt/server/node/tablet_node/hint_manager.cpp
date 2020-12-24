#include "hint_manager.h"
#include "private.h"

#include <yt/server/node/cluster_node/bootstrap.h>
#include <yt/server/node/cluster_node/config.h>

#include <yt/server/lib/dynamic_config/dynamic_config_manager.h>

#include <yt/server/lib/tablet_node/config.h>

namespace NYT::NTabletNode {

using namespace NClusterNode;
using namespace NDynamicConfig;

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

class THintManager::TImpl
    : public TRefCounted
{
public:
    TImpl(THintManagerConfigPtr config, const TBootstrap* bootstrap)
        : ReplicatorHintConfigFetcher_(New<TReplicatorHintConfigFetcher>(
            config->ReplicatorHintConfigFetcher,
            bootstrap))
    {
        ReplicatorHintConfigFetcher_->SubscribeConfigUpdated(BIND(&TImpl::OnDynamicConfigUpdated, MakeWeak(this)));
    }

    void Start()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ReplicatorHintConfigFetcher_->Start();
    }

    bool IsReplicaClusterBanned(TStringBuf clusterName) const
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

    YT_DECLARE_SPINLOCK(TReaderWriterSpinLock, SpinLock_);

    THashSet<TString> BannedReplicaClusters_;
    TReplicatorHintConfigFetcherPtr ReplicatorHintConfigFetcher_;

    void OnDynamicConfigUpdated(
        const TReplicatorHintConfigPtr& /* oldConfig */,
        const TReplicatorHintConfigPtr& newConfig)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto guard = WriterGuard(SpinLock_);

        BannedReplicaClusters_ = newConfig->BannedReplicaClusters;
        YT_LOG_DEBUG("Updated list of banned replica clusters (BannedReplicaClusters: %v)",
            BannedReplicaClusters_);
    }
};

////////////////////////////////////////////////////////////////////////////////

THintManager::THintManager(
    THintManagerConfigPtr config,
    const TBootstrap* bootstrap)
    : Impl_(New<TImpl>(std::move(config), bootstrap))
{ }

THintManager::~THintManager() = default;

void THintManager::Start()
{
    Impl_->Start();
}

bool THintManager::IsReplicaClusterBanned(TStringBuf clusterName) const
{
    return Impl_->IsReplicaClusterBanned(clusterName);
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NTabletNode
