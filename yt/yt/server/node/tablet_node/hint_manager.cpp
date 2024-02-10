#include "hint_manager.h"
#include "bootstrap.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NTabletNode {

using namespace NClusterNode;
using namespace NConcurrency;
using namespace NDynamicConfig;
using namespace NNet;
using namespace NNodeTrackerClient;

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
    TReplicatorHintConfigFetcher(TDynamicConfigManagerConfigPtr config, const IBootstrap* bootstrap)
        : TDynamicConfigManagerBase<TReplicatorHintConfig>(
            TDynamicConfigManagerOptions{
                .ConfigPath = "//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint",
                .Name = "ReplicatorHint",
                .ConfigIsTagged = false
            },
            std::move(config),
            bootstrap->GetClient(),
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
    explicit THintManager(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->TabletNode->HintManager)
        , ReplicatorHintConfigFetcher_(New<TReplicatorHintConfigFetcher>(
            Config_->ReplicatorHintConfigFetcher,
            Bootstrap_))
    {
        ReplicatorHintConfigFetcher_->SubscribeConfigChanged(BIND(&THintManager::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void Start() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ReplicatorHintConfigFetcher_->Start();
    }

    bool IsReplicaClusterBanned(TStringBuf clusterName) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!ReplicatorHintConfigFetcher_->IsConfigLoaded()) {
            return false;
        }

        auto guard = ReaderGuard(BannedReplicaClustersSpinLock_);
        return BannedReplicaClusters_.contains(clusterName);
    }

    void UpdateSuspicionMarkTime(
        TNodeId nodeId,
        TStringBuf address,
        bool suspicious,
        std::optional<TInstant> previousMarkTime) override
    {
        auto guard = WriterGuard(SuspiciousNodesSpinLock_);

        auto it = SuspiciousNodesMarkTime_.find(nodeId);
        if (it == SuspiciousNodesMarkTime_.end() && suspicious) {
            YT_LOG_DEBUG("Node is marked as suspicious (NodeId: %v, Address: %v)",
                nodeId,
                address);
            SuspiciousNodesMarkTime_[nodeId] = TInstant::Now();
        }
        if (it != SuspiciousNodesMarkTime_.end() &&
            previousMarkTime == it->second &&
            !suspicious)
        {
            YT_LOG_DEBUG("Node is not suspicious anymore (NodeId: %v, Address: %v)",
                nodeId,
                address);
            SuspiciousNodesMarkTime_.erase(nodeId);
        }
    }

    std::vector<std::optional<TInstant>> RetrieveSuspicionMarkTimes(
        const std::vector<TNodeId>& nodeIds) const override
    {
        if (nodeIds.empty()) {
            return {};
        }

        std::vector<std::optional<TInstant>> markTimes;
        markTimes.reserve(nodeIds.size());

        auto guard = ReaderGuard(SuspiciousNodesSpinLock_);

        for (auto nodeId : nodeIds) {
            auto it = SuspiciousNodesMarkTime_.find(nodeId);
            auto markTime = it != SuspiciousNodesMarkTime_.end()
                ? std::make_optional(it->second)
                : std::nullopt;
            markTimes.push_back(markTime);
        }

        return markTimes;
    }

    THashMap<TNodeId, TInstant> RetrieveSuspiciousNodeIdsWithMarkTime(
        const std::vector<TNodeId>& nodeIds) const override
    {
        if (nodeIds.empty()) {
            return {};
        }

        THashMap<TNodeId, TInstant> nodeIdToSuspicionMarkTime;

        auto guard = ReaderGuard(SuspiciousNodesSpinLock_);

        for (auto nodeId : nodeIds) {
            auto it = SuspiciousNodesMarkTime_.find(nodeId);
            if (it != SuspiciousNodesMarkTime_.end()) {
                nodeIdToSuspicionMarkTime[nodeId] = it->second;
            }
        }

        return nodeIdToSuspicionMarkTime;
    }

    bool ShouldMarkNodeSuspicious(const TError& error) const override
    {
        return
            error.FindMatching(NRpc::EErrorCode::TransportError) ||
            error.FindMatching(NChunkClient::EErrorCode::MasterNotConnected);
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    IBootstrap* const Bootstrap_;
    const THintManagerConfigPtr Config_;
    const TReplicatorHintConfigFetcherPtr ReplicatorHintConfigFetcher_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, BannedReplicaClustersSpinLock_);
    THashSet<TString> BannedReplicaClusters_;

    // TODO(akozhikhov): Add periodic to clear old suspicious nodes.
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SuspiciousNodesSpinLock_);
    THashMap<TNodeId, TInstant> SuspiciousNodesMarkTime_;

    void OnDynamicConfigChanged(
        const TReplicatorHintConfigPtr& /*oldConfig*/,
        const TReplicatorHintConfigPtr& newConfig)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        {
            auto guard = WriterGuard(BannedReplicaClustersSpinLock_);
            BannedReplicaClusters_ = newConfig->BannedReplicaClusters;
        }

        YT_LOG_DEBUG("Updated list of banned replica clusters (BannedReplicaClusters: %v)",
            newConfig->BannedReplicaClusters);
    }
};

IHintManagerPtr CreateHintManager(IBootstrap* bootstrap)
{
    return New<THintManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NTabletNode
