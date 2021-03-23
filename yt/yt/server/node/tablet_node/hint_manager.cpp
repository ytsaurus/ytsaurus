#include "hint_manager.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/dynamic_config/dynamic_config_manager.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/core/net/address.h>

namespace NYT::NTabletNode {

using namespace NClusterNode;
using namespace NConcurrency;
using namespace NDynamicConfig;
using namespace NNet;

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

        auto guard = ReaderGuard(BannedReplicaClustersSpinLock_);
        return BannedReplicaClusters_.contains(clusterName);
    }

    virtual void UpdateSuspicionMarkTime(
        const TString& nodeAddress,
        bool suspicious,
        std::optional<TInstant> previousMarkTime) override
    {
        auto hostName = GetServiceHostName(nodeAddress);

        auto guard = WriterGuard(SuspiciousNodesSpinLock_);

        auto it = SuspiciousNodesMarkTime_.find(hostName);
        if (it == SuspiciousNodesMarkTime_.end() && suspicious) {
            YT_LOG_DEBUG("Node is marked as suspicious (HostName: %v)",
                hostName);
            SuspiciousNodesMarkTime_[hostName] = TInstant::Now();
        }
        if (it != SuspiciousNodesMarkTime_.end() && 
            previousMarkTime == it->second &&
            !suspicious)
        {
            YT_LOG_DEBUG("Node is not suspicious anymore (HostName: %v)",
                hostName);
            SuspiciousNodesMarkTime_.erase(hostName);
        }
    }

    virtual std::vector<std::optional<TInstant>> RetrieveSuspicionMarkTimes(
        const std::vector<TString>& nodeAddresses) const override
    {
        auto guard = ReaderGuard(SuspiciousNodesSpinLock_);

        std::vector<std::optional<TInstant>> markTimes;
        markTimes.reserve(nodeAddresses.size());
        for (const auto& nodeAddress : nodeAddresses) {
            auto hostName = GetServiceHostName(nodeAddress);
            auto it = SuspiciousNodesMarkTime_.find(hostName);
            auto markTime = it != SuspiciousNodesMarkTime_.end()
                ? std::make_optional(it->second)
                : std::nullopt;
            markTimes.push_back(markTime);
        }

        return markTimes;
    }

    virtual bool ShouldMarkNodeSuspicious(TErrorCode errorCode) const override
    {
        return
            errorCode == NRpc::EErrorCode::TransportError ||
            errorCode == NChunkClient::EErrorCode::MasterNotConnected;
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    NClusterNode::TBootstrap* const Bootstrap_;
    const THintManagerConfigPtr Config_;
    const TReplicatorHintConfigFetcherPtr ReplicatorHintConfigFetcher_;

    YT_DECLARE_SPINLOCK(TReaderWriterSpinLock, BannedReplicaClustersSpinLock_);
    THashSet<TString> BannedReplicaClusters_;

    // TODO(akozhikhov): Add periodic to clear old suspicious nodes.
    YT_DECLARE_SPINLOCK(TReaderWriterSpinLock, SuspiciousNodesSpinLock_);
    THashMap<TString, TInstant> SuspiciousNodesMarkTime_;

    void OnDynamicConfigChanged(
        const TReplicatorHintConfigPtr& /* oldConfig */,
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

IHintManagerPtr CreateHintManager(NClusterNode::TBootstrap* bootstrap)
{
    return New<THintManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NTabletNode
