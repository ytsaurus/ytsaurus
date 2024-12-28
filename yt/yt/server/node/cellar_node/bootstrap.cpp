#include "bootstrap.h"

#include "bundle_dynamic_config_manager.h"
#include "master_connector.h"
#include "private.h"
#include "config.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/tablet_node/security_manager.h>
#include <yt/yt/server/node/tablet_node/tablet_cell_snapshot_validator.h>

#include <yt/yt/server/lib/cellar_agent/bootstrap_proxy.h>
#include <yt/yt/server/lib/cellar_agent/cellar_manager.h>
#include <yt/yt/server/lib/cellar_agent/occupant.h>

#include <yt/yt/server/lib/hydra/composite_automaton.h>
#include <yt/yt/server/lib/hydra/dry_run/dry_run_hydra_manager.h>
#include <yt/yt/server/lib/hydra/dry_run/journal_as_local_file_read_only_changelog.h>
#include <yt/yt/server/lib/hydra/dry_run/public.h>
#include <yt/yt/server/lib/hydra/local_snapshot_store.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_lease_tracker.h>

#include <yt/yt/core/misc/fs.h>

namespace NYT::NCellarNode {

using namespace NApi::NNative;
using namespace NCellarAgent;
using namespace NCellarClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NElection;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSecurityServer;
using namespace NTransactionSupervisor;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = CellarNodeLogger;
static YT_DEFINE_GLOBAL(const NLogging::TLogger, DryRunLogger, "DryRun");

////////////////////////////////////////////////////////////////////////////////

class TCellarBootstrapProxy
    : public ICellarBootstrapProxy
{
public:
    explicit TCellarBootstrapProxy(
        IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    TCellId GetCellId() const override
    {
        return Bootstrap_->GetCellId();
    }

    IClientPtr GetClient() const override
    {
        return Bootstrap_->GetClient();
    }

    IAuthenticatorPtr GetNativeAuthenticator() const override
    {
        return Bootstrap_->GetNativeAuthenticator();
    }

    TNetworkPreferenceList GetLocalNetworks() const override
    {
        return Bootstrap_->GetLocalNetworks();
    }

    IInvokerPtr GetControlInvoker() const override
    {
        return Bootstrap_->GetControlInvoker();
    }

    const ITransactionLeaseTrackerThreadPoolPtr& GetTransactionLeaseTrackerThreadPool() const override
    {
        return Bootstrap_->GetTransactionLeaseTrackerThreadPool();
    }

    IServerPtr GetRpcServer() const override
    {
        return Bootstrap_->GetRpcServer();
    }

    IResourceLimitsManagerPtr GetResourceLimitsManager() const override
    {
        return Bootstrap_->GetResourceLimitsManager();
    }

    void ScheduleCellarHeartbeat() const override
    {
        Bootstrap_->ScheduleCellarHeartbeat();
    }

    const IThroughputThrottlerPtr& GetChangelogOutThrottler() const override
    {
        return Bootstrap_->GetChangelogOutThrottler();
    }

    const IThroughputThrottlerPtr& GetSnapshotOutThrottler() const override
    {
        return Bootstrap_->GetSnapshotOutThrottler();
    }

    DECLARE_SIGNAL_OVERRIDE(void(std::vector<TError>* alerts), PopulateAlerts);

private:
    IBootstrap* const Bootstrap_;
};

DELEGATE_SIGNAL(TCellarBootstrapProxy, void(std::vector<TError>* alerts), PopulateAlerts, *Bootstrap_);

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
    , public NClusterNode::TBootstrapBase
{
public:
    explicit TBootstrap(NClusterNode::IBootstrap* bootstrap)
        : TBootstrapBase(bootstrap)
        , ClusterNodeBootstrap_(bootstrap)
    { }

    void Initialize() override
    {
        YT_LOG_INFO("Initializing cellar node");

        // Cycles are fine for bootstrap.
        GetBundleDynamicConfigManager()
            ->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TBootstrap::OnBundleDynamicConfigChanged, MakeStrong(this)));
        GetDynamicConfigManager()
            ->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TBootstrap::OnDynamicConfigChanged, MakeStrong(this)));

        TransactionLeaseTrackerThreadPool_ = CreateTransactionLeaseTrackerThreadPool(
            "TxTracker",
            GetConfig()->CellarNode->TransactionLeaseTracker);

        // TODO(gritukan): Move TSecurityManager from Tablet Node.
        ResourceLimitsManager_ = New<NTabletNode::TSecurityManager>(GetConfig()->TabletNode->SecurityManager, this);

        // COMPAT(savrus)
        auto getCellarManagerConfig = [&] {
            auto& config = GetConfig()->CellarNode->CellarManager;

            if (!ClusterNodeBootstrap_->IsTabletNode()) {
                return config;
            }

            for (const auto& [type, _] : config->Cellars) {
                if (type == ECellarType::Tablet) {
                    return config;
                }
            }

            auto cellarConfig = New<TCellarConfig>();
            cellarConfig->Size = GetConfig()->TabletNode->ResourceLimits->Slots;
            cellarConfig->Occupant = New<TCellarOccupantConfig>();
            cellarConfig->Occupant->Snapshots = GetConfig()->TabletNode->Snapshots;
            cellarConfig->Occupant->Changelogs = GetConfig()->TabletNode->Changelogs;
            cellarConfig->Occupant->HydraManager = GetConfig()->TabletNode->HydraManager;
            cellarConfig->Occupant->ElectionManager = GetConfig()->TabletNode->ElectionManager;
            cellarConfig->Occupant->HiveManager = GetConfig()->TabletNode->HiveManager;
            cellarConfig->Occupant->TransactionSupervisor = GetConfig()->TabletNode->TransactionSupervisor;
            cellarConfig->Occupant->ResponseKeeper = GetConfig()->TabletNode->HydraManager->ResponseKeeper;
            cellarConfig->Occupant->EnableDryRun = GetConfig()->DryRun->EnableDryRun;

            auto cellarManagerConfig = CloneYsonStruct(config);
            cellarManagerConfig->Cellars.insert({ECellarType::Tablet, std::move(cellarConfig)});
            return cellarManagerConfig;
        };

        if (GetConfig()->EnableFairThrottler) {
            ChangelogOutThrottler_ = ClusterNodeBootstrap_->GetOutThrottler(FormatEnum(
                NTabletNode::ETabletNodeThrottlerKind::ChangelogOut));
            SnapshotOutThrottler_ = ClusterNodeBootstrap_->GetOutThrottler(FormatEnum(
                NTabletNode::ETabletNodeThrottlerKind::SnapshotOut));
        } else {
            ChangelogOutThrottler_ = GetUnlimitedThrottler();
            SnapshotOutThrottler_ = GetUnlimitedThrottler();
        }

        auto cellarBootstrapProxy = New<TCellarBootstrapProxy>(this);
        CellarManager_ = CreateCellarManager(getCellarManagerConfig(), std::move(cellarBootstrapProxy));

        MasterConnector_ = CreateMasterConnector(this);

        CellarManager_->Initialize();
        MasterConnector_->Initialize();
    }

    void Run() override
    { }

    const ITransactionLeaseTrackerThreadPoolPtr& GetTransactionLeaseTrackerThreadPool() const override
    {
        return TransactionLeaseTrackerThreadPool_;
    }

    const IResourceLimitsManagerPtr& GetResourceLimitsManager() const override
    {
        return ResourceLimitsManager_;
    }

    const ICellarManagerPtr& GetCellarManager() const override
    {
        return CellarManager_;
    }

    const IMasterConnectorPtr& GetMasterConnector() const override
    {
        return MasterConnector_;
    }

    void ScheduleCellarHeartbeat() const override
    {
        if (!IsConnected()) {
            return;
        }

        const auto& clusterNodeMasterConnector = GetClusterNodeBootstrap()->GetMasterConnector();
        for (auto masterCellTag : clusterNodeMasterConnector->GetMasterCellTags()) {
            MasterConnector_->ScheduleHeartbeat(masterCellTag);
        }
    }

    void LoadSnapshot(
        const TString& fileName,
        NHydra::NProto::TSnapshotMeta meta = {},
        bool dumpSnapshot = false) override
    {
        BIND(&TBootstrap::DoLoadSnapshot, MakeStrong(this), fileName, meta, dumpSnapshot)
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
    }

    void ReplayChangelogs(std::vector<TString> changelogFileNames) override
    {
        BIND(&TBootstrap::DoReplayChangelogs, MakeStrong(this), Passed(std::move(changelogFileNames)))
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
    }

    void BuildSnapshot() override
    {
        BIND(&TBootstrap::DoBuildSnapshot, MakeStrong(this))
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
    }

    void FinishDryRun() override
    {
        BIND(&TBootstrap::DoFinishDryRun, MakeStrong(this))
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
    }

    const IThroughputThrottlerPtr& GetChangelogOutThrottler() const override
    {
        return ChangelogOutThrottler_;
    }

    const IThroughputThrottlerPtr& GetSnapshotOutThrottler() const override
    {
        return SnapshotOutThrottler_;
    }

private:
    NClusterNode::IBootstrap* const ClusterNodeBootstrap_;

    ITransactionLeaseTrackerThreadPoolPtr TransactionLeaseTrackerThreadPool_;
    IResourceLimitsManagerPtr ResourceLimitsManager_;

    ICellarManagerPtr CellarManager_;

    IMasterConnectorPtr MasterConnector_;

    ICellarOccupantPtr DryRunOccupant_;

    IThroughputThrottlerPtr ChangelogOutThrottler_;
    IThroughputThrottlerPtr SnapshotOutThrottler_;

    void EnsureDryRunOccupantCreated()
    {
        if (DryRunOccupant_) {
            return;
        }

        auto cellId = GetConfig()->DryRun->TabletCellId;
        auto tabletCellBundle = GetConfig()->DryRun->TabletCellBundle;
        auto clockClusterTag = GetConfig()->DryRun->ClockClusterTag;
        YT_VERIFY(cellId);
        YT_VERIFY(tabletCellBundle);

        YT_LOG_EVENT(
            DryRunLogger,
            NLogging::ELogLevel::Info,
            "Creating dry run occupant (CellId: %v, TabletCellBundle: %v, ClockClusterTag: %v)",
            cellId,
            tabletCellBundle,
            clockClusterTag);

        DryRunOccupant_ = NTabletNode::CreateFakeOccupant(ClusterNodeBootstrap_, cellId, tabletCellBundle, clockClusterTag);
    }

    void DoLoadSnapshot(
        const TString& fileName,
        const NHydra::NProto::TSnapshotMeta& meta = {},
        bool dumpSnapshot = false)
    {
        YT_LOG_EVENT(DryRunLogger, NLogging::ELogLevel::Info, "Snapshot meta received (Meta: %v)",
            meta);

        EnsureDryRunOccupantCreated();
        auto snapshotReader = CreateUncompressedHeaderlessLocalSnapshotReader(
            fileName,
            meta,
            DryRunOccupant_->GetSnapshotLocalIOInvoker());

        const auto& automaton = DryRunOccupant_->GetAutomaton();
        automaton->SetSerializationDumpEnabled(dumpSnapshot);

        const auto& hydraManager = DryRunOccupant_->GetHydraManager();
        auto dryRunHydraManager = StaticPointerCast<IDryRunHydraManager>(hydraManager);
        dryRunHydraManager->DryRunLoadSnapshot(std::move(snapshotReader));
    }

    void DoReplayChangelogs(const std::vector<TString>& changelogFileNames)
    {
        EnsureDryRunOccupantCreated();
        const auto& hydraManager = DryRunOccupant_->GetHydraManager();
        auto dryRunHydraManager = StaticPointerCast<IDryRunHydraManager>(hydraManager);

        for (const auto& changelogFileName : changelogFileNames) {
            int changelogId  = InvalidSegmentId;
            if (!TryFromString(NFS::GetFileNameWithoutExtension(changelogFileName), changelogId)) {
                YT_LOG_EVENT(DryRunLogger, NLogging::ELogLevel::Info, "Error parsing changelog name as id, using id %v as substitute",
                    changelogId);
            }

            YT_LOG_EVENT(DryRunLogger, NLogging::ELogLevel::Info, "Started loading changelog");
            auto changelog = CreateJournalAsLocalFileReadOnlyChangelog(changelogFileName, changelogId);
            dryRunHydraManager->DryRunReplayChangelog(changelog);
        }
    }

    void DoBuildSnapshot()
    {
        EnsureDryRunOccupantCreated();
        const auto& hydraManager = DryRunOccupant_->GetHydraManager();
        auto dryRunHydraManager = StaticPointerCast<IDryRunHydraManager>(hydraManager);
        dryRunHydraManager->DryRunBuildSnapshot();
    }

    void DoFinishDryRun()
    {
        EnsureDryRunOccupantCreated();
        const auto& hydraManager = DryRunOccupant_->GetHydraManager();
        auto dryRunHydraManager = StaticPointerCast<IDryRunHydraManager>(hydraManager);
        dryRunHydraManager->DryRunShutdown();
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldConfig*/,
        const TClusterNodeDynamicConfigPtr& newConfig)
    {
        auto bundleConfig = GetBundleDynamicConfigManager()->GetConfig();
        ReconfigureCellarManager(bundleConfig, newConfig);
    }

    void OnBundleDynamicConfigChanged(
        const TBundleDynamicConfigPtr& /*oldConfig*/,
        const TBundleDynamicConfigPtr& newConfig)
    {
        auto nodeConfig = GetDynamicConfigManager()->GetConfig();
        ReconfigureCellarManager(newConfig, nodeConfig);
    }

    void ReconfigureCellarManager(
        const TBundleDynamicConfigPtr& bundleConfig,
        const TClusterNodeDynamicConfigPtr& newConfig)
    {
        auto bundleDynamicSlotsCount = bundleConfig->CpuLimits->WriteThreadPoolSize;

        std::optional<int> slotsCount = bundleDynamicSlotsCount;
        if (!slotsCount && newConfig->TabletNode->Slots) {
            slotsCount = *newConfig->TabletNode->Slots;
        }

        // COMPAT(savrus, capone212)
        auto getCellarManagerConfig = [&] {
            auto& config = newConfig->CellarNode->CellarManager;
            if (!slotsCount) {
                return config;
            } else {
                auto cellarManagerConfig = CloneYsonStruct(config);
                for (const auto& [type, cellarConfig] : cellarManagerConfig->Cellars) {
                    if (type == ECellarType::Tablet) {
                        if (bundleDynamicSlotsCount) {
                            cellarConfig->Size = *bundleDynamicSlotsCount;
                        }
                        return cellarManagerConfig;
                    }
                }
                auto cellarConfig = New<TCellarDynamicConfig>();
                cellarConfig->Size = slotsCount;
                cellarConfig->HydraManager->EnableChangelogNetworkUsageAccounting =
                    newConfig->TabletNode->EnableChangelogNetworkUsageAccounting;
                cellarConfig->HydraManager->EnableSnapshotNetworkThrottling =
                    newConfig->TabletNode->EnableSnapshotNetworkThrottling;
                cellarManagerConfig->Cellars.insert({ECellarType::Tablet, std::move(cellarConfig)});
                return cellarManagerConfig;
            }
        };

        CellarManager_->Reconfigure(getCellarManagerConfig());

        ResourceLimitsManager_->Reconfigure(newConfig->TabletNode->SecurityManager);
    }
};

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateBootstrap(NClusterNode::IBootstrap* bootstrap)
{
    return New<TBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
