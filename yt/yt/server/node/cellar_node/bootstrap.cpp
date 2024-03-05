#include "bootstrap.h"

#include "bundle_dynamic_config_manager.h"
#include "master_connector.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/tablet_node/security_manager.h>
#include <yt/yt/server/node/tablet_node/tablet_cell_snapshot_validator.h>

#include <yt/yt/server/node/cellar_node/config.h>

#include <yt/yt/server/lib/cellar_agent/bootstrap_proxy.h>
#include <yt/yt/server/lib/cellar_agent/cellar_manager.h>
#include <yt/yt/server/lib/cellar_agent/occupant.h>

#include <yt/yt/server/lib/hydra/composite_automaton.h>
#include <yt/yt/server/lib/hydra/dry_run/dry_run_hydra_manager.h>
#include <yt/yt/server/lib/hydra/dry_run/journal_as_local_file_read_only_changelog.h>
#include <yt/yt/server/lib/hydra/dry_run/public.h>
#include <yt/yt/server/lib/hydra/local_snapshot_store.h>

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

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellarNodeLogger;
static inline const NLogging::TLogger DryRunLogger("DryRun");

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

    IInvokerPtr GetTransactionTrackerInvoker() const override
    {
        return Bootstrap_->GetTransactionTrackerInvoker();
    }

    IServerPtr GetRpcServer() const override
    {
        return Bootstrap_->GetRpcServer();
    }

    IResourceLimitsManagerPtr GetResourceLimitsManager() const override
    {
        return Bootstrap_->GetResourceLimitsManager();
    }

    void ScheduleCellarHeartbeat(bool immediately) const override
    {
        Bootstrap_->ScheduleCellarHeartbeat(immediately);
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

        GetBundleDynamicConfigManager()
            ->SubscribeConfigChanged(BIND(&TBootstrap::OnBundleDynamicConfigChanged, this));

        GetDynamicConfigManager()
            ->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, this));

        TransactionTrackerQueue_ = New<TActionQueue>("TxTracker");

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

        auto cellarBootstrapProxy = New<TCellarBootstrapProxy>(this);
        CellarManager_ = CreateCellarManager(getCellarManagerConfig(), std::move(cellarBootstrapProxy));

        MasterConnector_ = CreateMasterConnector(this);

        CellarManager_->Initialize();
        MasterConnector_->Initialize();
    }

    void Run() override
    { }

    const IInvokerPtr& GetTransactionTrackerInvoker() const override
    {
        return TransactionTrackerQueue_->GetInvoker();
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

    void ScheduleCellarHeartbeat(bool immediately) const override
    {
        if (!IsConnected()) {
            return;
        }

        for (auto masterCellTag : GetMasterCellTags()) {
            MasterConnector_->ScheduleHeartbeat(masterCellTag, immediately);
        }
    }

    void LoadSnapshotOrThrow(
        const TString& fileName,
        NHydra::NProto::TSnapshotMeta meta = {},
        bool dumpSnapshot = false) override
    {
        BIND(&TBootstrap::DoLoadSnapshot, this, fileName, meta, dumpSnapshot)
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
    }

    void ReplayChangelogsOrThrow(std::vector<TString> changelogFileNames) override
    {
        BIND(&TBootstrap::DoReplayChangelogs, this, Passed(std::move(changelogFileNames)))
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
    }

    void BuildSnapshotOrThrow() override
    {
        BIND(&TBootstrap::DoBuildSnapshot, this)
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
    }

    void FinishDryRunOrThrow() override
    {
        BIND(&TBootstrap::DoFinishDryRun, this)
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
    }

private:
    NClusterNode::IBootstrap* const ClusterNodeBootstrap_;

    TActionQueuePtr TransactionTrackerQueue_;

    IResourceLimitsManagerPtr ResourceLimitsManager_;

    ICellarManagerPtr CellarManager_;

    IMasterConnectorPtr MasterConnector_;

    ICellarOccupantPtr DryRunOccupant_;

    void EnsureDryRunOccupantCreated()
    {
        if (DryRunOccupant_) {
            return;
        }

        auto cellId = GetConfig()->DryRun->TabletCellId;
        auto clockClusterTag = GetConfig()->DryRun->ClockClusterTag;
        YT_VERIFY(cellId);

        YT_LOG_EVENT(
            DryRunLogger,
            NLogging::ELogLevel::Info,
            "Creating dry-run occupant (CellId: %v, ClockClusterTag: %v)",
            cellId,
            clockClusterTag);

        DryRunOccupant_ = NTabletNode::CreateFakeOccupant(ClusterNodeBootstrap_, cellId, clockClusterTag);
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
        automaton->SetSnapshotValidationOptions({dumpSnapshot, false, nullptr});

        const auto& hydraManager = DryRunOccupant_->GetHydraManager();
        auto dryRunHydraManager = StaticPointerCast<IDryRunHydraManager>(hydraManager);
        dryRunHydraManager->DryRunLoadSnapshot(std::move(snapshotReader));
    }

    void DoReplayChangelogs(const std::vector<TString>& changelogFileNames)
    {
        EnsureDryRunOccupantCreated();
        const auto& hydraManager = DryRunOccupant_->GetHydraManager();
        auto dryRunHydraManager = StaticPointerCast<IDryRunHydraManager>(hydraManager);

        for (auto changelogFileName : changelogFileNames) {
            auto changelogId = TryFromString<int>(NFS::GetFileNameWithoutExtension(changelogFileName));
            if (changelogId.Empty()) {
                changelogId = InvalidSegmentId;
                YT_LOG_EVENT(DryRunLogger, NLogging::ELogLevel::Info, "Cannot parse changelog name as id, using id %v as substitute",
                    changelogId);
            }

            YT_LOG_EVENT(DryRunLogger, NLogging::ELogLevel::Info, "Started loading changelog");
            auto changelog = CreateJournalAsLocalFileReadOnlyChangelog(changelogFileName, *changelogId);
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
                cellarManagerConfig->Cellars.insert({ECellarType::Tablet, std::move(cellarConfig)});
                return cellarManagerConfig;
            }
        };

        CellarManager_->Reconfigure(getCellarManagerConfig());

        ResourceLimitsManager_->Reconfigure(newConfig->TabletNode->SecurityManager);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(NClusterNode::IBootstrap* bootstrap)
{
    return std::make_unique<TBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
