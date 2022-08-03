#include "bundle_controller.h"
#include "bundle_scheduler.h"

#include "bootstrap.h"
#include "config.h"
#include "cypress_bindings.h"

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/transaction.h>

namespace NYT::NCellBalancer {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NProfiling;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellBalancerLogger;
static const TString TabletCellBundlesPath("//sys/tablet_cell_bundles");
static const TString TabletNodesPath("//sys/tablet_nodes");
static const TString TabletCellsPath("//sys/tablet_cells");

////////////////////////////////////////////////////////////////////////////////

class TBundleController
    : public IBundleController
{
public:
    TBundleController(IBootstrap* bootstrap, TBundleControllerConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , Profiler("/bundle_controller")
        , SuccessfulScanBundlesCounter_(Profiler.Counter("/successful_scan_bundles_count"))
        , FailedScanBundlesCounter_(Profiler.Counter("/failed_scan_bundles_count"))
        , AlarmsCounter_(Profiler.Counter("/scan_bundles_alarms_count"))
    { }

    void Start() override
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        StartTime_ = TInstant::Now();

        YT_VERIFY(!PeriodicExecutor_);
        PeriodicExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TBundleController::ScanBundles, MakeWeak(this)),
            Config_->BundleScanPeriod);
        PeriodicExecutor_->Start();
    }

private:
    IBootstrap* const Bootstrap_;
    const TBundleControllerConfigPtr Config_;

    TCellTrackerImplPtr CellTrackerImpl_;

    TInstant StartTime_;
    TPeriodicExecutorPtr PeriodicExecutor_;

    const TProfiler Profiler;
    TCounter SuccessfulScanBundlesCounter_;
    TCounter FailedScanBundlesCounter_;
    TCounter AlarmsCounter_;

    void ScanBundles()
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        if (!IsLeader()) {
            YT_LOG_DEBUG("Bundle Controller is not leading");
            return;
        }

        try {
            YT_PROFILE_TIMING("/bundle_controller/scan_bundles") {
                DoScanBundles();
                SuccessfulScanBundlesCounter_.Increment();
            }
        } catch (TErrorException& ex) {
            YT_LOG_ERROR(ex, "Scanning bundles failed");
            FailedScanBundlesCounter_.Increment();
        }
    }

    std::vector<TString> GetAliveAllocationsId(const TSchedulerInputState& context)
    {
        std::vector<TString> result;
        for (const auto& [_, bundleState] : context.States) {
            for (const auto& [allocId, _] : bundleState->Allocations) {
                result.push_back(allocId);
            }
        }

        return result;
    }

    std::vector<TString> GetAliveDeallocationsId(const TSchedulerInputState& context)
    {
        std::vector<TString> result;
        for (const auto& [_, bundleState] : context.States) {
            for (const auto& [deallocId, _] : bundleState->Deallocations) {
                result.push_back(deallocId);
            }
        }

        return result;
    }

    void DoScanBundles()
    {
        YT_LOG_DEBUG("Starting scan bundles");

        ITransactionPtr transaction = CreateTransaction();
        auto context = GetInputState(transaction);
        TSchedulerMutatingContext mutations;

        ScheduleBundles(context, &mutations);
        Mutate(transaction, mutations);

        transaction->Commit();
    }

    inline static const TString  NODE_ATTRIBUTE_BUNDLE_CONTROLLER_ANNOTATIONS = "bundle_controller_annotations";
    inline static const TString  NODE_ATTRIBUTE_USER_TAGS = "user_tags";
    inline static const TString  NODE_ATTRIBUTE_DECOMMISSIONED = "decommissioned";

    void Mutate(const ITransactionPtr& transaction, const TSchedulerMutatingContext& mutations)
    {
        CreateHulkRequests<TAllocationRequest>(transaction, Config_->HulkAllocationsPath, mutations.NewAllocations);
        CreateHulkRequests<TDeallocationRequest>(transaction, Config_->HulkDeallocationsPath, mutations.NewDeallocations);
        CypressSet(transaction, GetBundlesStatePath(), mutations.ChangedStates);

        SetNodeAttributes(transaction, NODE_ATTRIBUTE_BUNDLE_CONTROLLER_ANNOTATIONS, mutations.ChangeNodeAnnotations);
        SetNodeAttributes(transaction, NODE_ATTRIBUTE_USER_TAGS, mutations.ChangedNodeUserTags);
        SetNodeAttributes(transaction, NODE_ATTRIBUTE_DECOMMISSIONED, mutations.ChangedDecommissionedFlag);

        CreateTabletCells(transaction, mutations.CellsToCreate);
        RemoveTabletCells(transaction, mutations.CellsToRemove);

        // TODO(capone212): Fire alarms.
        if (!mutations.AlertsToFire.empty()) {
            AlarmsCounter_.Increment(mutations.AlertsToFire.size());
        }
    }

    ITransactionPtr CreateTransaction()
    {
        TTransactionStartOptions transactionOptions;
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", "BundleController ScanBundles");
        transactionOptions.Attributes = std::move(attributes);
        transactionOptions.Timeout = Config_->BundleScanTransactionTimeout;

        return WaitFor(Bootstrap_
            ->GetClient()
            ->StartTransaction(ETransactionType::Master, transactionOptions))
            .ValueOrThrow();
    }

    TSchedulerInputState GetInputState(const ITransactionPtr& transaction)
    {
        TSchedulerInputState context{
            .Config = Config_,
        };

        context.Zones = CypressGet<TZoneInfo>(transaction, GetZonesPath());
        context.Bundles = CypressGet<TBundleInfo>(transaction, TabletCellBundlesPath);
        context.States = CypressGet<TBundleControllerState>(transaction, GetBundlesStatePath());
        context.TabletNodes = CypressGet<TTabletNodeInfo>(transaction, TabletNodesPath);
        context.TabletCells = CypressGet<TTabletCellInfo>(transaction, TabletCellsPath);

        context.AllocationRequests = LoadHulkRequests<TAllocationRequest>(transaction,
            {Config_->HulkAllocationsPath, Config_->HulkAllocationsHistoryPath},
            GetAliveAllocationsId(context));

        context.DeallocationRequests = LoadHulkRequests<TDeallocationRequest>(transaction,
            {Config_->HulkDeallocationsPath, Config_->HulkDeallocationsHistoryPath},
            GetAliveDeallocationsId(context));

        return context;
    }

    template <typename TEntryInfo>
    static TIndexedEntries<TEntryInfo> CypressGet(const ITransactionPtr& transaction, const TString& path)
    {
        TListNodeOptions options;
        options.Attributes = TEntryInfo::GetAttributes();

        auto yson = WaitFor(transaction->ListNode(path, options))
            .ValueOrThrow();
        auto entryList = ConvertTo<IListNodePtr>(yson);

        TIndexedEntries<TEntryInfo> result;
        for (const auto& entry : entryList->GetChildren()) {
            const auto& name = entry->AsString()->GetValue();
            result[name] = ConvertTo<TIntrusivePtr<TEntryInfo>>(&entry->Attributes());
        }

        return result;
    }

    template <typename TEntryInfo>
    void CypressSet(
        const ITransactionPtr& transaction,
        const TString& basePath,
        const TIndexedEntries<TEntryInfo>& entries)
    {
        for (const auto& [name, entry] : entries) {
            CypressSet(transaction, basePath, name, entry);
        }
    }

    template <typename TEntryInfoPtr>
    void CypressSet(
        const ITransactionPtr& transaction,
        const TString& basePath,
        const TString& name,
        const TEntryInfoPtr& entry)
    {
        auto path = Format("%v/%v", basePath, name);

        TCreateNodeOptions createOptions;
        createOptions.IgnoreExisting = true;
        createOptions.Recursive = true;
        WaitFor(transaction->CreateNode(path, EObjectType::MapNode, createOptions))
            .ThrowOnError();

        TMultisetAttributesNodeOptions options;
        WaitFor(transaction->MultisetAttributesNode(
            path + "/@",
            ConvertTo<IMapNodePtr>(entry),
            options))
            .ThrowOnError();
    }

    template <typename TEntryInfo>
    void CreateHulkRequests(
        const ITransactionPtr& transaction,
        const TString& basePath,
        const TIndexedEntries<TEntryInfo>& requests)
    {
        for (const auto& [requestId, requestBody] : requests) {
            auto path = Format("%v/%v", basePath, requestId);

            TCreateNodeOptions createOptions;
            createOptions.Attributes = CreateEphemeralAttributes();
            createOptions.Attributes->Set("value", ConvertToYsonString(requestBody));
            createOptions.Recursive = true;
            WaitFor(transaction->CreateNode(path, EObjectType::Document, createOptions))
                .ThrowOnError();
        }
    }

    template <typename TAttribute>
    void SetNodeAttributes(
        const ITransactionPtr& transaction,
        const TString& attributeName,
        const THashMap<TString, TAttribute>& attributes) {
        for (const auto& [nodeId, attribute] : attributes) {
            auto path = Format("%v/%v/@%v", TabletNodesPath, nodeId, attributeName);

            TSetNodeOptions setOptions;
            WaitFor(transaction->SetNode(path, ConvertToYsonString(attribute), setOptions))
                .ThrowOnError();
        }
    }

    template <typename TEntryInfo>
    TIndexedEntries<TEntryInfo> LoadHulkRequests(
        const ITransactionPtr& transaction,
        const std::vector<TString>& basePaths,
        const std::vector<TString>& requests)
    {
        TIndexedEntries<TEntryInfo> results;
        using TEntryInfoPtr = TIntrusivePtr<TEntryInfo>;

        for (const auto& requestId : requests) {
            auto request = LoadHulkRequest<TEntryInfoPtr>(transaction, basePaths, requestId);
            if (request) {
                results[requestId] = request;
            }
        }
        return results;
    }

    template <typename TEntryInfoPtr>
    TEntryInfoPtr LoadHulkRequest(
        const ITransactionPtr& transaction,
        const std::vector<TString>& basePaths,
        const TString& id)
    {
        for (const auto& basePath: basePaths) {
            auto path = basePath + "/" + id;
            if (!WaitFor(transaction->NodeExists(path)).ValueOrThrow()) {
                continue;
            }

            auto yson = WaitFor(transaction->GetNode(path))
                .ValueOrThrow();

            return ConvertTo<TEntryInfoPtr>(yson);
        }

        return {};
    }

    void CreateTabletCells(const ITransactionPtr& transaction, const THashMap<TString, int>& cellsToCreate) {
        for (auto& [bundleName, cellsCount] : cellsToCreate) {
            TCreateObjectOptions createOptions;
            createOptions.Attributes = CreateEphemeralAttributes();
            createOptions.Attributes->Set("tablet_cell_bundle", bundleName);

            for (int index = 0; index < cellsCount; ++index) {
                WaitFor(transaction->CreateObject(EObjectType::TabletCell, createOptions))
                    .ThrowOnError();
            }
        }
    }

    void RemoveTabletCells(const ITransactionPtr& transaction, const std::vector<TString>& cellsToRemove) {
        for (const auto& cellId : cellsToRemove) {
            WaitFor(transaction->RemoveNode(Format("%v/%v", TabletCellsPath, cellId)))
                .ThrowOnError();
        }
    }

    bool IsLeader()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetElectionManager()->IsLeader();
    }

    TYPath GetZonesPath()
    {
        return Config_->RootPath + "/zones";
    }

    TYPath GetBundlesStatePath()
    {
        return Config_->RootPath + "/bundles_state";
    }
};

////////////////////////////////////////////////////////////////////////////////

IBundleControllerPtr CreateBundleController(IBootstrap* bootstrap, TBundleControllerConfigPtr config)
{
    return New<TBundleController>(bootstrap, std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
