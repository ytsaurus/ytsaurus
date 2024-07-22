#include "input_manager.h"

#include "helpers.h"
#include "input_transactions_manager.h"
#include "table.h"
#include "task.h"

#include <yt/yt/server/controller_agent/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_scraper.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec_fetcher.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/object_client/helpers.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_slice_fetcher.h>
#include <yt/yt/ytlib/table_client/chunk_slice_size_fetcher.h>
#include <yt/yt/ytlib/table_client/chunk_slice_fetcher.h>
#include <yt/yt/ytlib/table_client/columnar_statistics_fetcher.h>
#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/table_client/check_schema_compatibility.h>

#include <yt/yt/client/scheduler/public.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NApi;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkPools;
using namespace NScheduler;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NNodeTrackerClient;

using NYT::ToProto;
using NYT::FromProto;

using NLogging::TLogger;
using NTableClient::NProto::TBoundaryKeysExt;
using NTableClient::NProto::THeavyColumnStatisticsExt;

using NApi::NNative::IClientPtr;

namespace {

template <class TReturn, class... TArgs>
auto MakeSafe(TWeakPtr<IInputManagerHost> weakHost, TCallback<TReturn(TArgs...)> callback)
{
    return BIND_NO_PROPAGATE(
        [weakHost, callback = std::move(callback)] (TArgs&&... args) {
            auto strongHost = weakHost.Lock();
            YT_VERIFY(strongHost);
            strongHost->InvokeSafely([&] () {
                callback.Run(std::forward<TArgs>(args)...);
            });
        });
}

bool AreAllElementsEqual(const auto& container, auto compare)
{
    auto invertCompare = [&compare] (const auto& lhs, const auto& rhs) {
        return !compare(lhs, rhs);
    };
    // std::adjacent_find with inverted compare function finds a pair which has different elements.
    return std::adjacent_find(container.begin(), container.end(), invertCompare) == container.end();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TInputManager::TStripeDescriptor::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Stripe);
    Persist(context, Cookie);
    Persist(context, Task);
}

////////////////////////////////////////////////////////////////////////////////

int TInputManager::TInputChunkDescriptor::GetTableIndex() const
{
    YT_VERIFY(!InputChunks.empty());
    return InputChunks[0]->GetTableIndex();
}

void TInputManager::TInputChunkDescriptor::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, InputStripes);
    Persist(context, InputChunks);
    Persist(context, State);
}

////////////////////////////////////////////////////////////////////////////////

TUnavailableChunksWatcher::TUnavailableChunksWatcher(std::vector<IFetcherChunkScraperPtr> chunkScrapers)
    : ChunkScrapers_(std::move(chunkScrapers))
{ }

i64 TUnavailableChunksWatcher::GetUnavailableChunkCount() const
{
    i64 chunkCount = 0;
    for (const auto& scraper : ChunkScrapers_) {
        // NB(coteeq): Can be null if UnavailableChunkStrategy != EUnavailableChunkAction::Wait.
        if (scraper) {
            chunkCount += scraper->GetUnavailableChunkCount();
        }
    }
    return chunkCount;
}

////////////////////////////////////////////////////////////////////////////////

TCombiningSamplesFetcher::TCombiningSamplesFetcher(
    std::vector<TSamplesFetcherPtr> samplesFetchers)
    : SamplesFetchers_(std::move(samplesFetchers))
{ }

TFuture<void> TCombiningSamplesFetcher::Fetch() const
{
    std::vector<TFuture<void>> fetchFutures;
    for (const auto& fetcher : SamplesFetchers_) {
        fetchFutures.push_back(fetcher->Fetch());
    }
    return AllSucceeded(fetchFutures);
}

std::vector<TSample> TCombiningSamplesFetcher::GetSamples() const
{
    std::vector<TSample> samples;
    for (const auto& fetcher : SamplesFetchers_) {
        const auto& newSamples = fetcher->GetSamples();
        samples.insert(
            samples.end(),
            newSamples.begin(),
            newSamples.end());
    }
    return samples;
}

////////////////////////////////////////////////////////////////////////////////

TNodeDirectoryBuilderFactory::TNodeDirectoryBuilderFactory(
    NProto::TJobSpecExt* jobSpecExt,
    TInputManagerPtr inputManager,
    EOperationType operationType)
    : JobSpecExt_(jobSpecExt)
    , InputManager_(std::move(inputManager))
    , IsRemoteCopy_(operationType == EOperationType::RemoteCopy)
{ }

std::shared_ptr<TNodeDirectoryBuilder> TNodeDirectoryBuilderFactory::GetNodeDirectoryBuilder(
    const TChunkStripePtr& stripe)
{
    YT_VERIFY(stripe->GetTableIndex() < std::ssize(InputManager_->InputTables_));
    const auto& clusterName = InputManager_->InputTables_[stripe->GetTableIndex()]->ClusterName;
    if (!IsRemoteCopy_ && IsLocal(clusterName)) {
        // Local node directory is filled by exe-node (see also job.proto).
        // Except for the RemoteCopy operations.
        return nullptr;
    }

    if (!Builders_.contains(clusterName)) {
        auto* protoNodeDirectory = JobSpecExt_->mutable_input_node_directory();
        if (!IsLocal(clusterName)) {
            auto* remoteClusterProto = &((*JobSpecExt_->mutable_remote_input_clusters())[clusterName.Underlying()]);
            remoteClusterProto->set_name(clusterName.Underlying());
            protoNodeDirectory = remoteClusterProto->mutable_node_directory();
        }
        Builders_.emplace(
            clusterName,
            std::make_shared<TNodeDirectoryBuilder>(
                InputManager_->GetClusterOrCrash(clusterName)->NodeDirectory(),
                protoNodeDirectory));
    }

    return Builders_[clusterName];
}

////////////////////////////////////////////////////////////////////////////////

TInputCluster::TInputCluster(
    TClusterName name,
    TLogger logger)
    : Name(std::move(name))
    , Logger(logger.WithTag("Cluster: %v", Name))
{ }

void TInputCluster::InitializeClient(IClientPtr localClient)
{
    if (IsLocal(Name)) {
        Client_ = localClient;
    } else {
        Client_ = localClient
            ->GetNativeConnection()
            ->GetClusterDirectory()
            ->GetConnectionOrThrow(Name.Underlying())
            ->CreateNativeClient(localClient->GetOptions());
    }
}

void TInputCluster::ReportIfHasUnavailableChunks() const
{
    if (!UnavailableInputChunkIds_.empty()) {
        YT_LOG_INFO(
            "Have pending unavailable input chunks (Cluster: %v, SampleChunkIds: %v)",
            Name,
            MakeShrunkFormattableView(UnavailableInputChunkIds_, TDefaultFormatter(), SampleChunkIdCount));
    }
}

void TInputCluster::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Name);
    Persist(context, Logger);
    Persist(context, NodeDirectory_);
    Persist(context, InputTables_);
    Persist(context, UnavailableInputChunkIds_);
    Persist(context, PathToInputTables_);
}

////////////////////////////////////////////////////////////////////////////////

TInputManager::TInputManager(IInputManagerHost* host, TLogger logger)
    : Host_(host)
    , Logger(std::move(logger))
{ }

void TInputManager::InitializeClients(IClientPtr client)
{
    for (const auto& [_, cluster] : Clusters_) {
        cluster->InitializeClient(client);
    }
}

void TInputManager::InitializeStructures(
    IClientPtr client,
    const TInputTransactionsManagerPtr& inputTransactionsManager)
{
    YT_VERIFY(inputTransactionsManager);
    InputTables_.clear();
    Clusters_.clear();
    ClusterResolver_ = inputTransactionsManager->GetClusterResolver();

    auto ensureCluster = [&](const auto& name) {
        if (!Clusters_.contains(name)) {
            const auto& cluster = Clusters_.emplace(name, New<TInputCluster>(name, Logger)).first->second;
            cluster->InitializeClient(client);
        }
    };

    for (const auto& path : Host_->GetInputTablePaths()) {
        auto clusterName = ClusterResolver_->GetClusterName(path);
        auto table = New<TInputTable>(
            path,
            inputTransactionsManager->GetTransactionIdForObject(path));
        table->ColumnRenameDescriptors = path.GetColumnRenameDescriptors().value_or(TColumnRenameDescriptors());
        table->ClusterName = clusterName;
        ensureCluster(clusterName);

        GetClusterOrCrash(clusterName)->InputTables().push_back(table);
        InputTables_.push_back(table);
    }

    if (inputTransactionsManager->GetLocalInputTransactionId()) {
        ensureCluster(LocalClusterName);
    }
}

void TInputCluster::LockTables()
{
    //! TODO(ignat): Merge in with lock input files method.
    YT_LOG_INFO("Locking input tables");

    auto proxy = CreateObjectServiceWriteProxy(Client_);
    auto batchReq = proxy.ExecuteBatchWithRetries(Client_->GetNativeConnection()->GetConfig()->ChunkFetchRetries);

    for (const auto& table : InputTables_) {
        auto req = TTableYPathProxy::Lock(table->GetPath());
        req->Tag() = table;
        req->set_mode(ToProto<int>(ELockMode::Snapshot));
        SetTransactionId(req, *table->TransactionId);
        GenerateMutationId(req);
        batchReq->AddRequest(req);
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error locking input tables");

    const auto& batchRsp = batchRspOrError.Value();
    for (const auto& rspOrError : batchRsp->GetResponses<TCypressYPathProxy::TRspLock>()) {
        const auto& rsp = rspOrError.Value();
        auto table = std::any_cast<TInputTablePtr>(rsp->Tag());
        table->ObjectId = FromProto<TObjectId>(rsp->node_id());
        table->Revision = rsp->revision();
        table->ExternalCellTag = FromProto<TCellTag>(rsp->external_cell_tag());
        table->ExternalTransactionId = rsp->has_external_transaction_id()
            ? FromProto<TTransactionId>(rsp->external_transaction_id())
            : *table->TransactionId;
        PathToInputTables_[table->GetPath()].push_back(table);
    }
}

void TInputManager::LockInputTables()
{
    std::vector<TFuture<void>> lockFutures;
    for (const auto& [name, cluster] : Clusters_) {
        lockFutures.push_back(
            BIND(&TInputCluster::LockTables, cluster)
                .AsyncVia(GetCurrentInvoker())
                .Run());
    }
    WaitFor(AllSucceeded(lockFutures))
        .ThrowOnError();
}

void TInputCluster::ValidateOutputTableLockedCorrectly(const TOutputTablePtr& outputTable) const
{
    if (auto it = PathToInputTables_.find(outputTable->GetPath())) {
        for (const auto& inputTable : it->second) {
            // NB: remote copy is a special case.
            // XXX(coteeq): Is it possible for table to change its cell, but remain on the same cluster?
            if (CellTagFromId(inputTable->ObjectId) != CellTagFromId(outputTable->ObjectId)) {
                continue;
            }
            if (inputTable->ObjectId != outputTable->ObjectId || inputTable->Revision != outputTable->Revision) {
                THROW_ERROR_EXCEPTION(
                    NScheduler::EErrorCode::OperationFailedWithInconsistentLocking,
                    "Table %v has changed between taking input and output locks",
                    inputTable->GetPath())
                    << TErrorAttribute("input_object_id", inputTable->ObjectId)
                    << TErrorAttribute("input_revision", inputTable->Revision)
                    << TErrorAttribute("output_object_id", outputTable->ObjectId)
                    << TErrorAttribute("output_revision", outputTable->Revision);
            }
        }
    }
}

void TInputManager::ValidateOutputTableLockedCorrectly(const TOutputTablePtr& outputTable) const
{
    if (auto clusterIter = Clusters_.find(LocalClusterName); clusterIter != Clusters_.end()) {
        clusterIter->second->ValidateOutputTableLockedCorrectly(outputTable);
    }
}

IFetcherChunkScraperPtr TInputManager::CreateFetcherChunkScraper(const TInputClusterPtr& cluster) const
{
    return Host_->GetSpec()->UnavailableChunkStrategy == EUnavailableChunkAction::Wait
        ? NChunkClient::CreateFetcherChunkScraper(
            Host_->GetConfig()->ChunkScraper,
            Host_->GetChunkScraperInvoker(),
            Host_->GetChunkLocationThrottlerManager(),
            cluster->Client(),
            cluster->NodeDirectory(),
            Logger)
        : nullptr;
}

IFetcherChunkScraperPtr TInputManager::CreateFetcherChunkScraper(const TClusterName& clusterName) const
{
    return CreateFetcherChunkScraper(GetOrCrash(Clusters_, clusterName));
}

TMasterChunkSpecFetcherPtr TInputManager::CreateChunkSpecFetcher(const TInputClusterPtr& cluster) const
{
    TPeriodicYielder yielder(PrepareYieldPeriod);

    auto chunkSpecFetcher = New<TMasterChunkSpecFetcher>(
        cluster->Client(),
        TMasterReadOptions{},
        cluster->NodeDirectory(),
        Host_->GetCancelableInvoker(EOperationControllerQueue::Default),
        Host_->GetConfig()->MaxChunksPerFetch,
        Host_->GetConfig()->MaxChunksPerLocateRequest,
        [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& req, int tableIndex) {
            const auto& table = InputTables_[tableIndex];
            req->set_fetch_all_meta_extensions(false);
            req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);

            if (table->Path.GetColumns() && Host_->GetSpec()->InputTableColumnarStatistics->Enabled.value_or(Host_->GetConfig()->UseColumnarStatisticsDefault)) {
                req->add_extension_tags(TProtoExtensionTag<THeavyColumnStatisticsExt>::Value);
            }
            if (table->Dynamic || Host_->IsBoundaryKeysFetchEnabled()) {
                req->add_extension_tags(TProtoExtensionTag<TBoundaryKeysExt>::Value);
            }
            if (table->Dynamic) {
                if (!Host_->GetSpec()->EnableDynamicStoreRead.value_or(true)) {
                    req->set_omit_dynamic_stores(true);
                }
                if (Host_->GetOperationType() == EOperationType::RemoteCopy) {
                    req->set_throw_on_chunk_views(true);
                }
            }
            // NB: we always fetch parity replicas since
            // erasure reader can repair data on flight.
            req->set_fetch_parity_replicas(true);
            AddCellTagToSyncWith(req, table->ObjectId);
            SetTransactionId(req, table->ExternalTransactionId);
        },
        Logger);

    for (int tableIndex = 0; tableIndex < std::ssize(InputTables_); ++tableIndex) {
        yielder.TryYield();

        const auto& table = InputTables_[tableIndex];
        // NB(coteeq): I could've used cluster.InputTables, but I need
        // passthrough numbering along InputTables_.
        if (table->ClusterName != cluster->Name) {
            continue;
        }
        auto ranges = table->Path.GetNewRanges(table->Comparator, table->Schema->GetKeyColumnTypes());

        // XXX(max42): does this ever happen?
        if (ranges.empty()) {
            continue;
        }

        bool hasColumnSelectors = table->Path.GetColumns().operator bool();

        if (std::ssize(ranges) > Host_->GetConfig()->MaxRangesOnTable) {
            THROW_ERROR_EXCEPTION(
                "Too many ranges on table: maximum allowed %v, actual %v",
                Host_->GetConfig()->MaxRangesOnTable,
                ranges.size())
                << TErrorAttribute("table_path", table->Path);
        }

        YT_LOG_DEBUG("Adding input table for fetch (Path: %v, Id: %v, Dynamic: %v, ChunkCount: %v, RangeCount: %v, "
            "HasColumnSelectors: %v, EnableDynamicStoreRead: %v)",
            table->GetPath(),
            table->ObjectId,
            table->Dynamic,
            table->ChunkCount,
            ranges.size(),
            hasColumnSelectors,
            Host_->GetSpec()->EnableDynamicStoreRead);

        chunkSpecFetcher->Add(
            table->ObjectId,
            table->ExternalCellTag,
            table->Dynamic && !table->Schema->IsSorted() ? -1 : table->ChunkCount,
            tableIndex,
            ranges);
    }

    return chunkSpecFetcher;
}

TFetchInputTablesStatistics TInputManager::FetchInputTables()
{
    TPeriodicYielder yielder(PrepareYieldPeriod);

    TFetchInputTablesStatistics fetchStatistics;

    bool useChunkSliceStatistics = true;
    if (auto error = Host_->GetUseChunkSliceStatisticsError(); Host_->GetSpec()->UseChunkSliceStatistics && !error.IsOK()) {
        Host_->SetOperationAlert(EOperationAlertType::UseChunkSliceStatisticsDisabled, error);
        useChunkSliceStatistics = false;
    }

    THashMap<TClusterName, TColumnarStatisticsFetcherPtr> columnarStatisticsFetchers;
    THashMap<TClusterName, TChunkSliceSizeFetcherPtr> chunkSliceSizeFetchers;

    for (const auto& [clusterName, cluster] : Clusters_) {
        columnarStatisticsFetchers[clusterName] = New<TColumnarStatisticsFetcher>(
            Host_->GetChunkScraperInvoker(),
            cluster->Client(),
            TColumnarStatisticsFetcher::TOptions{
                .Config = Host_->GetConfig()->Fetcher,
                .NodeDirectory = cluster->NodeDirectory(),
                .ChunkScraper = CreateFetcherChunkScraper(cluster),
                .Mode = Host_->GetSpec()->InputTableColumnarStatistics->Mode,
                .EnableEarlyFinish = Host_->GetConfig()->EnableColumnarStatisticsEarlyFinish,
                .Logger = Logger,
            });

        if (useChunkSliceStatistics) {
            chunkSliceSizeFetchers[clusterName] = New<TChunkSliceSizeFetcher>(
                Host_->GetConfig()->Fetcher,
                cluster->NodeDirectory(),
                Host_->GetChunkScraperInvoker(),
                /*chunkScraper*/ CreateFetcherChunkScraper(cluster),
                cluster->Client(),
                Logger);
        }
    }

    YT_LOG_INFO("Fetching input tables");

    THashMap<TClusterName, TMasterChunkSpecFetcherPtr> chunkSpecFetchers;
    std::vector<TFuture<void>> fetchChunkSpecFutures;
    for (const auto& [clusterName, cluster] : Clusters_) {
        auto fetcher = CreateChunkSpecFetcher(cluster);
        chunkSpecFetchers.emplace(clusterName, fetcher);
        fetchChunkSpecFutures.push_back(fetcher->Fetch());
    }

    // TODO(coteeq): This is a barrier and probably deserves to be removed.
    WaitFor(AllSucceeded(fetchChunkSpecFutures))
        .ThrowOnError();

    YT_LOG_INFO("Input tables fetched");

    auto processChunk = [&] (const NChunkClient::NProto::TChunkSpec& chunkSpec) {
        yielder.TryYield();

        int tableIndex = chunkSpec.table_index();
        auto& table = InputTables_[tableIndex];
        auto& columnarStatisticsFetcher = columnarStatisticsFetchers[table->ClusterName];
        auto& chunkSliceSizeFetcher = chunkSliceSizeFetchers[table->ClusterName];

        auto inputChunk = New<TInputChunk>(
            chunkSpec,
            /*keyColumnCount*/ table->Comparator.GetLength());
        inputChunk->SetTableIndex(tableIndex);
        inputChunk->SetChunkIndex(fetchStatistics.ChunkCount++);

        if (inputChunk->IsDynamicStore() && !table->Schema->IsSorted()) {
            if (!InputHasOrderedDynamicStores_) {
                YT_LOG_DEBUG("Operation input has ordered dynamic stores, job interrupts "
                    "are disabled (TableId: %v, TablePath: %v)",
                    table->ObjectId,
                    table->GetPath());
                InputHasOrderedDynamicStores_ = true;
            }
        }

        if (inputChunk->GetRowCount() > 0 || inputChunk->IsFile()) {
            // Input chunks may have zero row count in case of unsensible read range with coinciding
            // lower and upper row index. We skip such chunks.
            // NB(coteeq): File chunks have zero rows as well, but we want to be able to remote_copy them.
            table->Chunks.emplace_back(inputChunk);
            for (const auto& extension : chunkSpec.chunk_meta().extensions().extensions()) {
                fetchStatistics.ExtensionSize += extension.data().size();
            }
            RegisterInputChunk(table->Chunks.back());

            bool shouldSkipChunkInFetchers = IsUnavailable(inputChunk, Host_->GetChunkAvailabilityPolicy()) && Host_->GetSpec()->UnavailableChunkStrategy == EUnavailableChunkAction::Skip;

            // We only fetch chunk slice sizes for unversioned table chunks with non-trivial limits.
            // We do not fetch slice sizes in cases when ChunkSliceFetcher should later be used, since it performs similar computations and will misuse the scaling factors.
            // To be more exact, we do not fetch slice sizes in operations that use any of the two sorted controllers.
            bool willFetchChunkSliceStatistics =
                !shouldSkipChunkInFetchers &&
                chunkSliceSizeFetcher &&
                !table->IsVersioned() &&
                !inputChunk->IsCompleteChunk() &&
                Host_->GetSpec()->UseChunkSliceStatistics;
            if (willFetchChunkSliceStatistics) {
                YT_VERIFY(!inputChunk->IsFile());

                if (auto columns = table->Path.GetColumns()) {
                    chunkSliceSizeFetcher->AddChunk(inputChunk, MapNamesToStableNames(*table->Schema, *columns, NonexistentColumnName));
                } else {
                    chunkSliceSizeFetcher->AddChunk(inputChunk);
                }
            }

            // We fetch columnar statistics only for the tables that have column selectors specified.
            // NB: We do not fetch columnar statistics for chunks for which chunk slice statistics are fetched
            // because chunk slice statistics already take columns into consideration.
            auto hasColumnSelectors = table->Path.GetColumns().has_value();
            if (!shouldSkipChunkInFetchers &&
                !willFetchChunkSliceStatistics &&
                hasColumnSelectors &&
                Host_->GetSpec()->InputTableColumnarStatistics->Enabled.value_or(Host_->GetConfig()->UseColumnarStatisticsDefault))
            {
                auto stableColumnNames = MapNamesToStableNames(
                    *table->Schema,
                    *table->Path.GetColumns(),
                    NonexistentColumnName);
                columnarStatisticsFetcher->AddChunk(inputChunk, stableColumnNames);
            }

            if (hasColumnSelectors) {
                inputChunk->SetValuesPerRow(table->Path.GetColumns()->size());
            } else if (table->Schema && table->Schema->GetStrict()) {
                inputChunk->SetValuesPerRow(table->Schema->Columns().size());
            }
        }
    };

    for (const auto& [_, chunkSpecFetcher] : chunkSpecFetchers) {
        for (const auto& chunkSpec : chunkSpecFetcher->ChunkSpecs()) {
            processChunk(chunkSpec);
        }
    }


    std::vector<TFuture<void>> statisticsFutures;
    for (const auto& [clusterName, fetcher] : columnarStatisticsFetchers) {
        if (fetcher->GetChunkCount() > 0) {
            YT_LOG_INFO("Fetching chunk columnar statistics for tables with column selectors (Cluster: %v, ChunkCount: %v)",
                clusterName,
                fetcher->GetChunkCount());
            Host_->MaybeCancel(ECancelationStage::ColumnarStatisticsFetch);
            fetcher->SetCancelableContext(Host_->GetCancelableContext());

            auto applySelectivityFactors =
                BIND([fetcher, Logger = GetClusterOrCrash(clusterName)->Logger] {
                    YT_LOG_INFO("Columnar statistics fetched");
                    fetcher->ApplyColumnSelectivityFactors();
                })
                    .AsyncVia(GetCurrentInvoker());

            statisticsFutures.push_back(
                fetcher->Fetch().Apply(applySelectivityFactors));
        }
    }

    for (const auto& [clusterName, fetcher] : chunkSliceSizeFetchers) {
        if (fetcher && fetcher->GetChunkCount() > 0) {
            YT_LOG_INFO("Fetching input chunk slice statistics for input tables (Cluster: %v, ChunkCount: %v)",
                clusterName,
                fetcher->GetChunkCount());
            fetcher->SetCancelableContext(Host_->GetCancelableContext());

            auto applySelectivityFactors =
                BIND([fetcher, Logger = GetClusterOrCrash(clusterName)->Logger] {
                    YT_LOG_INFO("Input chunk slice statistics fetched");
                    fetcher->ApplyBlockSelectivityFactors();
                })
                    .AsyncVia(GetCurrentInvoker());

            statisticsFutures.push_back(
                fetcher->Fetch().Apply(applySelectivityFactors));
        }
    }

    WaitFor(AllSucceeded(statisticsFutures))
        .ThrowOnError();

    YT_LOG_INFO("All statistics fetched from nodes");

    // TODO(galtsev): remove after YT-20281 is fixed
    if (AnyOf(InputTables_, IsStaticTableWithHunks)) {
        InputHasStaticTableWithHunks_ = true;
        YT_LOG_INFO("Static tables with hunks found, disabling job splitting");
    }

    return fetchStatistics;
}

void TInputManager::Prepare()
{
    if (!InputTables_.empty()) {
        FetchInputTablesAttributes();
        for (const auto& table : InputTables_) {
            if (table->IsFile()) {
                TError error;
                if (table->Path.GetColumns()) {
                    error = TError("Input file path must not contain column selectors");
                }
                if (
                    table->Path.Attributes().Contains("upper_limit") ||
                    table->Path.Attributes().Contains("lower_limit") ||
                    table->Path.Attributes().Contains("ranges")) {
                    error = TError("Input file path must not contain row selectors");
                }

                if (!error.IsOK()) {
                    error <<= TErrorAttribute("path", table->Path);
                    THROW_ERROR error;
                }
            }
        }
    } else {
        YT_LOG_INFO("Operation has no input tables");
    }
}

void TInputManager::FetchInputTablesAttributes()
{
    YT_LOG_INFO("Getting input tables attributes");

    // NB(coteeq): Transaction id may still be NullTransactionId.
    YT_VERIFY(
        AllOf(
            InputTables_,
            [] (const auto& table) {
                return table->TransactionId.has_value();
            }));

    std::vector<TFuture<void>> getBasicAttributesFutures;
    for (const auto& [_, cluster] : Clusters_) {
        auto getAttributes = BIND([&] {
            GetUserObjectBasicAttributes(
                cluster->Client(),
                MakeUserObjectList(cluster->InputTables()),
                /*defaultTransactionId*/ NullTransactionId,
                Logger,
                EPermission::Read,
                TGetUserObjectBasicAttributesOptions{
                    .OmitInaccessibleColumns = Host_->GetSpec()->OmitInaccessibleColumns,
                    .PopulateSecurityTags = true,
                });
        });

        getBasicAttributesFutures.push_back(
            getAttributes
                .AsyncVia(GetCurrentInvoker())
                .Run());
    }

    WaitFor(AllSucceeded(getBasicAttributesFutures))
        .ThrowOnError();

    Host_->ValidateInputTablesTypes();

    std::vector<TYsonString> omittedInaccessibleColumnsList;
    for (const auto& table : InputTables_) {
        if (!table->OmittedInaccessibleColumns.empty()) {
            omittedInaccessibleColumnsList.push_back(BuildYsonStringFluently()
                .BeginMap()
                    .Item("path").Value(table->GetPath())
                    .Item("columns").Value(table->OmittedInaccessibleColumns)
                .EndMap());
        }
    }
    if (!omittedInaccessibleColumnsList.empty()) {
        auto error = TError("Some columns of input tables are inaccessible and were omitted")
            << TErrorAttribute("input_tables", omittedInaccessibleColumnsList);
        Host_->SetOperationAlert(EOperationAlertType::OmittedInaccessibleColumnsInInputTables, error);
    }

    THashMap<TCellTag, std::vector<TInputTablePtr>> externalCellTagToTables;
    for (const auto& table : InputTables_) {
        externalCellTagToTables[table->ExternalCellTag].push_back(table);
    }

    std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
    for (const auto& [externalCellTag, tables] : externalCellTagToTables) {
        YT_VERIFY(
            AreAllElementsEqual(
                tables,
                [] (const auto& lhs, const auto& rhs) {
                    return lhs->ClusterName == rhs->ClusterName;
                }));
        auto proxy = CreateObjectServiceReadProxy(
            GetClusterOrCrash(tables[0]->ClusterName)->Client(),
            EMasterChannelKind::Follower,
            externalCellTag);
        auto batchReq = proxy.ExecuteBatch();
        for (const auto& table : tables) {
            auto req = TTableYPathProxy::Get(table->GetObjectIdPath() + "/@");
            ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
                "dynamic",
                "chunk_count",
                "retained_timestamp",
                "schema_mode",
                "schema_id",
                "unflushed_timestamp",
                "content_revision",
                "enable_dynamic_store_read",
                "tablet_state",
                "account",
            });
            AddCellTagToSyncWith(req, table->ObjectId);
            SetTransactionId(req, table->ExternalTransactionId);
            req->Tag() = table;
            batchReq->AddRequest(req);
        }

        asyncResults.push_back(batchReq->Invoke());
    }

    auto checkError = [] (const auto& error) {
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error getting attributes of input tables");
    };

    auto result = WaitFor(AllSucceeded(asyncResults));
    checkError(result);

    THashMap<TInputTablePtr, IAttributeDictionaryPtr> tableAttributes;
    for (const auto& batchRsp : result.Value()) {
        checkError(GetCumulativeError(batchRsp));
        for (const auto& rspOrError : batchRsp->GetResponses<TTableYPathProxy::TRspGet>()) {
            const auto& rsp = rspOrError.Value();
            auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
            auto table = std::any_cast<TInputTablePtr>(rsp->Tag());
            tableAttributes.emplace(std::move(table), std::move(attributes));
        }
    }

    bool needFetchSchemas = !InputTables_.empty() && InputTables_[0]->Type == EObjectType::Table;

    if (needFetchSchemas) {
        // Fetch the schemas based on schema IDs. We didn't fetch the schemas initially to allow deduplication
        // if there are multiple tables sharing same schema.
        for (const auto& [table, attributes] : tableAttributes) {
            table->SchemaId = attributes->Get<TGuid>("schema_id");
        }

        std::vector<TFuture<void>> fetchSchemasFutures;
        for (const auto& [_, cluster] : Clusters_) {
            fetchSchemasFutures.push_back(
                BIND([client = cluster->Client(), tables = cluster->InputTables()] {
                    FetchTableSchemas(
                        client,
                        tables,
                        BIND([] (const TInputTablePtr& table) { return table->ExternalTransactionId; }),
                        /*fetchFromExternalCells*/ true);
                })
                    .AsyncVia(GetCurrentInvoker())
                    .Run());
        }

        WaitFor(AllSucceeded(fetchSchemasFutures))
            .ThrowOnError();
    }

    bool haveTablesWithEnabledDynamicStoreRead = false;

    for (const auto& [table, attributes] : tableAttributes) {
        table->ChunkCount = attributes->Get<int>("chunk_count");
        table->ContentRevision = attributes->Get<NHydra::TRevision>("content_revision");
        table->Account = attributes->Get<TString>("account");
        if (table->Type == EObjectType::File) {
            // NB(coteeq): Files have none of the following attributes.
            continue;
        }

        table->Dynamic = attributes->Get<bool>("dynamic");
        if (table->Schema->IsSorted()) {
            table->Comparator = table->Schema->ToComparator();
        }
        table->SchemaMode = attributes->Get<ETableSchemaMode>("schema_mode");

        haveTablesWithEnabledDynamicStoreRead |= attributes->Get<bool>("enable_dynamic_store_read", false);

        // Validate that timestamp is correct.
        ValidateDynamicTableTimestamp(
            table->Path,
            table->Dynamic,
            *table->Schema,
            *attributes,
            !Host_->GetSpec()->EnableDynamicStoreRead.value_or(true));

        YT_LOG_INFO("Input table locked (Path: %v, ObjectId: %v, Schema: %v, Dynamic: %v, ChunkCount: %v, SecurityTags: %v, "
            "Revision: %x, ContentRevision: %x)",
            table->GetPath(),
            table->ObjectId,
            *table->Schema,
            table->Dynamic,
            table->ChunkCount,
            table->SecurityTags,
            table->Revision,
            table->ContentRevision);

        if (!table->ColumnRenameDescriptors.empty()) {
            if (table->Path.GetTeleport()) {
                THROW_ERROR_EXCEPTION("Cannot rename columns in table with teleport")
                    << TErrorAttribute("table_path", table->Path);
            }
            YT_LOG_DEBUG("Start renaming columns of input table");
            auto description = Format("input table %v", table->GetPath());
            table->Schema = RenameColumnsInSchema(
                description,
                table->Schema,
                table->Dynamic,
                table->ColumnRenameDescriptors,
                /*changeStableName*/ !Host_->GetConfig()->EnableTableColumnRenaming);
            YT_LOG_DEBUG("Columns of input table are renamed (Path: %v, NewSchema: %v)",
                table->GetPath(),
                *table->Schema);
        }

        if (table->Dynamic && Host_->GetOperationType() == EOperationType::RemoteCopy) {
            if (!Host_->GetConfig()->EnableVersionedRemoteCopy) {
                THROW_ERROR_EXCEPTION("Remote copy for dynamic tables is disabled");
            }

            auto tabletState = attributes->Get<ETabletState>("tablet_state");
            if (tabletState != ETabletState::Frozen && tabletState != ETabletState::Unmounted) {
                THROW_ERROR_EXCEPTION("Input table has tablet state %Qlv: expected %Qlv or %Qlv",
                    tabletState,
                    ETabletState::Frozen,
                    ETabletState::Unmounted)
                    << TErrorAttribute("table_path", table->Path);
            }
        }

        // TODO(ifsmirnov): YT-20044
        if (table->Schema->HasHunkColumns() && Host_->GetOperationType() == EOperationType::RemoteCopy) {
            if (!Host_->GetSpec()->BypassHunkRemoteCopyProhibition.value_or(false)) {
                THROW_ERROR_EXCEPTION("Table with hunk columns cannot be copied to another cluster")
                    << TErrorAttribute("table_path", table->Path);
            }
        }
    }

    if (Host_->GetSpec()->EnableDynamicStoreRead == true && !haveTablesWithEnabledDynamicStoreRead) {
        Host_->SetOperationAlert(
            EOperationAlertType::NoTablesWithEnabledDynamicStoreRead,
            TError(
                "enable_dynamic_store_read in operation spec set to true, "
                "but no input tables have @enable_dynamic_store_read attribute set"));
    }
}

void TInputManager::InitInputChunkScrapers()
{
    THashMap<TInputClusterPtr, THashSet<TChunkId>> clusterToChunkIds;
    for (const auto& [chunkId, chunkDescriptor] : InputChunkMap_) {
        if (!IsDynamicTabletStoreType(TypeFromId(chunkId))) {
            auto cluster = GetClusterOrCrash(chunkDescriptor);
            clusterToChunkIds[cluster].insert(chunkId);
        }
    }

    for (const auto& [clusterName, cluster] : Clusters_) {
        YT_VERIFY(!cluster->ChunkScraper());
        cluster->ChunkScraper() = New<TChunkScraper>(
            Host_->GetConfig()->ChunkScraper,
            Host_->GetChunkScraperInvoker(),
            Host_->GetChunkLocationThrottlerManager(),
            cluster->Client(),
            cluster->NodeDirectory(),
            std::move(clusterToChunkIds[cluster]),
            MakeSafe(
                MakeWeak(Host_),
                BIND(&TInputManager::OnInputChunkLocated, MakeWeak(this)))
                    .Via(Host_->GetCancelableInvoker()),
            Logger);
        if (!cluster->UnavailableInputChunkIds().empty()) {
            YT_LOG_INFO(
                "Waiting for unavailable input chunks (Cluster: %v, Count: %v, SampleIds: %v)",
                clusterName,
                cluster->UnavailableInputChunkIds().size(),
                MakeShrunkFormattableView(cluster->UnavailableInputChunkIds(), TDefaultFormatter(), SampleChunkIdCount));
            cluster->ChunkScraper()->Start();
        }
    }
}

bool TInputManager::CanInterruptJobs() const
{
    return !InputHasOrderedDynamicStores_ && !InputHasStaticTableWithHunks_;
}

const std::vector<TInputTablePtr>& TInputManager::GetInputTables() const
{
    return InputTables_;
}

void TInputManager::RegisterInputStripe(
    const TChunkStripePtr& stripe,
    const TTaskPtr& task)
{
    THashSet<TChunkId> visitedChunks;

    TStripeDescriptor stripeDescriptor;
    stripeDescriptor.Stripe = stripe;
    stripeDescriptor.Task = task;
    stripeDescriptor.Cookie = task->GetChunkPoolInput()->Add(stripe);

    for (const auto& dataSlice : stripe->DataSlices) {
        for (const auto& slice : dataSlice->ChunkSlices) {
            auto inputChunk = slice->GetInputChunk();
            auto chunkId = inputChunk->GetChunkId();

            if (!visitedChunks.insert(chunkId).second) {
                continue;
            }

            auto& chunkDescriptor = GetOrCrash(InputChunkMap_, chunkId);
            chunkDescriptor.InputStripes.push_back(stripeDescriptor);

            if (chunkDescriptor.State == EInputChunkState::Waiting) {
                ++stripe->WaitingChunkCount;
            }
        }
    }

    if (stripe->WaitingChunkCount > 0) {
        task->GetChunkPoolInput()->Suspend(stripeDescriptor.Cookie);
    }
}

TInputChunkPtr TInputManager::GetInputChunk(NChunkClient::TChunkId chunkId, int chunkIndex) const
{
    const auto& inputChunks = GetOrCrash(InputChunkMap_, chunkId).InputChunks;
    auto chunkIt = std::find_if(
        inputChunks.begin(),
        inputChunks.end(),
        [&] (const TInputChunkPtr& inputChunk) -> bool {
            return inputChunk->GetChunkIndex() == chunkIndex;
        });
    YT_VERIFY(chunkIt != inputChunks.end());
    return *chunkIt;
}

void TInputManager::OnInputChunkLocated(
    TChunkId chunkId,
    const TChunkReplicaWithMediumList& replicas,
    bool missing)
{
    VERIFY_INVOKER_AFFINITY(Host_->GetCancelableInvoker());

    if (missing) {
        // We must have locked all the relevant input chunks, but when user transaction is aborted
        // there can be a race between operation completion and chunk scraper.
        Host_->OnOperationFailed(TError("Input chunk %v is missing", chunkId));
        return;
    }

    ++ChunkLocatedCallCount_;
    if (ChunkLocatedCallCount_ >= Host_->GetConfig()->ChunkScraper->MaxChunksPerRequest) {
        ChunkLocatedCallCount_ = 0;
        YT_LOG_DEBUG(
            "Located another batch of chunks (Count: %v)",
            Host_->GetConfig()->ChunkScraper->MaxChunksPerRequest);

        for (const auto& [_, cluster] : Clusters_) {
            cluster->ReportIfHasUnavailableChunks();
        }
    }

    auto& descriptor = GetOrCrash(InputChunkMap_, chunkId);
    YT_VERIFY(!descriptor.InputChunks.empty());

    const auto& chunkSpec = descriptor.InputChunks.front();
    auto codecId = chunkSpec->GetErasureCodec();

    if (IsUnavailable(replicas, codecId, Host_->GetChunkAvailabilityPolicy())) {
        OnInputChunkUnavailable(chunkId, &descriptor);
    } else {
        OnInputChunkAvailable(chunkId, replicas, &descriptor);
    }
}

void TInputManager::OnInputChunkAvailable(
    TChunkId chunkId,
    const TChunkReplicaWithMediumList& replicas,
    TInputChunkDescriptor* descriptor)
{
    VERIFY_INVOKER_AFFINITY(Host_->GetCancelableInvoker(EOperationControllerQueue::Default));

    if (descriptor->State != EInputChunkState::Waiting) {
        return;
    }

    UnregisterUnavailableInputChunk(chunkId);

    auto& cluster = GetClusterOrCrash(*descriptor);
    if (cluster->UnavailableInputChunkIds().empty()) {
        YT_UNUSED_FUTURE(cluster->ChunkScraper()->Stop());
    }

    // Update replicas in place for all input chunks with current chunkId.
    for (const auto& chunkSpec : descriptor->InputChunks) {
        chunkSpec->SetReplicaList(replicas);
    }

    descriptor->State = EInputChunkState::Active;

    for (const auto& inputStripe : descriptor->InputStripes) {
        --inputStripe.Stripe->WaitingChunkCount;
        if (inputStripe.Stripe->WaitingChunkCount > 0) {
            continue;
        }

        const auto& task = inputStripe.Task;
        task->GetChunkPoolInput()->Resume(inputStripe.Cookie);
        Host_->UpdateTask(task.Get());
    }
}

void TInputManager::OnInputChunkUnavailable(TChunkId chunkId, TInputChunkDescriptor* descriptor)
{
    VERIFY_INVOKER_AFFINITY(Host_->GetCancelableInvoker(EOperationControllerQueue::Default));

    if (descriptor->State != EInputChunkState::Active) {
        return;
    }

    RegisterUnavailableInputChunk(chunkId);

    switch (Host_->GetSpec()->UnavailableChunkTactics) {
        case EUnavailableChunkAction::Fail:
            Host_->OnOperationFailed(TError(NChunkClient::EErrorCode::ChunkUnavailable, "Input chunk %v is unavailable",
                chunkId));
            break;

        case EUnavailableChunkAction::Skip: {
            descriptor->State = EInputChunkState::Skipped;
            for (const auto& inputStripe : descriptor->InputStripes) {
                inputStripe.Stripe->DataSlices.erase(
                    std::remove_if(
                        inputStripe.Stripe->DataSlices.begin(),
                        inputStripe.Stripe->DataSlices.end(),
                        [&] (TLegacyDataSlicePtr slice) {
                            try {
                                return chunkId == slice->GetSingleUnversionedChunk()->GetChunkId();
                            } catch (const std::exception& ex) {
                                //FIXME(savrus) allow data slices to be unavailable.
                                Host_->OnOperationFailed(TError(NChunkClient::EErrorCode::ChunkUnavailable, "Dynamic table chunk became unavailable")
                                    << ex);
                                return true;
                            }
                        }),
                    inputStripe.Stripe->DataSlices.end());

                // Store information that chunk disappeared in the chunk mapping.
                for (const auto& chunk : descriptor->InputChunks) {
                    inputStripe.Task->GetChunkMapping()->OnChunkDisappeared(chunk);
                }

                Host_->UpdateTask(inputStripe.Task.Get());
            }
            GetClusterOrCrash(*descriptor)->ChunkScraper()->Start();
            break;
        }

        case EUnavailableChunkAction::Wait: {
            descriptor->State = EInputChunkState::Waiting;
            for (const auto& inputStripe : descriptor->InputStripes) {
                if (inputStripe.Stripe->WaitingChunkCount == 0) {
                    inputStripe.Task->GetChunkPoolInput()->Suspend(inputStripe.Cookie);
                }
                ++inputStripe.Stripe->WaitingChunkCount;
            }
            GetClusterOrCrash(*descriptor)->ChunkScraper()->Start();
            break;
        }

        default:
            YT_ABORT();
    }
}

void TInputManager::RegisterUnavailableInputChunks(bool reportIfFound)
{
    for (const auto& [_, cluster] : Clusters_) {
        cluster->UnavailableInputChunkIds().clear();
    }
    for (const auto& [chunkId, chunkDescriptor] : InputChunkMap_) {
        if (chunkDescriptor.State == EInputChunkState::Waiting) {
            RegisterUnavailableInputChunk(chunkId);
        }
    }

    if (reportIfFound) {
        for (const auto& [_, cluster] : Clusters_) {
            cluster->ReportIfHasUnavailableChunks();
        }
    }

    InitInputChunkScrapers();
}

void TInputManager::RegisterUnavailableInputChunk(TChunkId chunkId)
{
    auto& cluster = GetClusterOrCrash(chunkId);
    InsertOrCrash(cluster->UnavailableInputChunkIds(), chunkId);

    YT_LOG_TRACE("Input chunk is unavailable (ChunkId: %v)", chunkId);
}

void TInputManager::UnregisterUnavailableInputChunk(TChunkId chunkId)
{
    auto& cluster = GetClusterOrCrash(chunkId);
    EraseOrCrash(cluster->UnavailableInputChunkIds(), chunkId);

    YT_LOG_TRACE("Input chunk is no longer unavailable (ChunkId: %v)", chunkId);
}

const TInputClusterPtr& TInputManager::GetClusterOrCrash(const TClusterName& clusterName) const
{
    return GetOrCrash(Clusters_, clusterName);
}

const TInputClusterPtr& TInputManager::GetClusterOrCrash(NChunkClient::TChunkId chunkId) const
{
    return GetClusterOrCrash(InputTables_[GetOrCrash(InputChunkMap_, chunkId).GetTableIndex()]->ClusterName);
}

const TInputClusterPtr& TInputManager::GetClusterOrCrash(const TInputChunkDescriptor& chunkDescriptor) const
{
    return GetClusterOrCrash(InputTables_[chunkDescriptor.GetTableIndex()]->ClusterName);
}

void TInputManager::BuildUnavailableInputChunksYson(TFluentAny fluent) const
{
    fluent
        .BeginMap()
            .DoFor(Clusters_, [] (TFluentMap fluent, const auto& clusterNameAndCluster) {
                const auto& [name, cluster] = clusterNameAndCluster;
                fluent
                    .Item(Format("%v", name))
                    .Value(cluster->UnavailableInputChunkIds());
            })
        .EndMap();
}

int TInputManager::GetUnavailableInputChunkCount() const
{
    int chunkCount = 0;
    for (const auto& [_, cluster] : Clusters_) {
        chunkCount += std::ssize(cluster->UnavailableInputChunkIds());
    }
    return chunkCount;
}

bool TInputManager::OnInputChunkFailed(TChunkId chunkId, TJobId jobId)
{
    auto it = InputChunkMap_.find(chunkId);
    if (it != InputChunkMap_.end()) {
        YT_LOG_DEBUG("Input chunk has failed (ChunkId: %v, JobId: %v)", chunkId, jobId);
        OnInputChunkUnavailable(chunkId, &it->second);
        return true;
    } else {
        // This is an intermediate chunk.
        return false;
    }
}

void TInputManager::RegisterInputChunk(const TInputChunkPtr& inputChunk)
{
    auto chunkId = inputChunk->GetChunkId();

    // Insert an empty TInputChunkDescriptor if a new chunkId is encountered.
    auto& chunkDescriptor = InputChunkMap_[chunkId];
    chunkDescriptor.InputChunks.push_back(inputChunk);

    if (IsUnavailable(inputChunk, Host_->GetChunkAvailabilityPolicy())) {
        chunkDescriptor.State = EInputChunkState::Waiting;
    }
}

////////////////////////////////////////////////////////////////////////////////


// NB: must preserve order of chunks in the input tables, no shuffling.
std::vector<TInputChunkPtr> TInputManager::CollectPrimaryChunks(bool versioned, std::optional<TClusterName> clusterName) const
{
    std::vector<TInputChunkPtr> result;
    const auto& tables = clusterName ? GetClusterOrCrash(*clusterName)->InputTables() : InputTables_;
    for (const auto& table : tables) {
        if (!table->IsForeign() && ((table->Dynamic && table->Schema->IsSorted()) == versioned)) {
            for (const auto& chunk : table->Chunks) {
                if (IsUnavailable(chunk, Host_->GetChunkAvailabilityPolicy())) {
                    switch (Host_->GetSpec()->UnavailableChunkStrategy) {
                        case EUnavailableChunkAction::Skip:
                            continue;

                        case EUnavailableChunkAction::Wait:
                            // Do nothing.
                            break;

                        default:
                            YT_ABORT();
                    }
                }
                result.push_back(chunk);
            }
        }
    }
    return result;
}

std::vector<TInputChunkPtr> TInputManager::CollectPrimaryUnversionedChunks() const
{
    return CollectPrimaryChunks(false);
}

std::vector<TInputChunkPtr> TInputManager::CollectPrimaryVersionedChunks() const
{
    return CollectPrimaryChunks(true);
}

std::pair<NTableClient::IChunkSliceFetcherPtr, TUnavailableChunksWatcherPtr> TInputManager::CreateChunkSliceFetcher() const
{
    std::vector<IChunkSliceFetcherPtr> chunkSliceFetchers;
    std::vector<IFetcherChunkScraperPtr> fetcherChunkScrapers;
    THashMap<TClusterName, int> clusterOrder;
    for (const auto& [clusterName, cluster] : Clusters_) {
        clusterOrder[clusterName] = clusterOrder.size();

        fetcherChunkScrapers.push_back(CreateFetcherChunkScraper(cluster));
        chunkSliceFetchers.push_back(
            NTableClient::CreateChunkSliceFetcher(
                Host_->GetConfig()->ChunkSliceFetcher,
                cluster->NodeDirectory(),
                Host_->GetCancelableInvoker(),
                fetcherChunkScrapers.back(),
                cluster->Client(),
                Host_->GetRowBuffer(),
                Logger.WithTag("Cluster: %v", clusterName)));

        chunkSliceFetchers.back()->SetCancelableContext(Host_->GetCancelableContext());
    }

    std::vector<int> tableIndexToFetcherIndex;
    for (const auto& table : InputTables_) {
        tableIndexToFetcherIndex.push_back(clusterOrder[table->ClusterName]);
    }

    return std::pair(
        NTableClient::CreateCombiningChunkSliceFetcher(
            std::move(chunkSliceFetchers),
            std::move(tableIndexToFetcherIndex)),
        New<TUnavailableChunksWatcher>(std::move(fetcherChunkScrapers)));
}

std::pair<TCombiningSamplesFetcherPtr, TUnavailableChunksWatcherPtr> TInputManager::CreateSamplesFetcher(
    const NTableClient::TTableSchemaPtr& sampleSchema,
    const NTableClient::TRowBufferPtr& rowBuffer,
    i64 sampleCount,
    int maxSampleSize) const
{
    std::vector<IFetcherChunkScraperPtr> fetcherChunkScrapers;
    std::vector<TSamplesFetcherPtr> samplesFetchers;

    for (const auto& [clusterName, cluster] : Clusters_) {
        fetcherChunkScrapers.push_back(CreateFetcherChunkScraper(cluster));

        auto samplesFetcher = New<TSamplesFetcher>(
            Host_->GetConfig()->Fetcher,
            ESamplingPolicy::Sorting,
            sampleCount,
            sampleSchema->GetColumnNames(),
            maxSampleSize,
            cluster->NodeDirectory(),
            Host_->GetCancelableInvoker(),
            rowBuffer,
            fetcherChunkScrapers.back(),
            cluster->Client(),
            Logger);

        for (const auto& chunk : CollectPrimaryChunks(/*versioned=*/ false, clusterName)) {
            if (!chunk->IsDynamicStore()) {
                samplesFetcher->AddChunk(chunk);
            }
        }
        for (const auto& chunk : CollectPrimaryChunks(/*versioned=*/ true, clusterName)) {
            if (!chunk->IsDynamicStore()) {
                samplesFetcher->AddChunk(chunk);
            }
        }

        samplesFetcher->SetCancelableContext(Host_->GetCancelableContext());
        samplesFetchers.push_back(samplesFetcher);
    }
    return std::pair(
        New<TCombiningSamplesFetcher>(std::move(samplesFetchers)),
        New<TUnavailableChunksWatcher>(std::move(fetcherChunkScrapers)));
}

////////////////////////////////////////////////////////////////////////////////

const TNodeDirectoryPtr& TInputManager::GetNodeDirectory(const TClusterName& clusterName) const
{
    return GetOrCrash(Clusters_, clusterName)->NodeDirectory();
}

IClientPtr TInputManager::GetClient(const TClusterName& clusterName) const
{
    return GetOrCrash(Clusters_, clusterName)->Client();
}

////////////////////////////////////////////////////////////////////////////////

void TInputManager::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Host_);

    Persist(context, Logger);

    Persist(context, InputTables_);
    if (context.GetVersion() < ESnapshotVersion::RemoteInputForOperations) {
        const auto& emplacedIterator = EmplaceOrCrash(
            Clusters_,
            LocalClusterName,
            New<TInputCluster>(LocalClusterName, Logger));
        emplacedIterator->second->InputTables() = InputTables_;
    }

    if (context.GetVersion() < ESnapshotVersion::RemoteInputForOperations) {
        Persist(context, GetClusterOrCrash(LocalClusterName)->NodeDirectory());
    }
    Persist(context, InputChunkMap_);
    if (context.GetVersion() < ESnapshotVersion::RemoteInputForOperations) {
        Persist(context, GetClusterOrCrash(LocalClusterName)->UnavailableInputChunkIds());
    }
    Persist(context, InputHasStaticTableWithHunks_);
    Persist(context, InputHasOrderedDynamicStores_);
    if (context.GetVersion() >= ESnapshotVersion::RemoteInputForOperations) {
        Persist(context, Clusters_);
        Persist(context, ClusterResolver_);
    }
}

void TInputManager::PrepareToBeLoadedFromAncientVersion()
{
    Clusters_[LocalClusterName] = New<TInputCluster>(LocalClusterName, Logger);
}

void TInputManager::LoadInputNodeDirectory(const TPersistenceContext& context)
{
    YT_VERIFY(context.IsLoad() && context.GetVersion() < ESnapshotVersion::InputManagerIntroduction);
    NYT::Persist(context, GetClusterOrCrash(LocalClusterName)->NodeDirectory());
}

void TInputManager::LoadInputChunkMap(const TPersistenceContext& context)
{
    YT_VERIFY(context.IsLoad() && context.GetVersion() < ESnapshotVersion::InputManagerIntroduction);
    NYT::Persist(context, InputChunkMap_);
}

void TInputManager::LoadPathToInputTables(const TPersistenceContext& context)
{
    YT_VERIFY(context.IsLoad() && context.GetVersion() < ESnapshotVersion::InputManagerIntroduction);
    NYT::Persist(context, GetClusterOrCrash(LocalClusterName)->PathToInputTables());
}

void TInputManager::LoadInputTables(const TPersistenceContext& context)
{
    YT_VERIFY(context.IsLoad() && context.GetVersion() < ESnapshotVersion::InputManagerIntroduction);
    NYT::Persist(context, InputTables_);
    GetClusterOrCrash(LocalClusterName)->InputTables() = InputTables_;
}

void TInputManager::LoadInputHasOrderedDynamicStores(const TPersistenceContext& context)
{
    YT_VERIFY(context.IsLoad() && context.GetVersion() < ESnapshotVersion::InputManagerIntroduction);
    NYT::Persist(context, InputHasOrderedDynamicStores_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
