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

#include <yt/yt/ytlib/object_client/helpers.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_slice_size_fetcher.h>
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

}

////////////////////////////////////////////////////////////////////////////////

void TInputManager::TStripeDescriptor::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Stripe);
    Persist(context, Cookie);
    Persist(context, Task);
}

////////////////////////////////////////////////////////////////////////////////

void TInputManager::TInputChunkDescriptor::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, InputStripes);
    Persist(context, InputChunks);
    Persist(context, State);
}

////////////////////////////////////////////////////////////////////////////////

TInputManager::TInputManager(IInputManagerHost* host, TLogger logger)
    : Host_(host)
    , Logger(logger)
{ }

void TInputManager::InitializeClient(NNative::IClientPtr client)
{
    Client_ = client;
}

void TInputManager::InitializeStructures(TInputTransactionsManagerPtr inputTransactions)
{
    YT_VERIFY(inputTransactions);
    InputTransactions_ = inputTransactions;
    for (const auto& path : Host_->GetInputTablePaths()) {
        auto table = New<TInputTable>(
            path,
            InputTransactions_->GetTransactionIdForObject(path));
        table->ColumnRenameDescriptors = path.GetColumnRenameDescriptors().value_or(TColumnRenameDescriptors());
        InputTables_.push_back(std::move(table));
    }
}

void TInputManager::LockInputTables()
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

void TInputManager::ValidateOutputTableLockedCorrectly(TOutputTablePtr outputTable) const
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

IFetcherChunkScraperPtr TInputManager::CreateFetcherChunkScraper() const
{
    return Host_->GetSpec()->UnavailableChunkStrategy == EUnavailableChunkAction::Wait
        ? NChunkClient::CreateFetcherChunkScraper(
            Host_->GetConfig()->ChunkScraper,
            Host_->GetChunkScraperInvoker(),
            Host_->GetChunkLocationThrottlerManager(),
            Client_,
            InputNodeDirectory_,
            Logger)
        : nullptr;
}

TMasterChunkSpecFetcherPtr TInputManager::MakeChunkSpecFetcher() const
{
    TPeriodicYielder yielder(PrepareYieldPeriod);

    auto chunkSpecFetcher = New<TMasterChunkSpecFetcher>(
        Client_,
        TMasterReadOptions{},
        InputNodeDirectory_,
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

        auto& table = InputTables_[tableIndex];
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

    auto chunkSliceSizeFetcher = New<TChunkSliceSizeFetcher>(
        Host_->GetConfig()->Fetcher,
        InputNodeDirectory_,
        Host_->GetCancelableInvoker(EOperationControllerQueue::Default),
        /*chunkScraper*/ CreateFetcherChunkScraper(),
        Client_,
        Logger);

    if (auto error = Host_->GetUseChunkSliceStatisticsError(); Host_->GetSpec()->UseChunkSliceStatistics && !error.IsOK()) {
        Host_->SetOperationAlert(EOperationAlertType::UseChunkSliceStatisticsDisabled, error);
        chunkSliceSizeFetcher = nullptr;
    }

    auto chunkSpecFetcher = MakeChunkSpecFetcher();

    auto columnarStatisticsFetcher = New<TColumnarStatisticsFetcher>(
        Host_->GetChunkScraperInvoker(),
        Client_,
        TColumnarStatisticsFetcher::TOptions{
            .Config = Host_->GetConfig()->Fetcher,
            .NodeDirectory = InputNodeDirectory_,
            .ChunkScraper = CreateFetcherChunkScraper(),
            .Mode = Host_->GetSpec()->InputTableColumnarStatistics->Mode,
            .EnableEarlyFinish = Host_->GetConfig()->EnableColumnarStatisticsEarlyFinish,
            .Logger = Logger,
        });

    YT_LOG_INFO("Fetching input tables");

    WaitFor(chunkSpecFetcher->Fetch())
        .ThrowOnError();

    YT_LOG_INFO("Input tables fetched");

    for (const auto& chunkSpec : chunkSpecFetcher->ChunkSpecs()) {
        yielder.TryYield();

        int tableIndex = chunkSpec.table_index();
        auto& table = InputTables_[tableIndex];

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
            auto hasColumnSelectors = table->Path.GetColumns().operator bool();
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
    }

    if (columnarStatisticsFetcher->GetChunkCount() > 0) {
        YT_LOG_INFO("Fetching chunk columnar statistics for tables with column selectors (ChunkCount: %v)",
            columnarStatisticsFetcher->GetChunkCount());
        Host_->MaybeCancel(ECancelationStage::ColumnarStatisticsFetch);
        columnarStatisticsFetcher->SetCancelableContext(Host_->GetCancelableContext());
        WaitFor(columnarStatisticsFetcher->Fetch())
            .ThrowOnError();
        YT_LOG_INFO("Columnar statistics fetched");
        columnarStatisticsFetcher->ApplyColumnSelectivityFactors();
    }

    if (chunkSliceSizeFetcher && chunkSliceSizeFetcher->GetChunkCount() > 0) {
        YT_LOG_INFO("Fetching input chunk slice statistics for input tables (ChunkCount: %v)",
            chunkSliceSizeFetcher->GetChunkCount());
        chunkSliceSizeFetcher->SetCancelableContext(Host_->GetCancelableContext());
        WaitFor(chunkSliceSizeFetcher->Fetch())
            .ThrowOnError();
        YT_LOG_INFO("Input chunk slice statistics fetched");
        chunkSliceSizeFetcher->ApplyBlockSelectivityFactors();
    }

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

    GetUserObjectBasicAttributes(
        Client_,
        MakeUserObjectList(InputTables_),
        InputTransactions_->GetNativeInputTransactionId(),
        Logger,
        EPermission::Read,
        TGetUserObjectBasicAttributesOptions{
            .OmitInaccessibleColumns = Host_->GetSpec()->OmitInaccessibleColumns,
            .PopulateSecurityTags = true
        });

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
        auto proxy = CreateObjectServiceReadProxy(Client_, EMasterChannelKind::Follower, externalCellTag);
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

        FetchTableSchemas(
            Client_,
            InputTables_,
            BIND([] (const TInputTablePtr& table) { return table->ExternalTransactionId; }),
            /*fetchFromExternalCells*/ true);
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

void TInputManager::InitInputChunkScraper()
{
    THashSet<TChunkId> chunkIds;
    for (const auto& [chunkId, chunkDescriptor] : InputChunkMap_) {
        if (!IsDynamicTabletStoreType(TypeFromId(chunkId))) {
            chunkIds.insert(chunkId);
        }
    }

    YT_VERIFY(!InputChunkScraper_);
    InputChunkScraper_ = New<TChunkScraper>(
        Host_->GetConfig()->ChunkScraper,
        Host_->GetChunkScraperInvoker(),
        Host_->GetChunkLocationThrottlerManager(),
        Client_,
        InputNodeDirectory_,
        std::move(chunkIds),
        MakeSafe(
            MakeWeak(Host_),
            BIND(&TInputManager::OnInputChunkLocated, MakeWeak(this)))
                .Via(Host_->GetCancelableInvoker()),
        Logger);

    if (!UnavailableInputChunkIds_.empty()) {
        YT_LOG_INFO(
            "Waiting for unavailable input chunks (Count: %v, SampleIds: %v)",
            UnavailableInputChunkIds_.size(),
            MakeUnavailableInputChunksView());
        InputChunkScraper_->Start();
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
        YT_LOG_DEBUG("Located another batch of chunks (Count: %v, UnavailableInputChunkCount: %v, SampleUnavailableInputChunkIds: %v)",
            Host_->GetConfig()->ChunkScraper->MaxChunksPerRequest,
            GetUnavailableInputChunkCount(),
            MakeUnavailableInputChunksView());
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

    if (UnavailableInputChunkIds_.empty()) {
        YT_UNUSED_FUTURE(InputChunkScraper_->Stop());
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
        Host_->UpdateTask(task);
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

                Host_->UpdateTask(inputStripe.Task);
            }
            InputChunkScraper_->Start();
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
            InputChunkScraper_->Start();
            break;
        }

        default:
            YT_ABORT();
    }
}

void TInputManager::RegisterUnavailableInputChunks(bool reportIfFound)
{
    UnavailableInputChunkIds_.clear();
    for (const auto& [chunkId, chunkDescriptor] : InputChunkMap_) {
        if (chunkDescriptor.State == EInputChunkState::Waiting) {
            RegisterUnavailableInputChunk(chunkId);
        }
    }

    if (reportIfFound && !UnavailableInputChunkIds_.empty()) {
        YT_LOG_INFO("Found unavailable input chunks (UnavailableInputChunkCount: %v, SampleUnavailableInputChunkIds: %v)",
            UnavailableInputChunkIds_.size(),
            MakeUnavailableInputChunksView());
    }

    InitInputChunkScraper();
}

void TInputManager::RegisterUnavailableInputChunk(TChunkId chunkId)
{
    InsertOrCrash(UnavailableInputChunkIds_, chunkId);

    YT_LOG_TRACE("Input chunk is unavailable (ChunkId: %v)", chunkId);
}

void TInputManager::UnregisterUnavailableInputChunk(TChunkId chunkId)
{
    EraseOrCrash(UnavailableInputChunkIds_, chunkId);

    YT_LOG_TRACE("Input chunk is no longer unavailable (ChunkId: %v)", chunkId);
}

void TInputManager::BuildUnavailableInputChunksYson(TFluentAny fluent) const
{
    fluent.Value(UnavailableInputChunkIds_);
}

TFormattableView<THashSet<TChunkId>, TDefaultFormatter> TInputManager::MakeUnavailableInputChunksView() const
{
    return MakeShrunkFormattableView(UnavailableInputChunkIds_, TDefaultFormatter(), SampleChunkIdCount);
}

size_t TInputManager::GetUnavailableInputChunkCount() const
{
    return UnavailableInputChunkIds_.size();
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

const TNodeDirectoryPtr& TInputManager::GetInputNodeDirectory() const
{
    return InputNodeDirectory_;
}

////////////////////////////////////////////////////////////////////////////////

void TInputManager::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Host_);

    Persist(context, Logger);

    Persist(context, InputTables_);
    Persist(context, InputNodeDirectory_);
    Persist(context, InputChunkMap_);
    Persist(context, UnavailableInputChunkIds_);
    Persist(context, InputHasStaticTableWithHunks_);
    Persist(context, InputHasOrderedDynamicStores_);
}

void TInputManager::LoadInputNodeDirectory(const TPersistenceContext& context)
{
    YT_VERIFY(context.IsLoad() && context.GetVersion() < ESnapshotVersion::InputManagerIntroduction);
    NYT::Persist(context, InputNodeDirectory_);
}

void TInputManager::LoadInputChunkMap(const TPersistenceContext& context)
{
    YT_VERIFY(context.IsLoad() && context.GetVersion() < ESnapshotVersion::InputManagerIntroduction);
    NYT::Persist(context, InputChunkMap_);
}

void TInputManager::LoadPathToInputTables(const TPersistenceContext& context)
{
    YT_VERIFY(context.IsLoad() && context.GetVersion() < ESnapshotVersion::InputManagerIntroduction);
    NYT::Persist(context, PathToInputTables_);
}

void TInputManager::LoadInputTables(const TPersistenceContext& context)
{
    YT_VERIFY(context.IsLoad() && context.GetVersion() < ESnapshotVersion::InputManagerIntroduction);
    NYT::Persist(context, InputTables_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
