#include "subquery.h"

#include "config.h"
#include "helpers.h"
#include "host.h"
#include "index.h"
#include "job_size_constraints.h"
#include "query_analyzer.h"
#include "query_context.h"
#include "conversion.h"
#include "subquery_spec.h"
#include "table.h"
#include "virtual_column.h"

#include <yt/yt/server/lib/chunk_pools/helpers.h>
#include <yt/yt/server/lib/chunk_pools/unordered_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/new_sorted_chunk_pool.h>

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>
#include <yt/yt/server/lib/controller_agent/read_range_registry.h>
#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec_fetcher.h>

#include <yt/yt/ytlib/chunk_pools/chunk_stripe.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/ytlib/object_client/object_attribute_cache.h>

#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_slice_fetcher.h>
#include <yt/yt/ytlib/table_client/columnar_statistics_fetcher.h>
#include <yt/yt/ytlib/table_client/table_read_spec.h>
#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>
#include <yt/yt/ytlib/table_client/virtual_value_directory.h>

#include <yt/yt/client/table_client/key_bound.h>
#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/ypath/helpers.h>
#include <yt/yt/core/ytree/convert.h>

#include <Storages/MergeTree/KeyCondition.h>
#include <DataTypes/DataTypeNullable.h>

#include <cmath>

#include <library/cpp/iterator/functools.h>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NChunkClient;
using namespace NChunkPools;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NLogging;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EKeyConditionScale,
    (Partition)
    (Tablet)
    (TopLevelDataSlice)
);

class TInputFetcher
    : public TRefCounted
{
public:
    DEFINE_BYREF_RW_PROPERTY(TDataSourceDirectoryPtr, DataSourceDirectory, New<TDataSourceDirectory>());
    DEFINE_BYREF_RW_PROPERTY(TInputStreamDirectory, InputStreamDirectory);
    DEFINE_BYREF_RW_PROPERTY(TChunkStripeListPtr, ResultStripeList, New<TChunkStripeList>());
    using TMiscExtMap = THashMap<TChunkId, TRefCountedMiscExtPtr>;
    DEFINE_BYREF_RW_PROPERTY(TMiscExtMap, MiscExtMap);
    //! Number of operands to join. May be either 1 (if no JOIN present or joinee is not a YT table) or 2 (otherwise).
    DEFINE_BYREF_RW_PROPERTY(int, OperandCount, 0);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TTablePtr>, InputTables);

public:
    TInputFetcher(
        TStorageContext* storageContext,
        const TQueryAnalysisResult& queryAnalysisResult,
        const std::vector<std::string>& realColumnNames,
        const std::vector<std::string>& virtualColumnNames,
        const TClickHouseIndexBuilder& indexBuilder,
        TTransactionId transactionId)
        : StorageContext_(storageContext)
        , QueryContext_(StorageContext_->QueryContext)
        , Client_(QueryContext_->Client())
        , TransactionId_(transactionId)
        , Invoker_(QueryContext_->Host->GetClickHouseFetcherInvoker())
        , OperandSchemas_(queryAnalysisResult.TableSchemas)
        , KeyConditions_(queryAnalysisResult.KeyConditions)
        , RealColumnNames_(realColumnNames)
        , VirtualColumnNames_(virtualColumnNames)
        , IndexBuilder_(indexBuilder)
        , Config_(QueryContext_->Host->GetConfig()->Subquery)
        , RowBuffer_(QueryContext_->RowBuffer)
        , Logger(StorageContext_->Logger)
    {
        Y_UNUSED(StorageContext_);

        OperandCount_ = queryAnalysisResult.Tables.size();
        for (int operandIndex = 0; operandIndex < static_cast<int>(queryAnalysisResult.Tables.size()); ++operandIndex) {
            for (auto& table : queryAnalysisResult.Tables[operandIndex]) {
                table->OperandIndex = operandIndex;
                InputTables_.emplace_back(std::move(table));
            }
            KeyColumnDataTypes_.push_back(ToDataTypes(*OperandSchemas_[operandIndex]->ToKeys(), StorageContext_->Settings->Composite));
        }

        CanBeTrueOnTable_.assign(InputTables_.size(), true);
    }

    TFuture<void> Fetch()
    {
        return BIND(&TInputFetcher::DoFetch, MakeWeak(this))
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    const TStorageContext* StorageContext_;
    TQueryContext* QueryContext_;

    NApi::NNative::IClientPtr Client_;

    TTransactionId TransactionId_;

    IInvokerPtr Invoker_;

    std::vector<TTableSchemaPtr> OperandSchemas_;
    std::vector<std::optional<DB::KeyCondition>> KeyConditions_;

    std::vector<std::string> RealColumnNames_;
    std::vector<std::string> VirtualColumnNames_;

    TClickHouseIndexBuilder IndexBuilder_;

    //! Per-table flag indicating if table is not discarded by 'where' condition.
    //! We do not delete such tables from InputTables_ since it can corrupt $table_index.
    //! We do not need to fetch chunk specs for such 'useless' tables.
    std::vector<bool> CanBeTrueOnTable_;

    std::vector<DB::DataTypes> KeyColumnDataTypes_;

    TSubqueryConfigPtr Config_;

    TRowBufferPtr RowBuffer_;

    std::vector<TChunkStripePtr> ResultStripes_;

    // Table index to input data slices.
    std::vector<std::vector<TLegacyDataSlicePtr>> InputDataSlices_;

    std::vector<TTableReadSpec> TableReadSpecs_;

    TMasterChunkSpecFetcherPtr MasterChunkSpecFetcher_;
    TTabletChunkSpecFetcherPtr TabletChunkSpecFetcher_;

    TLogger Logger;

    //! Fetch input tables. Result goes to ResultStripeList_.
    void DoFetch()
    {
        FilterTablesByVirtualColumnIndex();

        FetchTables();

        if (Config_->UseColumnarStatistics) {
            auto columnarStatisticsFetcher = New<TColumnarStatisticsFetcher>(
                Invoker_,
                Client_,
                TColumnarStatisticsFetcher::TOptions{
                    .Config = Config_->ColumnarStatisticsFetcher,
                    .Mode = EColumnarStatisticsFetcherMode::FromMaster,
                    .Logger = Logger,
                });

            YT_VERIFY(OperandSchemas_.size() == ResultStripes_.size());
            for (const auto& [resultStripe, schema] : Zip(ResultStripes_, OperandSchemas_)) {
                auto columnStableNames = MapNamesToStableNames(
                    *schema,
                    RealColumnNames_,
                    NonexistentColumnName);
                for (auto& inputDataSlice : resultStripe->DataSlices) {
                    for (auto& inputChunkSlice : inputDataSlice->ChunkSlices) {
                        columnarStatisticsFetcher->AddChunk(inputChunkSlice->GetInputChunk(), columnStableNames);
                    }
                }
            }

            WaitFor(columnarStatisticsFetcher->Fetch())
                .ThrowOnError();
            columnarStatisticsFetcher->ApplyColumnSelectivityFactors();
        }

        for (int operandIndex = 0; operandIndex < std::ssize(ResultStripes_); ++operandIndex) {
            auto& stripe = ResultStripes_[operandIndex];
            auto& dataSlices = stripe->DataSlices;

            auto removePred = [&] (const TLegacyDataSlicePtr& dataSlice) {
                if (!dataSlice->LowerLimit().KeyBound && !dataSlice->UpperLimit().KeyBound) {
                    return false;
                }
                return !GetRangeMask(
                    EKeyConditionScale::TopLevelDataSlice,
                    dataSlice->LowerLimit().KeyBound,
                    dataSlice->UpperLimit().KeyBound,
                    operandIndex).can_be_true;
            };

            auto it = std::remove_if(dataSlices.begin(), dataSlices.end(), [&] (const TLegacyDataSlicePtr& dataSlice) {
                bool needToRemove = removePred(dataSlice);

                using namespace NStatisticPath;
                if (needToRemove) {
                    QueryContext_->AddStatisticsSample("/input_fetcher/filtered_data_slices/row_count"_SP, dataSlice->GetRowCount());
                    QueryContext_->AddStatisticsSample("/input_fetcher/filtered_data_slices/data_weight"_SP, dataSlice->GetRowCount());
                } else {
                    QueryContext_->AddStatisticsSample("/input_fetcher/data_slices/row_count"_SP, dataSlice->GetRowCount());
                    QueryContext_->AddStatisticsSample("/input_fetcher/data_slices/data_weight"_SP, dataSlice->GetRowCount());
                }

                return needToRemove;
            });
            dataSlices.resize(it - dataSlices.begin());

            ResultStripeList_->AddStripe(std::move(stripe));
        }

        YT_LOG_INFO(
            "Input fetched (TotalChunkCount: %v, TotalDataWeight: %v, TotalRowCount: %v)",
            ResultStripeList_->TotalChunkCount,
            ResultStripeList_->TotalDataWeight,
            ResultStripeList_->TotalRowCount);
    }

    //! Create set index for present virtual columns.
    TClickHouseIndexPtr CreateVirtualColumnIndex()
    {
        DB::NamesAndTypesList namesAndTypes;

        for (const auto& column : VirtualColumnNames_) {
            auto nameAndType = VirtualColumns.tryGet(column);
            YT_VERIFY(nameAndType);
            namesAndTypes.emplace_back(*nameAndType);
        }

        return IndexBuilder_.CreateIndex(namesAndTypes, "set");
    }

    //! Check if query condition can be true on tables. Fill CanBeTrueOnTable_.
    void FilterTablesByVirtualColumnIndex()
    {
        if (VirtualColumnNames_.empty()) {
            return;
        }

        auto index = CreateVirtualColumnIndex();
        const auto& condition = index->Condition();

        // For example if 'where' condition does not depend on virtual columns.
        if (condition->alwaysUnknownOrTrue()) {
            return;
        }

        int discardedByIndex = 0;

        // Relative tableIndex for each operand.
        std::vector<int> operandTableIndexes(2);

        auto aggregator = index->CreateAggregator();

        for (int tableIndex = 0; tableIndex < std::ssize(InputTables_); ++tableIndex) {
            const auto& table = InputTables_[tableIndex];
            auto operandIndex = table->OperandIndex;

            // TODO(dakovalkov): It looks like CreateVirtualValueDirectory(), generalize it?

            DB::Block virtualValues;
            const auto& sampleBlock = index->Description().sample_block;

            for (const auto& [_, type, name] : sampleBlock.getColumnsWithTypeAndName()) {
                auto column = type->createColumn();

                if (name == TableIndexColumnName) {
                    column->insert(static_cast<Int64>(operandTableIndexes[operandIndex]));
                } else if (name == TablePathColumnName) {
                    const auto& path = InputTables_[tableIndex]->Path.GetPath();
                    column->insert(std::string(path.data(), path.size()));
                } else if (name == TableKeyColumnName) {
                    auto [_, baseName] = DirNameAndBaseName(InputTables_[tableIndex]->Path.GetPath());
                    column->insert(std::string(baseName.data(), baseName.size()));
                } else {
                    // Unreachable.
                    YT_ABORT();
                }
                virtualValues.insert({std::move(column), type, name});
            }

            size_t position = 0;
            aggregator->update(virtualValues, &position, /*limit*/ 1);
            // Aggregator should read rows and update position.
            YT_VERIFY(position == 1);

            auto granule = aggregator->getGranuleAndReset();

            if (!condition->mayBeTrueOnGranule(granule)) {
                CanBeTrueOnTable_[tableIndex] = false;
                ++discardedByIndex;
            }

            ++operandTableIndexes[operandIndex];
        }

        YT_LOG_DEBUG("Tables were filtered by virtual column index (TotalTableCount: %v, DiscardedByIndex: %v)",
            InputTables_.size(),
            discardedByIndex);
    }

    //! Fetch all tables and fill ResultStripes_.
    void FetchTables()
    {
        int unversionedTableCount = 0;
        int versionedTableCount = 0;
        for (const auto& table : InputTables_) {
            if (table->IsSortedDynamic()) {
                ++versionedTableCount;
            } else {
                ++unversionedTableCount;
            }
        }

        YT_LOG_INFO(
            "Fetching input tables (UnversionedTableCount: %v, VersionedTableCount: %v)",
            unversionedTableCount,
            versionedTableCount);

        // We fetch table read spec for each table separately, put them into TableReadSpecs_ vector,
        // which will later be joined by JoinTableReadSpecs function.
        TableReadSpecs_.resize(InputTables_.size());
        FetchTableReadSpecs();

        YT_LOG_INFO("Input tables fetched, preparing data slices");

        auto [dataSourceDirectory, dataSliceDescriptors] = JoinTableReadSpecs(TableReadSpecs_);
        YT_VERIFY(dataSourceDirectory->DataSources().size() == InputTables_.size());

        // Transform (single-chunk) data slice descriptors into data slices.
        // NB: by this moment versioned chunk data slice descriptors store separate chunk specs,
        // so they are not "correct" from the dynamic table point of view.
        InputDataSlices_.resize(InputTables_.size());
        for (auto& dataSliceDescriptor : dataSliceDescriptors) {
            RegisterDataSlice(dataSliceDescriptor);
        }

        // Fix slices for dynamic tables.
        for (size_t tableIndex = 0; tableIndex < InputTables_.size(); ++tableIndex) {
            if (InputTables_[tableIndex]->IsSortedDynamic()) {
                CombineVersionedDataSlices(tableIndex);
            }
        }

        // Put data slices to their destination stripes.
        ResultStripes_.resize(OperandCount_);
        for (auto& stripe : ResultStripes_) {
            stripe = New<TChunkStripe>();
        }

        // InputTables_ contains tables for both operands.
        // For $table_index column we need to calculate the index regarding the corresponding operand.
        std::vector<int> operandTableIndexes(2);

        for (size_t tableIndex = 0; tableIndex < InputTables_.size(); ++tableIndex) {
            auto operandIndex = InputTables_[tableIndex]->OperandIndex;

            if (!VirtualColumnNames_.empty()) {
                YT_VERIFY(!dataSourceDirectory->DataSources()[tableIndex].GetVirtualValueDirectory());
                dataSourceDirectory->DataSources()[tableIndex].SetVirtualValueDirectory(
                    CreateVirtualValueDirectory(tableIndex, operandTableIndexes[operandIndex]));
            }

            for (auto& dataSlice : InputDataSlices_[tableIndex]) {
                YT_VERIFY(!dataSlice->IsLegacy);
                dataSlice->SetInputStreamIndex(tableIndex);

                if (!VirtualColumnNames_.empty()) {
                    dataSlice->VirtualRowIndex = 0;
                }

                ResultStripes_[operandIndex]->DataSlices.emplace_back(std::move(dataSlice));
            }

            ++operandTableIndexes[operandIndex];
        }

        DataSourceDirectory_ = std::move(dataSourceDirectory);

        std::vector<NChunkPools::TInputStreamDescriptor> inputStreams;
        for (const auto& [tableIndex, dataSource] : Enumerate(DataSourceDirectory_->DataSources())) {
            auto& descriptor = inputStreams.emplace_back(
                /*isTeleportable*/ false,
                /*isPrimary*/ true,
                /*isVersioned*/ dataSource.GetType() == EDataSourceType::VersionedTable);
            descriptor.SetTableIndex(tableIndex);
        }
        InputStreamDirectory_ = TInputStreamDirectory(std::move(inputStreams));

        YT_LOG_INFO("Data slices ready");
    }

    //! Store values for columns $table_index, $table_path, etc.
    TVirtualValueDirectoryPtr CreateVirtualValueDirectory(int tableIndex, int operandTableIndex)
    {
        if (InputTables_[tableIndex]->Dynamic) {
            THROW_ERROR_EXCEPTION("Virtual columns are not supported for dynamic tables (CHYT-506)");
        }

        auto directory = New<TVirtualValueDirectory>();
        directory->NameTable = TNameTable::FromKeyColumns(VirtualColumnNames_);

        TUnversionedOwningRowBuilder rowBuilder(VirtualColumnNames_.size());

        for (const auto& column : VirtualColumnNames_) {
            auto id = directory->NameTable->GetIdOrThrow(column);

            if (column == TableIndexColumnName) {
                rowBuilder.AddValue(MakeUnversionedInt64Value(operandTableIndex, id));
            } else if (column == TablePathColumnName) {
                rowBuilder.AddValue(MakeUnversionedStringValue(InputTables_[tableIndex]->Path.GetPath(), id));
            } else if (column == TableKeyColumnName) {
                auto [_, baseName] = DirNameAndBaseName(InputTables_[tableIndex]->Path.GetPath());
                rowBuilder.AddValue(MakeUnversionedStringValue(baseName, id));
            } else {
                THROW_ERROR_EXCEPTION("Unknown virtual column %Qv", column);
            }
        }

        auto row = rowBuilder.FinishRow();

        std::vector<TColumnSchema> columnSchemas;
        columnSchemas.reserve(VirtualColumnNames_.size());
        for (const auto& column : VirtualColumnNames_) {
            auto id = directory->NameTable->GetIdOrThrow(column);
            columnSchemas.emplace_back(column, MakeLogicalType(GetLogicalType(row[id].Type), /*required*/ true));
        }
        directory->Schema = New<TTableSchema>(std::move(columnSchemas));

        TUnversionedRowsBuilder rowsBuilder;
        rowsBuilder.AddRow(row.Get());
        directory->Rows = rowsBuilder.Build();

        return directory;
    }

    //! Make proper versioned data slices from single-chunk unversioned data slices.
    void CombineVersionedDataSlices(int tableIndex)
    {
        auto& dataSlices = InputDataSlices_[tableIndex];

        std::vector<TInputChunkSlicePtr> chunkSlices;
        chunkSlices.reserve(dataSlices.size());

        // Yes, that looks weird, but we extract chunk slices again
        // from data slices so that we can form new data slices.
        for (auto& dataSlice : dataSlices) {
            for (auto& chunkSlice : dataSlice->ChunkSlices) {
                YT_VERIFY(!chunkSlice->IsLegacy);
                chunkSlices.emplace_back(std::move(chunkSlice));
            }
        }

        dataSlices = CombineVersionedChunkSlices(chunkSlices, InputTables_[tableIndex]->Comparator);
    }

    //! When reading from dynamic tables, it is generally a good idea to start with checking each of the tablets
    //! against key condition in order to reduce number of chunk specs to be fetched from master.
    //! This method does such optimization.
    void InferDynamicTableRangesFromPivotKeys()
    {
        YT_LOG_DEBUG("Inferring dynamic table ranges from tablet pivot keys");
        for (const auto& inputTable : InputTables_) {
            if (!inputTable->IsSortedDynamic()) {
                // We do not need to infer ranges for static or ordered dynamic tables.
                continue;
            }
            if (inputTable->Path.HasNontrivialRanges()) {
                // We skip tables with non-trivial ranges.
                YT_LOG_DEBUG("Skipping table as it already has non-trivial ranges (Table: %v)", inputTable->Path);
                continue;
            }

            std::vector<TReadRange> ranges;

            const auto& tablets = inputTable->TableMountInfo->Tablets;

            TOwningKeyBound lowerBound;

            auto flushRange = [&] (TOwningKeyBound upperBound) {
                YT_VERIFY(lowerBound);
                auto& range = ranges.emplace_back();
                range.LowerLimit().KeyBound() = lowerBound;
                range.UpperLimit().KeyBound() = upperBound;
                lowerBound = TOwningKeyBound();
            };

            for (ui32 index = 0; index < tablets.size(); ++index) {
                const auto& lowerPivotKey = (TOwningKeyBound::FromRow() >= tablets[index]->PivotKey);
                const auto& upperPivotKey = (index + 1 < tablets.size())
                    ? (TOwningKeyBound::FromRow() < tablets[index + 1]->PivotKey)
                    : TOwningKeyBound::MakeUniversal(/*isUpper*/ true);

                if (GetRangeMask(
                        EKeyConditionScale::Tablet,
                        lowerPivotKey,
                        upperPivotKey,
                        inputTable->OperandIndex).can_be_true)
                {
                    if (!lowerBound) {
                        lowerBound = lowerPivotKey;
                    }
                } else {
                    if (lowerBound) {
                        flushRange(lowerPivotKey.Invert());
                    }
                }
            }

            if (lowerBound) {
                flushRange(TOwningKeyBound::MakeUniversal(/*isUpper*/ true));
            }

            YT_LOG_DEBUG("Dynamic table ranges inferred from tablet pivot keys (Table: %v, Ranges: %v)", inputTable->Path, ranges);
            inputTable->Path.SetRanges(ranges);
        }
    }

    void InitializeChunkSpecFetchers()
    {
        MasterChunkSpecFetcher_ = New<TMasterChunkSpecFetcher>(
            Client_,
            *QueryContext_->Settings->FetchChunksReadOptions,
            Client_->GetNativeConnection()->GetNodeDirectory(),
            Invoker_,
            Config_->MaxChunksPerFetch,
            Config_->MaxChunksPerLocateRequest,
            [=, this] (const TChunkOwnerYPathProxy::TReqFetchPtr& req, int /*tableIndex*/) {
                req->set_fetch_all_meta_extensions(false);
                req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
                req->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value);
                req->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::THeavyColumnStatisticsExt>::Value);
                if (!QueryContext_->Settings->DynamicTable->EnableDynamicStoreRead) {
                    req->set_omit_dynamic_stores(true);
                }
                SetTransactionId(req, TransactionId_);
                SetSuppressAccessTracking(req, true);
                SetSuppressExpirationTimeoutRenewal(req, true);
            },
            Logger);

        TTabletChunkSpecFetcher::TOptions options{
            .Client = Client_,
            .RowBuffer = RowBuffer_,
            .InitializeFetchRequest = [=, this] (TTabletChunkSpecFetcher::TRequest* req) {
                req->set_fetch_all_meta_extensions(true);
                req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
                req->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value);
                req->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::THeavyColumnStatisticsExt>::Value);
                if (!QueryContext_->Settings->DynamicTable->EnableDynamicStoreRead) {
                    req->set_omit_dynamic_stores(true);
                }
            }
        };

        TabletChunkSpecFetcher_ = New<TTabletChunkSpecFetcher>(
            std::move(options),
            Invoker_,
            Logger);
    }

    void AddTableForFetching(const TTablePtr& table, int tableIndex)
    {
        // TODO(achulkov2): Support ordered tables in tablet chunk spec fetcher?
        if (table->IsSortedDynamic() && QueryContext_->Settings->DynamicTable->FetchFromTablets &&
            QueryContext_->Settings->Execution->TableReadLockMode == ETableReadLockMode::None &&
            table->TableMountInfo->MountedTablets.size() == table->TableMountInfo->Tablets.size())
        {
            TabletChunkSpecFetcher_->Add(
                FromObjectId(table->ObjectId),
                table->ChunkCount,
                tableIndex,
                table->Path.GetNewRanges(table->Comparator));
        } else {
            MasterChunkSpecFetcher_->Add(
                table->ObjectId,
                table->ExternalCellTag,
                table->ChunkCount,
                tableIndex,
                table->Path.GetNewRanges(table->Comparator));
        }
    }

    void FetchTableReadSpecs()
    {
        i64 totalChunkCount = 0;
        for (const auto& inputTable : InputTables_) {
            totalChunkCount += inputTable->ChunkCount;
        }
        YT_LOG_INFO("Fetching tables (TableCount: %v, TotalChunkCount: %v)",
            InputTables_.size(),
            totalChunkCount);

        if (StorageContext_->Settings->InferDynamicTableRangesFromPivotKeys) {
            InferDynamicTableRangesFromPivotKeys();
        }

        InitializeChunkSpecFetchers();

        for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables_.size()); ++tableIndex) {
            const auto& table = InputTables_[tableIndex];
            TableReadSpecs_[tableIndex].DataSourceDirectory = New<TDataSourceDirectory>();
            auto& dataSource = TableReadSpecs_[tableIndex].DataSourceDirectory->DataSources().emplace_back();
            if (table->IsSortedDynamic()) {
                dataSource = MakeVersionedDataSource(
                    /*path*/ std::nullopt,
                    table->Schema,
                    /*columns*/ std::nullopt,
                    // TODO(max42): YT-10402, omitted inaccessible columns
                    /*omittedInaccessibleColumns*/ {},
                    table->Path.GetTimestamp().value_or(QueryContext_->DynamicTableReadTimestamp),
                    table->Path.GetRetentionTimestamp().value_or(NullTimestamp),
                    /*columnRenameDescriptors*/ {});
            } else {
                dataSource = MakeUnversionedDataSource(
                    std::nullopt /*path*/,
                    table->Schema,
                    std::nullopt /*columns*/,
                    // TODO(max42): YT-10402, omitted inaccessible columns
                    /*omittedInaccessibleColumns*/ {},
                    /*columnRenameDescriptors*/ {});
            }

            // We do not need to fetch anything if table was filtered by index.
            if (CanBeTrueOnTable_[tableIndex]) {
                AddTableForFetching(table, tableIndex);
            }
        }

        if (auto breakpointFilename = QueryContext_->Settings->Testing->ChunkSpecFetcherBreakpoint) {
            HandleBreakpoint(*breakpointFilename, Client_);
            YT_LOG_DEBUG("Chunk spec fetcher handled breakpoint (Breakpoint: %v)", breakpointFilename);
        }

        std::vector<TFuture<void>> asyncResults = {
            MasterChunkSpecFetcher_->Fetch(),
            TabletChunkSpecFetcher_->Fetch()
        };

        WaitFor(AllSucceeded(asyncResults))
            .ThrowOnError();

        int chunkCount = 0;
        for (auto& chunkSpec : Concatenate(
            MasterChunkSpecFetcher_->ChunkSpecs(),
            TabletChunkSpecFetcher_->ChunkSpecs()))
        {
            chunkCount++;
            auto tableIndex = chunkSpec.table_index();

            // Table indices will be properly reassigned later by JoinTableReadSpecs.
            chunkSpec.set_table_index(0);
            TableReadSpecs_[tableIndex].DataSliceDescriptors.emplace_back(TDataSliceDescriptor(std::move(chunkSpec)));
        }

        YT_LOG_INFO("Chunk specs fetched (ChunkCount: %v)", chunkCount);
    }

    //! Wrap chunk spec from data slice descriptor into data slice,
    //! keep its misc ext and push it to InputDataSlices_[tableIndex].
    void RegisterDataSlice(TDataSliceDescriptor& dataSliceDescriptor)
    {
        auto& chunkSpec = dataSliceDescriptor.GetSingleChunk();

        auto tableIndex = chunkSpec.table_index();
        int keyLength = InputTables_[tableIndex]->Comparator.GetLength();

        auto inputChunk = New<TInputChunk>(chunkSpec, keyLength);

        auto miscExt = FindProtoExtension<NChunkClient::NProto::TMiscExt>(chunkSpec.chunk_meta().extensions());
        if (miscExt) {
            // Note that misc extension for given chunk may already be present as same chunk may appear several times.
            MiscExtMap_.emplace(inputChunk->GetChunkId(), New<TRefCountedMiscExt>(*miscExt));
        } else {
            MiscExtMap_.emplace(inputChunk->GetChunkId(), nullptr);
        }

        auto chunkSlice = CreateInputChunkSlice(std::move(inputChunk));
        if (OperandSchemas_[InputTables_[tableIndex]->OperandIndex]->IsSorted()) {
            InferLimitsFromBoundaryKeys(chunkSlice, RowBuffer_);
        }
        auto dataSlice = CreateUnversionedInputDataSlice(chunkSlice);

        dataSlice->VirtualRowIndex = dataSliceDescriptor.VirtualRowIndex;

        dataSlice->TransformToNew(RowBuffer_, keyLength, /*trimChunkSliceKeys*/ true);

        InputDataSlices_[tableIndex].emplace_back(std::move(dataSlice));
    }

    BoolMask GetRangeMask(EKeyConditionScale scale, const TKeyBound& lowerBound, const TKeyBound& upperBound, int operandIndex)
    {
        const auto& keyCondition = KeyConditions_[operandIndex];
        const auto& keyColumnDataTypes = KeyColumnDataTypes_[operandIndex];
        const auto& schema = OperandSchemas_[operandIndex];

        YT_LOG_TRACE(
            "Checking range mask (Scale: %v, LowerBound: %v, UpperBound: %v, OperandIndex: %v, KeyCondition: %v)",
            scale,
            lowerBound,
            upperBound,
            operandIndex,
            keyCondition ? keyCondition->toString() : "(n/a)");

        if (!keyCondition ||
            !StorageContext_->Settings->Testing->EnableKeyConditionFiltering ||
            keyCondition->alwaysUnknownOrTrue())
        {
            YT_LOG_TRACE("Can not process key condition");
            return BoolMask(true, true);
        }

        int usedKeyColumnCount = keyCondition->getKeyIndices().size();
        YT_VERIFY(usedKeyColumnCount <= OperandSchemas_[operandIndex]->GetKeyColumnCount());

        auto chKeys = ToClickHouseKeys(
            lowerBound,
            upperBound,
            *schema,
            keyColumnDataTypes,
            usedKeyColumnCount,
            StorageContext_->Settings->Testing->MakeUpperBoundInclusive);

        auto toFormattable = [&] (DB::FieldRef* fields, int keyColumnUsed) {
            return MakeFormattableView(
                TRange(fields, fields + keyColumnUsed),
                [&] (auto* builder, DB::FieldRef field) { builder->AppendString(TStringBuf(field.dump())); });
        };

        YT_LOG_TRACE("Chunk keys were successfully converted to CH keys (LowerBound: %v, UpperBound: %v, MinKey: %v, MaxKey: %v)",
            lowerBound,
            upperBound,
            toFormattable(chKeys.MinKey.data(), usedKeyColumnCount),
            toFormattable(chKeys.MaxKey.data(), usedKeyColumnCount));

        YT_LOG_TRACE(
            "Checking if predicate can be true in range (Scale: %v, KeyColumnCount: %v, MinKey: %v, MaxKey: %v)",
            scale,
            usedKeyColumnCount,
            toFormattable(chKeys.MinKey.data(), usedKeyColumnCount),
            toFormattable(chKeys.MaxKey.data(), usedKeyColumnCount));

        BoolMask result = BoolMask(keyCondition->mayBeTrueInRange(
            usedKeyColumnCount,
            chKeys.MinKey.data(),
            chKeys.MaxKey.data(),
            keyColumnDataTypes), false);

        YT_LOG_EVENT(Logger,
            StorageContext_->Settings->LogKeyConditionDetails ? ELogLevel::Debug : ELogLevel::Trace,
            "Range mask (Scale: %v, LowerBound: %v, UpperBound: %v, CanBeTrue: %v)",
            scale,
            lowerBound,
            upperBound,
            result.can_be_true);
        return result;
    }
};

DEFINE_REFCOUNTED_TYPE(TInputFetcher)

////////////////////////////////////////////////////////////////////////////////

TQueryInput FetchInput(
    TStorageContext* storageContext,
    const TQueryAnalysisResult& queryAnalysisResult,
    const std::vector<std::string>& realColumnNames,
    const std::vector<std::string>& virtualColumnNames,
    const TClickHouseIndexBuilder& indexBuilder,
    NTransactionClient::TTransactionId transactionId)
{
    auto inputFetcher = New<TInputFetcher>(
        storageContext,
        queryAnalysisResult,
        realColumnNames,
        virtualColumnNames,
        indexBuilder,
        transactionId);

    WaitFor(inputFetcher->Fetch())
        .ThrowOnError();

    return TQueryInput{
        .OperandCount = inputFetcher->OperandCount(),
        .InputTables = std::move(inputFetcher->InputTables()),
        .StripeList = std::move(inputFetcher->ResultStripeList()),
        .MiscExtMap = std::move(inputFetcher->MiscExtMap()),
        .DataSourceDirectory = std::move(inputFetcher->DataSourceDirectory()),
        .InputStreamDirectory = std::move(inputFetcher->InputStreamDirectory()),
    };
}

void LogSubqueryDebugInfo(const std::vector<TSubquery>& subqueries, TStringBuf phase, const TLogger& logger)
{
    const auto& Logger = logger;

    i64 totalChunkCount = 0;
    i64 totalDataWeight = 0;
    i64 totalRowCount = 0;
    i64 maxDataWeight = -1;
    i64 minDataWeight = 1024 * 1024 * 1_TB;
    int maxChunkCount = -1;
    int minChunkCount = 1'000'000'000;

    if (subqueries.empty()) {
        YT_LOG_INFO("Subquery debug info: result is empty (Phase: %v)", phase);
        return;
    }

    for (const auto& subquery : subqueries) {
        const auto& stripeList = subquery.StripeList;
        totalChunkCount += stripeList->TotalChunkCount;
        totalDataWeight += stripeList->TotalDataWeight;
        totalRowCount += stripeList->TotalRowCount;
        maxDataWeight = std::max(maxDataWeight, stripeList->TotalDataWeight);
        minDataWeight = std::min(minDataWeight, stripeList->TotalDataWeight);
        maxChunkCount = std::max(maxChunkCount, stripeList->TotalChunkCount);
        minChunkCount = std::min(minChunkCount, stripeList->TotalChunkCount);
    }

    YT_LOG_INFO(
        "Subquery debug info (Phase: %v, SubqueryCount: %v, TotalChunkCount: %v, AvgChunkCount: %v, MinChunkCount: %v, MaxChunkCount: %v,"
        "TotalDataWeight: %v, AvgDataWeight: %v, MinDataWeight: %v, MaxDataWeight: %v, TotalRowCount: %v, AvgRowCount: %v)",
        phase,
        subqueries.size(),
        totalChunkCount,
        static_cast<double>(totalChunkCount) / subqueries.size(),
        minChunkCount,
        maxChunkCount,
        totalDataWeight,
        static_cast<double>(totalDataWeight) / subqueries.size(),
        minDataWeight,
        maxDataWeight,
        totalRowCount,
        static_cast<double>(totalRowCount) / subqueries.size());
}

std::vector<TSubquery> BuildThreadSubqueries(
    const TQueryInput& queryInput,
    const TQueryAnalysisResult& queryAnalysisResult,
    int jobCount,
    std::optional<double> samplingRate,
    const TStorageContext* storageContext,
    const TSubqueryConfigPtr& config)
{
    const auto& inputStripeList = queryInput.StripeList;

    auto* queryContext = storageContext->QueryContext;
    const auto& Logger = storageContext->Logger;

    YT_LOG_INFO(
        "Building subqueries (TotalDataWeight: %v, TotalChunkCount: %v, TotalRowCount: %v, "
        "JobCount: %v, PoolKind: %v, ReadInOrderMode: %v, SamplingRate: %v, KeyColumnCount: %v)",
        inputStripeList->TotalDataWeight,
        inputStripeList->TotalChunkCount,
        inputStripeList->TotalRowCount,
        jobCount,
        queryAnalysisResult.PoolKind,
        queryAnalysisResult.ReadInOrderMode,
        samplingRate,
        queryAnalysisResult.KeyColumnCount);

    std::vector<TSubquery> subqueries;

    if (inputStripeList->TotalRowCount * samplingRate.value_or(1.0) < 1.0) {
        YT_LOG_INFO("Total row count times sampling rate is less than 1, returning empty subqueries");
        return subqueries;
    }

    auto jobSizeSpec = CreateClickHouseJobSizeSpec(
        queryContext->Settings->Execution,
        config,
        inputStripeList->TotalDataWeight,
        inputStripeList->TotalRowCount,
        jobCount,
        samplingRate,
        queryAnalysisResult.ReadInOrderMode,
        Logger);

    IPersistentChunkPoolPtr chunkPool;

    if (queryAnalysisResult.PoolKind == EPoolKind::Unordered) {
        chunkPool = CreateUnorderedChunkPool(
            TUnorderedChunkPoolOptions{
                .JobSizeConstraints = jobSizeSpec.JobSizeConstraints,
                .RowBuffer = queryContext->RowBuffer,
                .Logger = queryContext->Logger.WithTag("Name: Root"),
            },
            TInputStreamDirectory({TInputStreamDescriptor(false /*isTeleportable*/, true /*isPrimary*/, false /*isVersioned*/)}));
    } else if (queryAnalysisResult.PoolKind == EPoolKind::Sorted) {
        YT_VERIFY(queryAnalysisResult.KeyColumnCount);
        TComparator comparator(std::vector<ESortOrder>(*queryAnalysisResult.KeyColumnCount, ESortOrder::Ascending));

        // TODO(achulkov2): Make using this fetcher configurable? Not sure whether it could cause degradations. IMO it should make things better.
        auto chunkSliceFetcher = CreateChunkSliceFetcher(
            queryContext->Host->GetConfig()->Subquery->ChunkSliceFetcher,
            queryContext->Client()->GetNativeConnection()->GetNodeDirectory(),
            queryContext->Host->GetClickHouseFetcherInvoker(),
            /*chunkScraper*/ nullptr,
            queryContext->Client(),
            queryContext->RowBuffer,
            queryContext->Logger);

        chunkPool = CreateNewSortedChunkPool(
            TSortedChunkPoolOptions{
                .SortedJobOptions = TSortedJobOptions{
                    .EnableKeyGuarantee = true,
                    .PrimaryComparator = comparator,
                    .PrimaryPrefixLength = *queryAnalysisResult.KeyColumnCount,
                    .ShouldSlicePrimaryTableByKeys = true,
                    .ValidateOrder = false,
                    .MaxTotalSliceCount = std::numeric_limits<int>::max() / 2,
                    .JobSizeTrackerOptions = jobSizeSpec.JobSizeTrackerOptions,
                },
                .MinTeleportChunkSize = std::numeric_limits<i64>::max() / 2,
                .JobSizeConstraints = jobSizeSpec.JobSizeConstraints,
                .RowBuffer = queryContext->RowBuffer,
                .Logger = queryContext->Logger.WithTag("Name: Root"),
            },
            CreateCallbackChunkSliceFetcherFactory(BIND([chunkSliceFetcher = std::move(chunkSliceFetcher)] {
                return chunkSliceFetcher;
            })),
            queryInput.InputStreamDirectory);
    } else {
        Y_UNREACHABLE();
    }

    auto adjustDataSliceForPool = [&] (const TLegacyDataSlicePtr& dataSlice) {
        YT_VERIFY(!dataSlice->IsLegacy);

        if (queryAnalysisResult.PoolKind == EPoolKind::Unordered) {
            dataSlice->LowerLimit().KeyBound = TKeyBound();
            dataSlice->UpperLimit().KeyBound = TKeyBound();

            if (dataSlice->Type == EDataSourceType::UnversionedTable) {
                for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                    chunkSlice->LowerLimit().KeyBound = TKeyBound();
                    chunkSlice->UpperLimit().KeyBound = TKeyBound();
                }
            }

        } else {
            YT_VERIFY(queryAnalysisResult.KeyColumnCount);
            dataSlice->LowerLimit().KeyBound = ShortenKeyBound(dataSlice->LowerLimit().KeyBound, *queryAnalysisResult.KeyColumnCount, queryContext->RowBuffer);
            dataSlice->UpperLimit().KeyBound = ShortenKeyBound(dataSlice->UpperLimit().KeyBound, *queryAnalysisResult.KeyColumnCount, queryContext->RowBuffer);

            if (dataSlice->Type == EDataSourceType::UnversionedTable) {
                // New sorted pool makes no use of chunk slice bounds.
                for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                    chunkSlice->LowerLimit().KeyBound = dataSlice->LowerLimit().KeyBound;
                    chunkSlice->UpperLimit().KeyBound = dataSlice->UpperLimit().KeyBound;
                }
            }
        }
    };

    TReadRangeRegistry inputReadRangeRegistry;

    for (const auto& chunkStripe : inputStripeList->Stripes) {
        for (const auto& dataSlice : chunkStripe->DataSlices) {
            YT_VERIFY(!dataSlice->IsLegacy);
            if ((dataSlice->LowerLimit().KeyBound && !dataSlice->LowerLimit().KeyBound.IsUniversal()) ||
                (dataSlice->UpperLimit().KeyBound && !dataSlice->UpperLimit().KeyBound.IsUniversal()))
            {
                inputReadRangeRegistry.RegisterDataSlice(dataSlice);
            }
            adjustDataSliceForPool(dataSlice);
        }

        chunkPool->Add(chunkStripe);
    }
    chunkPool->Finish();

    while (true) {
        auto cookie = chunkPool->Extract();
        if (cookie == IChunkPoolOutput::NullCookie) {
            break;
        }
        auto& subquery = subqueries.emplace_back();
        subquery.StripeList = chunkPool->GetStripeList(cookie);

        for (const auto& chunkStripe : subquery.StripeList->Stripes) {
            for (const auto& dataSlice : chunkStripe->DataSlices) {
                YT_VERIFY(!dataSlice->IsLegacy);
                if (dataSlice->ReadRangeIndex) {
                    auto comparator = queryInput.DataSourceDirectory->DataSources()[dataSlice->GetTableIndex()].GetComparator();
                    inputReadRangeRegistry.ApplyReadRange(dataSlice, comparator);
                }

                dataSlice->TransformToLegacy(queryContext->RowBuffer);
                YT_VERIFY(dataSlice->IsLegacy);
            }
        }

        subquery.Cookie = cookie;
        if (queryAnalysisResult.PoolKind == EPoolKind::Sorted) {
            auto bounds = static_cast<ISortedChunkPool*>(chunkPool.Get())->GetBounds(cookie);
            subquery.Bounds.first = TOwningKeyBound::FromRowUnchecked(
                TUnversionedOwningRow(bounds.first.Prefix),
                bounds.first.IsInclusive,
                bounds.first.IsUpper);
            subquery.Bounds.second = TOwningKeyBound::FromRowUnchecked(
                TUnversionedOwningRow(bounds.second.Prefix),
                bounds.second.IsInclusive,
                bounds.second.IsUpper);
        }
    }

    // Pools do not produce results suitable for further query analysis.
    // For each subquery, we produce a single potentially empty slice for each operand.
    // Each slice contains a flattened list of all corresponding data slices.
    // In the end, each stripe list contains exactly one or two stripes, depending
    // on the number of operands.
    for (auto& subquery : subqueries) {
        auto flattenedStripeList = New<TChunkStripeList>();
        flattenedStripeList->Stripes.resize(queryInput.OperandCount);
        for (int operandIndex = 0; operandIndex < queryInput.OperandCount; ++operandIndex) {
            flattenedStripeList->Stripes[operandIndex] = New<TChunkStripe>();
        }
        for (const auto& stripe : subquery.StripeList->Stripes) {
            int operandIndex = queryInput.InputTables[stripe->GetTableIndex()]->OperandIndex;
            YT_VERIFY(operandIndex >= 0);
            YT_VERIFY(operandIndex < queryInput.OperandCount);
            auto& flattenedStripe = flattenedStripeList->Stripes[operandIndex];
            for (const auto& dataSlice : stripe->DataSlices) {
                flattenedStripe->DataSlices.push_back(dataSlice);
            }
        }
        for (const auto& stripe : flattenedStripeList->Stripes) {
            AccountStripeInList(stripe, flattenedStripeList);
        }
        subquery.StripeList.Swap(flattenedStripeList);
    }

    YT_LOG_INFO("Pool produced subqueries (SubqueryCount: %v)", subqueries.size());
    LogSubqueryDebugInfo(subqueries, "AfterPool", Logger);

    if (samplingRate && *samplingRate != 1.0) {
        double sampledSubqueryCount = std::round(*samplingRate * subqueries.size());
        YT_LOG_INFO("Leaving random subqueries to perform sampling (SubqueryCount: %v, SampledSubqueryCount: %v)",
            subqueries.size(),
            sampledSubqueryCount);
        std::mt19937 gen;
        std::shuffle(subqueries.begin(), subqueries.end(), gen);
        subqueries.resize(std::min<int>(subqueries.size(), sampledSubqueryCount));
        LogSubqueryDebugInfo(subqueries, "AfterSampling", Logger);
    }

    // TODO(dakovalkov): Should we do it for Unordered chunk pool for the sake of better caching?
    if (queryAnalysisResult.PoolKind == EPoolKind::Sorted) {
        std::sort(subqueries.begin(), subqueries.end(), [] (const TSubquery& lhs, const TSubquery& rhs) {
            return lhs.Cookie < rhs.Cookie;
        });
        LogSubqueryDebugInfo(subqueries, "AfterSort", Logger);
    }

    // Enlarge subqueries if needed.
    {
        std::vector<TSubquery> enlargedSubqueries;
        for (size_t leftIndex = 0, rightIndex = 0; leftIndex < subqueries.size(); leftIndex = rightIndex) {
            i64 dataWeight = 0;
            while (rightIndex < subqueries.size()) {
                dataWeight += subqueries[rightIndex].StripeList->TotalDataWeight;
                rightIndex++;
                if (dataWeight >= config->MinDataWeightPerThread) {
                    break;
                }
            }
            if (leftIndex + 1 == rightIndex) {
                enlargedSubqueries.emplace_back(std::move(subqueries[leftIndex]));
            } else {
                YT_LOG_DEBUG("Joining several subqueries together (LeftIndex: %v, RightIndex: %v, DataWeight: %v)",
                    leftIndex,
                    rightIndex,
                    dataWeight);
                auto& enlargedSubquery = enlargedSubqueries.emplace_back();
                enlargedSubquery.StripeList = New<TChunkStripeList>();
                std::vector<TChunkStripePtr> stripes(subqueries[leftIndex].StripeList->Stripes.size());
                for (auto& stripe : stripes) {
                    stripe = New<TChunkStripe>();
                }
                for (size_t index = leftIndex; index < rightIndex; ++index) {
                    for (size_t stripeIndex = 0; stripeIndex < stripes.size(); ++stripeIndex) {
                        for (auto& dataSlice : subqueries[index].StripeList->Stripes[stripeIndex]->DataSlices) {
                            stripes[stripeIndex]->DataSlices.emplace_back(std::move(dataSlice));
                        }
                    }
                }
                for (auto& stripe : stripes) {
                    AddStripeToList(std::move(stripe), enlargedSubquery.StripeList);
                }

                enlargedSubquery.Bounds = {subqueries[leftIndex].Bounds.first, subqueries[rightIndex - 1].Bounds.second};
                // This cookie is used as a hint for sorting in storage distributor, so the following line is ok.
                enlargedSubquery.Cookie = subqueries[leftIndex].Cookie;
            }
        }
        subqueries.swap(enlargedSubqueries);
        LogSubqueryDebugInfo(subqueries, "AfterEnlarging", Logger);
    }

    return subqueries;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
