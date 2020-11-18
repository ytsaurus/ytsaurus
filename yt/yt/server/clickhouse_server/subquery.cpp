#include "subquery.h"

#include "query_context.h"
#include "config.h"
#include "subquery_spec.h"
#include "schema.h"
#include "helpers.h"
#include "table.h"
#include "host.h"
#include "query_analyzer.h"
#include "job_size_constraints.h"

#include <yt/server/lib/chunk_pools/chunk_stripe.h>
#include <yt/server/lib/chunk_pools/helpers.h>
#include <yt/server/lib/chunk_pools/unordered_chunk_pool.h>
#include <yt/server/lib/chunk_pools/legacy_sorted_chunk_pool.h>

#include <yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/chunk_spec_fetcher.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/object_attribute_cache.h>

#include <yt/ytlib/security_client/permission_cache.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/chunk_slice_fetcher.h>
#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/table_client/table_ypath_proxy.h>
#include <yt/ytlib/table_client/columnar_statistics_fetcher.h>
#include <yt/ytlib/table_client/table_read_spec.h>

#include <yt/client/tablet_client/table_mount_cache.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/ypath/rich.h>

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/row_buffer.h>

#include <yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/core/logging/log.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/ytree/convert.h>
#include <yt/core/concurrency/action_queue.h>

#include <Storages/MergeTree/KeyCondition.h>
#include <DataTypes/IDataType.h>

#include <cmath>

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

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EKeyConditionScale,
    (Partition)
    (Tablet)
    (TopLevelDataSlice)
)

class TInputFetcher
    : public TRefCounted
{
public:
    DEFINE_BYREF_RW_PROPERTY(TDataSourceDirectoryPtr, DataSourceDirectory, New<TDataSourceDirectory>());
    DEFINE_BYREF_RW_PROPERTY(TChunkStripeListPtr, ResultStripeList, New<TChunkStripeList>());
    using TMiscExtMap = THashMap<TChunkId, TRefCountedMiscExtPtr>;
    DEFINE_BYREF_RW_PROPERTY(TMiscExtMap, MiscExtMap);

public:
    TInputFetcher(
        TStorageContext* storageContext,
        const TQueryAnalysisResult& queryAnalysisResult,
        const std::vector<std::string>& columnNames)
        : StorageContext_(storageContext)
        , QueryContext_(StorageContext_->QueryContext)
        , Client_(QueryContext_->Client())
        , Invoker_(CreateSerializedInvoker(QueryContext_->Host->GetClickHouseFetcherInvoker()))
        , TableSchemas_(queryAnalysisResult.TableSchemas)
        , KeyConditions_(queryAnalysisResult.KeyConditions)
        , ColumnNames_(columnNames.begin(), columnNames.end())
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
            KeyColumnDataTypes_.push_back(ToDataTypes(*TableSchemas_[operandIndex]->ToKeys(), StorageContext_->Settings->Composite));
        }
    }

    TFuture<void> Fetch()
    {
        return BIND(&TInputFetcher::DoFetch, MakeWeak(this))
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    TStorageContext* StorageContext_;
    TQueryContext* QueryContext_;

    NApi::NNative::IClientPtr Client_;

    IInvokerPtr Invoker_;


    std::vector<TTableSchemaPtr> TableSchemas_;
    std::vector<std::optional<DB::KeyCondition>> KeyConditions_;

    std::vector<TString> ColumnNames_;

    std::vector<DB::DataTypes> KeyColumnDataTypes_;

    //! Number of operands to join. May be either 1 (if no JOIN present or joinee is not a YT table) or 2 (otherwise).
    int OperandCount_ = 0;

    std::vector<TTablePtr> InputTables_;

    TSubqueryConfigPtr Config_;

    TRowBufferPtr RowBuffer_;

    std::vector<TChunkStripePtr> ResultStripes_;

    // Table index to input data slices.
    std::vector<std::vector<TLegacyDataSlicePtr>> InputDataSlices_;

    std::vector<TTableReadSpec> TableReadSpecs_;

    TLogger Logger;

    //! Fetch input tables. Result goes to ResultStripeList_.
    void DoFetch()
    {
        FetchTables();

        if (Config_->UseColumnarStatistics) {
            auto columnarStatisticsFetcher = New<TColumnarStatisticsFetcher>(
                Config_->ChunkSliceFetcher,
                /*nodeDirectory =*/ nullptr,
                Invoker_,
                /*chunkScraper =*/ nullptr,
                Client_,
                EColumnarStatisticsFetcherMode::FromMaster,
                /*storeChunkStatistics =*/ false,
                Logger);

            for (auto& resultStripe : ResultStripes_) {
                for (auto& inputDataSlice : resultStripe->DataSlices) {
                    for (auto& inputChunkSlice : inputDataSlice->ChunkSlices) {
                        columnarStatisticsFetcher->AddChunk(inputChunkSlice->GetInputChunk(), ColumnNames_);
                    }
                }
            }

            WaitFor(columnarStatisticsFetcher->Fetch())
                .ThrowOnError();
            columnarStatisticsFetcher->ApplyColumnSelectivityFactors();
        }

        for (auto& stripe : ResultStripes_) {
            auto& dataSlices = stripe->DataSlices;

            auto it = std::remove_if(dataSlices.begin(), dataSlices.end(), [&] (const TLegacyDataSlicePtr& dataSlice) {
                if (!dataSlice->LegacyLowerLimit().Key && !dataSlice->LegacyUpperLimit().Key) {
                    return false;
                }
                return !GetRangeMask(
                    EKeyConditionScale::TopLevelDataSlice,
                    dataSlice->LegacyLowerLimit().Key,
                    dataSlice->LegacyUpperLimit().Key,
                    dataSlice->InputStreamIndex).can_be_true;
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

    //! Fetch all tables and fill ResultStripes_.
    void FetchTables()
    {
        int staticTableCount = 0;
        int dynamicTableCount = 0;
        for (const auto& table : InputTables_) {
            if (table->Dynamic) {
                ++dynamicTableCount;
            } else {
                ++staticTableCount;
            }
        }
        if (dynamicTableCount > 0 && staticTableCount > 0) {
            THROW_ERROR_EXCEPTION("Reading static tables together with dynamic tables is not supported yet");
        }

        YT_LOG_INFO(
            "Fetching input tables (StaticTableCount: %v, DynamicTableCount: %v)",
            staticTableCount,
            dynamicTableCount);

        // We fetch table read spec for each table separately, put them into TableReadSpecs_ vector,
        // which will later be joined by JoinTableReadSpecs function.
        TableReadSpecs_.resize(InputTables_.size());
        FetchTableReadSpecs();

        YT_LOG_INFO("Input tables fetched, preparing data slices");

        auto joinedTableReadSpec = JoinTableReadSpecs(TableReadSpecs_);
        YT_VERIFY(joinedTableReadSpec.DataSourceDirectory->DataSources().size() == InputTables_.size());

        // Transform (single-chunk) data slice descriptors into data slices.
        // NB: by this moment versioned chunk data slice descriptors store separate chunk specs,
        // so they are not "correct" from the dynamic table point of view.
        InputDataSlices_.resize(InputTables_.size());
        for (auto& dataSliceDescriptor : joinedTableReadSpec.DataSliceDescriptors) {
            RegisterDataSlice(dataSliceDescriptor);
        }

        // Fix slices for dynamic tables.
        for (size_t tableIndex = 0; tableIndex < InputTables_.size(); ++tableIndex) {
            if (InputTables_[tableIndex]->Dynamic) {
                CombineVersionedDataSlices(tableIndex);
            }
        }

        // Put data slices to their destination stripes.
        ResultStripes_.resize(OperandCount_);
        for (auto& stripe : ResultStripes_) {
            stripe = New<TChunkStripe>();
        }
        for (size_t tableIndex = 0; tableIndex < InputTables_.size(); ++tableIndex) {
            auto operandIndex = InputTables_[tableIndex]->OperandIndex;
            for (auto& dataSlice : InputDataSlices_[tableIndex]) {
                dataSlice->InputStreamIndex = operandIndex;
                InferLimitsFromBoundaryKeys(
                    dataSlice,
                    RowBuffer_,
                    joinedTableReadSpec.DataSourceDirectory->DataSources()[tableIndex].GetVirtualValueDirectory());
                ResultStripes_[operandIndex]->DataSlices.emplace_back(std::move(dataSlice));
            }
        }

        DataSourceDirectory_ = std::move(joinedTableReadSpec.DataSourceDirectory);

        YT_LOG_INFO("Data slices ready");
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
                InferLimitsFromBoundaryKeys(chunkSlice, RowBuffer_, InputTables_[tableIndex]->Schema->GetKeyColumnCount());
                chunkSlices.emplace_back(std::move(chunkSlice));
            }
        }

        dataSlices = CombineVersionedChunkSlices(chunkSlices);
    }

    //! When reading from dynamic tables, it is generally a good idea to start with checking each of the tablets
    //! against key condition in order to reduce number of chunk specs to be fetched from master.
    //! This method does such optimization.
    void InferDynamicTableRangesFromPivotKeys()
    {
        YT_LOG_DEBUG("Inferring dynamic table ranges from tablet pivot keys");
        for (const auto& inputTable : InputTables_) {
            if (!inputTable->Dynamic) {
                continue;
            }
            if (inputTable->Path.HasNontrivialRanges()) {
                // We skip tables with non-trivial ranges.
                YT_LOG_DEBUG("Skipping table as it already has non-trivial ranges (Table: %v)", inputTable->Path);
                continue;
            }
            std::vector<TReadRange> ranges;

            const auto& tablets = inputTable->TableMountInfo->Tablets;

            std::optional<TLegacyKey> beginKey;

            auto flushRange = [&] (TLegacyKey key) {
                YT_VERIFY(beginKey);
                auto& range = ranges.emplace_back();
                range.LowerLimit().SetKey(TLegacyOwningKey(*beginKey));
                range.UpperLimit().SetKey(TLegacyOwningKey(key));
                beginKey = std::nullopt;
            };

            for (int index = 0; index < static_cast<int>(tablets.size()); ++index) {
                const auto& lowerKey = tablets[index]->PivotKey;
                const auto& upperKey = (index + 1 < static_cast<int>(tablets.size())) ? tablets[index + 1]->PivotKey : MaxKey();
                if (GetRangeMask(EKeyConditionScale::Tablet, lowerKey, upperKey, inputTable->OperandIndex).can_be_true) {
                    if (!beginKey) {
                        beginKey = lowerKey;
                    }
                } else {
                    if (beginKey) {
                        flushRange(lowerKey);
                    }
                }
            }

            if (beginKey) {
                flushRange(MaxKey());
            }

            YT_LOG_DEBUG("Dynamic table ranges inferred from tablet pivot keys (Table: %v, Ranges: %v)", inputTable->Path, ranges);
            inputTable->Path.SetRanges(ranges);
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

        auto chunkSpecFetcher = New<TChunkSpecFetcher>(
            Client_,
            nullptr /* nodeDirectory */,
            Invoker_,
            Config_->MaxChunksPerFetch,
            Config_->MaxChunksPerLocateRequest,
            [=] (const TChunkOwnerYPathProxy::TReqFetchPtr& req, int /*tableIndex*/) {
                req->set_fetch_all_meta_extensions(false);
                req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
                req->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value);
                req->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::THeavyColumnStatisticsExt>::Value);
                if (!QueryContext_->Settings->DynamicTable->EnableDynamicStoreRead) {
                    req->set_omit_dynamic_stores(true);
                }
                SetTransactionId(req, NullTransactionId);
                SetSuppressAccessTracking(req, true);
                SetSuppressExpirationTimeoutRenewal(req, true);
            },
            Logger);

        for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables_.size()); ++tableIndex) {
            const auto& table = InputTables_[tableIndex];
            TableReadSpecs_[tableIndex].DataSourceDirectory = New<TDataSourceDirectory>();
            auto& dataSource = TableReadSpecs_[tableIndex].DataSourceDirectory->DataSources().emplace_back();
            if (table->Dynamic) {
                dataSource = MakeVersionedDataSource(
                    std::nullopt /* path */,
                    table->Schema,
                    std::nullopt /* columns */,
                    // TODO(max42): YT-10402, omitted inaccessible columns
                    {},
                    table->Path.GetTimestamp().value_or(AsyncLastCommittedTimestamp),
                    table->Path.GetRetentionTimestamp().value_or(NullTimestamp));
            } else {
                dataSource = MakeUnversionedDataSource(
                    std::nullopt /* path */,
                    table->Schema,
                    std::nullopt /* columns */,
                    // TODO(max42): YT-10402, omitted inaccessible columns
                    {});
            }

            chunkSpecFetcher->Add(
                table->ObjectId,
                table->ExternalCellTag,
                table->ChunkCount,
                tableIndex,
                table->Path.GetRanges());
        }

        WaitFor(chunkSpecFetcher->Fetch())
            .ThrowOnError();

        YT_LOG_INFO("Chunk specs fetched (ChunkCount: %v)", chunkSpecFetcher->ChunkSpecs().size());

        for (auto& chunkSpec : chunkSpecFetcher->ChunkSpecs()) {
            auto tableIndex = chunkSpec.table_index();
            // Table indices will be properly reassigned later by JoinTableReadSpecs.
            chunkSpec.set_table_index(0);
            TableReadSpecs_[tableIndex].DataSliceDescriptors.emplace_back(TDataSliceDescriptor(std::move(chunkSpec)));
        }
    }

    //! Wrap chunk spec from data slice descriptor into data slice,
    //! keep its misc ext and push it to InputDataSlices_[tableIndex].
    void RegisterDataSlice(TDataSliceDescriptor& dataSliceDescriptor)
    {
        auto& chunkSpec = dataSliceDescriptor.GetSingleChunk();
        auto inputChunk = New<TInputChunk>(chunkSpec);
        auto miscExt = FindProtoExtension<NChunkClient::NProto::TMiscExt>(chunkSpec.chunk_meta().extensions());
        if (miscExt) {
            // Note that misc extension for given chunk may already be present as same chunk may appear several times.
            MiscExtMap_.emplace(inputChunk->ChunkId(), New<TRefCountedMiscExt>(*miscExt));
        } else {
            MiscExtMap_.emplace(inputChunk->ChunkId(), nullptr);
        }

        auto tableIndex = inputChunk->GetTableIndex();
        auto dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(std::move(inputChunk)));
        dataSlice->VirtualRowIndex = dataSliceDescriptor.VirtualRowIndex;

        InputDataSlices_[tableIndex].emplace_back(std::move(dataSlice));
    }

    BoolMask GetRangeMask(EKeyConditionScale scale, TLegacyKey lowerKey, TLegacyKey upperKey, int operandIndex)
    {
        YT_LOG_TRACE(
            "Checking range mask (Scale: %v, LowerKey: %v, UpperKey: %v, OperandIndex: %v, KeyCondition: %v)",
            scale,
            lowerKey,
            upperKey,
            operandIndex,
            KeyConditions_[operandIndex] ? KeyConditions_[operandIndex]->toString() : "(n/a)");

        if (!KeyConditions_[operandIndex]) {
            return BoolMask(true, true);
        }

        auto keyColumnCount = TableSchemas_[operandIndex]->GetKeyColumnCount();
        DB::FieldRef minKey[keyColumnCount];
        DB::FieldRef maxKey[keyColumnCount];

        // TODO(max42): Refactor!
        {
            int index = 0;
            for ( ; index < std::min<int>(lowerKey.GetCount(), keyColumnCount); ++index) {
                if (lowerKey[index].Type != EValueType::Max && lowerKey[index].Type != EValueType::Min && lowerKey[index].Type != EValueType::Null) {
                    minKey[index] = ConvertToField(lowerKey[index]);
                } else {
                    // Fill the rest with nulls.
                    break;
                }
            }
            for (; index < keyColumnCount; ++index) {
                minKey[index] = ConvertToField(MakeUnversionedNullValue());
            }
        }

        bool isMax = upperKey.GetCount() >= 1 && upperKey[0].Type == EValueType::Max;
        {
            if (!isMax) {
                int index = 0;
                for (; index < std::min<int>(upperKey.GetCount(), keyColumnCount); ++index) {
                    if (upperKey[index].Type == EValueType::Max) {
                        // We can not do anything better than this CH has no meaning of "infinite" value.
                        // Pretend like this is a half-open ray.
                        isMax = true;
                        break;
                    } else if (upperKey[index].Type == EValueType::Min || upperKey[index].Type == EValueType::Null) {
                        // Fill the rest with nulls.
                        break;
                    } else {
                        maxKey[index] = ConvertToField(upperKey[index]);
                    }
                }
                if (!isMax) {
                    for (; index < keyColumnCount; ++index) {
                        maxKey[index] = ConvertToField(MakeUnversionedNullValue());
                    }
                }
            }
        }

        auto toFormattable = [&] (DB::FieldRef* fields) {
            return MakeFormattableView(
                MakeRange(fields, fields + keyColumnCount),
                [&] (auto* builder, DB::FieldRef field) { builder->AppendString(TString(field.dump())); });
        };

        // We do not have any Max-equivalent in ClickHouse, but luckily again we have mayBeTrueAfter method which allows
        // us checking key condition on half-open ray.
        BoolMask result;
        if (!isMax) {
            YT_LOG_TRACE(
                "Checking if predicate can be true in range (Scale: %v, KeyColumnCount: %v, MinKey: %v, MaxKey: %v)",
                scale,
                keyColumnCount,
                toFormattable(minKey),
                toFormattable(maxKey));
            result = BoolMask(KeyConditions_[operandIndex]->mayBeTrueInRange(
                keyColumnCount,
                minKey,
                maxKey,
                KeyColumnDataTypes_[operandIndex]), false);
        } else {
            YT_LOG_TRACE(
                "Checking if predicate can be true in half-open ray (Scale: %v, KeyColumnCount: %v, MinKey: %v)",
                scale,
                keyColumnCount,
                toFormattable(minKey));
            result = BoolMask(KeyConditions_[operandIndex]->mayBeTrueAfter(
                keyColumnCount,
                minKey,
                KeyColumnDataTypes_[operandIndex]), false);
        }

        YT_LOG_EVENT(Logger,
            StorageContext_->Settings->LogKeyConditionDetails ? ELogLevel::Debug : ELogLevel::Trace,
            "Range mask (Scale: %v, LowerKey: %v, UpperKey: %v, CanBeTrue: %v)",
            scale,
            lowerKey,
            upperKey,
            result.can_be_true);
        return result;
    }
};

DEFINE_REFCOUNTED_TYPE(TInputFetcher);

////////////////////////////////////////////////////////////////////////////////

TQueryInput FetchInput(
    TStorageContext* storageContext,
    const TQueryAnalysisResult& queryAnalysisResult,
    const std::vector<std::string>& columnNames)
{
    auto inputFetcher = New<TInputFetcher>(storageContext, queryAnalysisResult, columnNames);
    WaitFor(inputFetcher->Fetch())
        .ThrowOnError();

    return TQueryInput{
        .StripeList = std::move(inputFetcher->ResultStripeList()),
        .MiscExtMap = std::move(inputFetcher->MiscExtMap()),
        .DataSourceDirectory = std::move(inputFetcher->DataSourceDirectory()),
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

std::vector<TSubquery> BuildSubqueries(
    const TChunkStripeListPtr& inputStripeList,
    std::optional<int> keyColumnCount,
    EPoolKind poolKind,
    int jobCount,
    std::optional<double> samplingRate,
    const TStorageContext* storageContext,
    const TSubqueryConfigPtr& config)
{
    auto* queryContext = storageContext->QueryContext;
    const auto& Logger = storageContext->Logger;

    YT_LOG_INFO(
        "Building subqueries (TotalDataWeight: %v, TotalChunkCount: %v, TotalRowCount: %v, "
        "JobCount: %v, PoolKind: %v, SamplingRate: %v, KeyColumnCount: %v)",
        inputStripeList->TotalDataWeight,
        inputStripeList->TotalChunkCount,
        inputStripeList->TotalRowCount,
        jobCount,
        poolKind,
        samplingRate,
        keyColumnCount);

    std::vector<TSubquery> subqueries;

    if (inputStripeList->TotalRowCount * samplingRate.value_or(1.0) < 1.0) {
        YT_LOG_INFO("Total row count times sampling rate is less than 1, returning empty subqueries");
        return subqueries;
    }

    auto jobSizeConstraints = CreateClickHouseJobSizeConstraints(
        config,
        inputStripeList->TotalDataWeight,
        inputStripeList->TotalRowCount,
        jobCount,
        samplingRate,
        Logger);

    IChunkPoolPtr chunkPool;

    if (poolKind == EPoolKind::Unordered) {
        chunkPool = CreateUnorderedChunkPool(
            TUnorderedChunkPoolOptions{
                .JobSizeConstraints = jobSizeConstraints,
                .RowBuffer = queryContext->RowBuffer,
                .OperationId = queryContext->QueryId,
            },
            TInputStreamDirectory({TInputStreamDescriptor(false /* isTeleportable */, true /* isPrimary */, false /* isVersioned */)}));
    } else if (poolKind == EPoolKind::Sorted) {
        YT_VERIFY(keyColumnCount);
        chunkPool = CreateLegacySortedChunkPool(
            TSortedChunkPoolOptions{
                .SortedJobOptions = TSortedJobOptions{
                    .EnableKeyGuarantee = true,
                    .PrimaryPrefixLength = *keyColumnCount,
                    .ShouldSlicePrimaryTableByKeys = true,
                    .MaxTotalSliceCount = std::numeric_limits<int>::max() / 2,
                },
                .MinTeleportChunkSize = std::numeric_limits<i64>::max() / 2,
                .JobSizeConstraints = jobSizeConstraints,
                .OperationId = queryContext->QueryId,
                .RowBuffer = queryContext->RowBuffer,
            },
            CreateCallbackChunkSliceFetcherFactory(BIND([] { return nullptr; })),
            TInputStreamDirectory({
                TInputStreamDescriptor(false /* isTeleportable */, true /* isPrimary */, false /* isVersioned */),
                TInputStreamDescriptor(false /* isTeleportable */, true /* isPrimary */, false /* isVersioned */)
            }));
    } else {
        Y_UNREACHABLE();
    }

    for (const auto& chunkStripe : inputStripeList->Stripes) {
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
        subquery.Cookie = cookie;
        if (poolKind == EPoolKind::Sorted) {
            auto limits = static_cast<ISortedChunkPool*>(chunkPool.Get())->GetLimits(cookie);
            subquery.Limits.first = TUnversionedOwningRow(limits.first);
            subquery.Limits.second = TUnversionedOwningRow(limits.second);
        }
    }

    // Pools not always produce suitable stripelists for further query
    // analyzer business transform them to the proper state.
    if (poolKind == EPoolKind::Unordered) {
        // Stripe lists from unordered pool consist of lot of stripes; we expect a single
        // stripe with lots of data slices inside, so we flatten them.
        for (auto& subquery : subqueries) {
            auto flattenedStripe = New<TChunkStripe>();
            for (const auto& stripe : subquery.StripeList->Stripes) {
                for (const auto& dataSlice : stripe->DataSlices) {
                    flattenedStripe->DataSlices.emplace_back(dataSlice);
                }
            }
            auto flattenedStripeList = New<TChunkStripeList>();
            AddStripeToList(std::move(flattenedStripe), flattenedStripeList);
            subquery.StripeList.Swap(flattenedStripeList);
        }
    } else {
        // Stripe lists from sorted pool sometimes miss stripes from certain inputs; we want
        // empty stripes to be present in any case.
        for (auto& subquery : subqueries) {
            auto fullStripeList = New<TChunkStripeList>();
            fullStripeList->Stripes.resize(inputStripeList->Stripes.size());
            for (auto& stripe : subquery.StripeList->Stripes) {
                size_t operandIndex = stripe->GetInputStreamIndex();
                YT_VERIFY(operandIndex >= 0);
                YT_VERIFY(operandIndex < fullStripeList->Stripes.size());
                YT_VERIFY(!fullStripeList->Stripes[operandIndex]);
                fullStripeList->Stripes[operandIndex] = std::move(stripe);
            }
            for (auto& stripe : fullStripeList->Stripes) {
                if (!stripe) {
                    stripe = New<TChunkStripe>();
                }
            }
            subquery.StripeList.Swap(fullStripeList);
        }
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

                enlargedSubquery.Limits = {subqueries[leftIndex].Limits.first, subqueries[rightIndex - 1].Limits.second};
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
