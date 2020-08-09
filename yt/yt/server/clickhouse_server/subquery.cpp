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
#include <yt/server/lib/chunk_pools/sorted_chunk_pool.h>

#include <yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/input_data_slice.h>
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

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/ypath/rich.h>

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
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NLogging;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

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
        , Invoker_(CreateSerializedInvoker(QueryContext_->Host->GetWorkerInvoker()))
        , TableSchemas_(queryAnalysisResult.TableSchemas)
        , KeyConditions_(queryAnalysisResult.KeyConditions)
        , ColumnNames_(columnNames)
        , KeyColumnCount_(queryAnalysisResult.KeyColumnCount)
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

    const std::vector<std::string>& ColumnNames_;

    std::optional<int> KeyColumnCount_ = 0;
    DB::DataTypes KeyColumnDataTypes_;

    //! Number of operands to join. May be either 1 (if no JOIN present or joinee is not a YT table) or 2 (otherwise).
    int OperandCount_ = 0;

    std::vector<TTablePtr> InputTables_;

    TSubqueryConfigPtr Config_;

    std::vector<TInputChunkPtr> InputChunks_;

    TRowBufferPtr RowBuffer_;

    TLogger Logger;

    void DoFetch()
    {
        // TODO(max42): do we need this check?
        if (!TableSchemas_.empty()) {
            // TODO(max42): it's better for query analyzer to do this substitution...
            if (TableSchemas_.size() == 1) {
                KeyColumnCount_ = TableSchemas_.front()->GetKeyColumnCount();
            }
            auto commonSchemaPart = TableSchemas_.front()->Columns();
            if (KeyColumnCount_) {
                commonSchemaPart.resize(*KeyColumnCount_);
            }
            KeyColumnDataTypes_ = ToDataTypes(TTableSchema(commonSchemaPart));
        }

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

            std::vector<TString> columnNames(ColumnNames_.begin(), ColumnNames_.end());
            for (const auto& inputChunk : InputChunks_) {
                columnarStatisticsFetcher->AddChunk(inputChunk, columnNames);
            }

            WaitFor(columnarStatisticsFetcher->Fetch())
                .ThrowOnError();
            columnarStatisticsFetcher->ApplyColumnSelectivityFactors();
        }

        std::vector<TChunkStripePtr> stripes;
        for (int index = 0; index < OperandCount_; ++index) {
            stripes.emplace_back(New<TChunkStripe>());
        }

        CollectUnversionedDataSlices(stripes);
        CollectVersionedDataSlices(stripes);

        for (auto& stripe : stripes) {
            auto& dataSlices = stripe->DataSlices;

            auto it = std::remove_if(dataSlices.begin(), dataSlices.end(), [&] (const TInputDataSlicePtr& dataSlice) {
                if (!dataSlice->LowerLimit().Key && !dataSlice->UpperLimit().Key) {
                    return false;
                }
                return !GetRangeMask(dataSlice->LowerLimit().Key, dataSlice->UpperLimit().Key, dataSlice->GetTableIndex()).can_be_true;
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

    void CollectUnversionedDataSlices(std::vector<TChunkStripePtr>& destinationStripes)
    {
        YT_LOG_DEBUG("Collecting unversioned data slices");

        for (const auto& chunk : InputChunks_) {
            auto tableIndex = chunk->GetTableIndex();
            const auto& table = InputTables_[tableIndex];
            if (table->Dynamic) {
                continue;
            }
            auto operandIndex = table->OperandIndex;
            SubstituteOperandIndex(chunk);
            auto dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
            InferLimitsFromBoundaryKeys(dataSlice, RowBuffer_);
            destinationStripes[operandIndex]->DataSlices.emplace_back(std::move(dataSlice));
        }
    }

    void CollectVersionedDataSlices(std::vector<TChunkStripePtr>& destinationStripes)
    {
        YT_LOG_DEBUG("Collecting versioned data slices");

        std::vector<std::vector<TInputChunkSlicePtr>> dynamicTableChunkSlices(InputTables_.size());

        for (const auto& chunk : InputChunks_) {
            auto tableIndex = chunk->GetTableIndex();
            const auto& table = InputTables_[tableIndex];
            if (!table->Dynamic) {
                continue;
            }

            SubstituteOperandIndex(chunk);
            auto chunkSlice = CreateInputChunkSlice(chunk);
            InferLimitsFromBoundaryKeys(chunkSlice, RowBuffer_, table->Schema->GetKeyColumnCount());
            dynamicTableChunkSlices[tableIndex].emplace_back(std::move(chunkSlice));
        }

        for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables_.size()); ++tableIndex) {
            const auto& chunkSlices = dynamicTableChunkSlices[tableIndex];
            if (chunkSlices.empty()) {
                continue;
            }

            const auto& table = InputTables_[tableIndex];
            auto operandIndex = table->OperandIndex;
            auto dataSlices = CombineVersionedChunkSlices(chunkSlices);

            for (auto& dataSlice : dataSlices) {
                destinationStripes[operandIndex]->DataSlices.emplace_back(std::move(dataSlice));
            }
        }
    }

    void SubstituteOperandIndex(const TInputChunkPtr& chunk)
    {
        const auto& table = InputTables_[chunk->GetTableIndex()];
        chunk->SetTableIndex(table->OperandIndex);
    }

    void FetchTables()
    {
        i64 totalChunkCount = 0;
        for (const auto& inputTable : InputTables_) {
            totalChunkCount += inputTable->ChunkCount;
        }
        YT_LOG_INFO("Fetching input tables (InputTableCount: %v, TotalChunkCount: %v)",
            InputTables_.size(),
            totalChunkCount);

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
                SetTransactionId(req, NullTransactionId);
                SetSuppressAccessTracking(req, true);
            },
            Logger);

        int dynamicTableCount = 0;
        int staticTableCount = 0;

        for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables_.size()); ++tableIndex) {
            const auto& table = InputTables_[tableIndex];
            TDataSource dataSource;
            if (table->Dynamic) {
                dataSource = MakeVersionedDataSource(
                    std::nullopt /* path */,
                    table->Schema,
                    std::nullopt /* columns */,
                    // TODO(max42): YT-10402, omitted inaccessible columns
                    {},
                    table->Path.GetTimestamp().value_or(AsyncLastCommittedTimestamp),
                    table->Path.GetRetentionTimestamp().value_or(NullTimestamp));
                ++dynamicTableCount;
            } else {
                dataSource = MakeUnversionedDataSource(
                    std::nullopt /* path */,
                    table->Schema,
                    std::nullopt /* columns */,
                    // TODO(max42): YT-10402, omitted inaccessible columns
                    {});
                ++staticTableCount;
            }
            DataSourceDirectory_->DataSources().push_back(std::move(dataSource));

            chunkSpecFetcher->Add(
                table->ObjectId,
                table->ExternalCellTag,
                table->ChunkCount,
                tableIndex,
                table->Path.GetRanges());
        }

        YT_LOG_INFO("Fetching chunk specs (StaticTableCount: %v, DynamicTableCount: %v)",
            staticTableCount,
            dynamicTableCount);

        if (dynamicTableCount > 0 && staticTableCount > 0) {
            THROW_ERROR_EXCEPTION("Reading static tables together with dynamic tables is not supported yet");
        }

        WaitFor(chunkSpecFetcher->Fetch())
            .ThrowOnError();

        YT_LOG_INFO("Chunk specs fetched (ChunkCount: %v)", chunkSpecFetcher->ChunkSpecs().size());

        for (const auto& chunkSpec : chunkSpecFetcher->ChunkSpecs()) {
            auto inputChunk = New<TInputChunk>(chunkSpec);
            auto miscExt = FindProtoExtension<NChunkClient::NProto::TMiscExt>(chunkSpec.chunk_meta().extensions());
            if (miscExt) {
                // Note that misc extension for given chunk may already be present as same chunk may appear several times.
                MiscExtMap_.emplace(inputChunk->ChunkId(), New<TRefCountedMiscExt>(*miscExt));
            } else {
                MiscExtMap_.emplace(inputChunk->ChunkId(), nullptr);
            }
            InputChunks_.emplace_back(std::move(inputChunk));
        }
        YT_LOG_DEBUG("Misc extension map statistics (Count: %v)", MiscExtMap_.size());
    }

    BoolMask GetRangeMask(TKey lowerKey, TKey upperKey, int tableIndex)
    {
        if (!KeyConditions_[tableIndex]) {
            return BoolMask(true, true);
        }
        YT_VERIFY(KeyColumnCount_);

        DB::FieldRef minKey[*KeyColumnCount_];
        DB::FieldRef maxKey[*KeyColumnCount_];

        // TODO(max42): Refactor!
        {
            int index = 0;
            for ( ; index < std::min<int>(lowerKey.GetCount(), *KeyColumnCount_); ++index) {
                if (lowerKey[index].Type != EValueType::Max && lowerKey[index].Type != EValueType::Min && lowerKey[index].Type != EValueType::Null) {
                    minKey[index] = ConvertToField(lowerKey[index]);
                } else {
                    // Fill the rest with nulls.
                    break;
                }
            }
            for (; index < *KeyColumnCount_; ++index) {
                minKey[index] = ConvertToField(MakeUnversionedNullValue());
            }
        }

        bool isMax = upperKey.GetCount() >= 1 && upperKey[0].Type == EValueType::Max;
        {
            if (!isMax) {
                int index = 0;
                for (; index < std::min<int>(upperKey.GetCount(), *KeyColumnCount_); ++index) {
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
                    for (; index < *KeyColumnCount_; ++index) {
                        maxKey[index] = ConvertToField(MakeUnversionedNullValue());
                    }
                }
            }
        }

        // We do not have any Max-equivalent in ClickHouse, but luckily again we have mayBeTrueAfter method which allows
        // us checking key condition on half-open ray.
        if (!isMax) {
            return BoolMask(KeyConditions_[tableIndex]->mayBeTrueInRange(*KeyColumnCount_, minKey, maxKey, KeyColumnDataTypes_), false);
        } else {
            return BoolMask(KeyConditions_[tableIndex]->mayBeTrueAfter(*KeyColumnCount_, minKey, KeyColumnDataTypes_), false);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TInputFetcher);

////////////////////////////////////////////////////////////////////////////////

TQueryInput FetchInput(
    TStorageContext* storageContext,
    const TQueryAnalysisResult& queryAnalysisResult,
    TSubquerySpec& specTemplate,
    const std::vector<std::string>& columnNames)
{
    auto inputFetcher = New<TInputFetcher>(storageContext, queryAnalysisResult, columnNames);
    WaitFor(inputFetcher->Fetch())
        .ThrowOnError();

    YT_VERIFY(!specTemplate.DataSourceDirectory);
    specTemplate.DataSourceDirectory = std::move(inputFetcher->DataSourceDirectory());

    return TQueryInput{
        .StripeList = std::move(inputFetcher->ResultStripeList()),
        .MiscExtMap = std::move(inputFetcher->MiscExtMap())
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
                .OperationId = queryContext->QueryId,
                .RowBuffer = queryContext->RowBuffer,
            },
            TInputStreamDirectory({TInputStreamDescriptor(false /* isTeleportable */, true /* isPrimary */, false /* isVersioned */)}));
    } else if (poolKind == EPoolKind::Sorted) {
        YT_VERIFY(keyColumnCount);
        chunkPool = CreateSortedChunkPool(
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
                size_t tableIndex = stripe->GetTableIndex();
                YT_VERIFY(tableIndex >= 0);
                YT_VERIFY(tableIndex < fullStripeList->Stripes.size());
                YT_VERIFY(!fullStripeList->Stripes[tableIndex]);
                fullStripeList->Stripes[tableIndex] = std::move(stripe);
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
