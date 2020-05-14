#include "subquery.h"

#include "query_context.h"
#include "config.h"
#include "subquery_spec.h"
#include "schema.h"
#include "helpers.h"
#include "table.h"
#include "host.h"
#include "query_analyzer.h"

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

// TODO(max42): rename
class TDataSliceFetcher
    : public TRefCounted
{
public:
    DEFINE_BYREF_RW_PROPERTY(TDataSourceDirectoryPtr, DataSourceDirectory, New<TDataSourceDirectory>());
    DEFINE_BYREF_RW_PROPERTY(TChunkStripeListPtr, ResultStripeList, New<TChunkStripeList>());
    using TMiscExtMap = THashMap<TChunkId, TRefCountedMiscExtPtr>;
    DEFINE_BYREF_RW_PROPERTY(TMiscExtMap, MiscExtMap);

public:
    TDataSliceFetcher(
        TQueryContext* queryContext,
        const TQueryAnalysisResult& queryAnalysisResult)
        : Client_(queryContext->Client())
        , Invoker_(CreateSerializedInvoker(queryContext->Host->GetWorkerInvoker()))
        , TableSchemas_(queryAnalysisResult.TableSchemas)
        , KeyConditions_(queryAnalysisResult.KeyConditions)
        , KeyColumnCount_(queryAnalysisResult.KeyColumnCount)
        , Config_(queryContext->Host->GetConfig()->Subquery)
        , Logger(queryContext->Logger)
    {
        OperandCount_ = queryAnalysisResult.Tables.size();
        for (int operandIndex = 0; operandIndex < static_cast<int>(queryAnalysisResult.Tables.size()); ++operandIndex) {
            for (auto& table : queryAnalysisResult.Tables[operandIndex]) {
                table->TableIndex = operandIndex;
                InputTables_.emplace_back(std::move(table));
            }
        }
    }

    TFuture<void> Fetch()
    {
        return BIND(&TDataSliceFetcher::DoFetch, MakeWeak(this))
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    NApi::NNative::IClientPtr Client_;

    IInvokerPtr Invoker_;

    std::vector<TTableSchema> TableSchemas_;
    std::vector<std::optional<DB::KeyCondition>> KeyConditions_;

    std::optional<int> KeyColumnCount_ = 0;
    DB::DataTypes KeyColumnDataTypes_;

    //! Number of operands to join. May be either 1 (if no JOIN present or joinee is not a YT table) or 2 (otherwise).
    int OperandCount_ = 0;

    std::vector<TTablePtr> InputTables_;

    TSubqueryConfigPtr Config_;

    std::vector<TInputChunkPtr> InputChunks_;

    TLogger Logger;

    void DoFetch()
    {
        // TODO(max42): do we need this check?
        if (!TableSchemas_.empty()) {
            // TODO(max42): it's better for query analyzer to do this substitution...
            if (TableSchemas_.size() == 1) {
                KeyColumnCount_ = TableSchemas_.front().GetKeyColumnCount();
            }
            auto commonSchemaPart = TableSchemas_.front().Columns();
            if (KeyColumnCount_) {
                commonSchemaPart.resize(*KeyColumnCount_);
            }
            KeyColumnDataTypes_ = ToDataTypes(TTableSchema(commonSchemaPart));
        }

        FetchChunks();

        std::vector<TChunkStripePtr> stripes;
        for (int index = 0; index < OperandCount_; ++index) {
            stripes.emplace_back(New<TChunkStripe>());
        }
        for (const auto& chunk : InputChunks_) {
            if (!chunk->BoundaryKeys() ||
                GetRangeMask(chunk->BoundaryKeys()->MinKey, chunk->BoundaryKeys()->MaxKey, chunk->GetTableIndex()).can_be_true)
            {
                stripes[chunk->GetTableIndex()]->DataSlices.emplace_back(CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk)));
            }
        }
        for (auto& stripe : stripes) {
            ResultStripeList_->AddStripe(std::move(stripe));
        }
        YT_LOG_INFO(
            "Input chunks fetched (TotalChunkCount: %v, TotalDataWeight: %v, TotalRowCount: %v)",
            ResultStripeList_->TotalChunkCount,
            ResultStripeList_->TotalDataWeight,
            ResultStripeList_->TotalRowCount);
    }

    void FetchChunks()
    {
        i64 totalChunkCount = 0;
        for (const auto& inputTable : InputTables_) {
            totalChunkCount += inputTable->ChunkCount;
        }
        YT_LOG_INFO("Fetching input chunks (InputTableCount: %v, TotalChunkCount: %v)",
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
                SetTransactionId(req, NullTransactionId);
                SetSuppressAccessTracking(req, true);
            },
            Logger);

        for (const auto& table : InputTables_) {
            auto logicalTableIndex = table->TableIndex;

            auto dataSource = MakeUnversionedDataSource(
                std::nullopt /* path */,
                table->Schema,
                std::nullopt /* columns */,
                // TODO(max42): YT-10402, omitted inaccessible columns
                {});

            DataSourceDirectory_->DataSources().push_back(std::move(dataSource));

            chunkSpecFetcher->Add(
                table->ObjectId,
                table->ExternalCellTag,
                table->ChunkCount,
                logicalTableIndex,
                table->Path.GetRanges());
        }

        WaitFor(chunkSpecFetcher->Fetch())
            .ThrowOnError();

        YT_LOG_INFO("Chunks fetched (ChunkCount: %v)", chunkSpecFetcher->ChunkSpecs().size());

        for (const auto& chunkSpec : chunkSpecFetcher->ChunkSpecs()) {
            auto inputChunk = New<TInputChunk>(chunkSpec);
            auto miscExt = FindProtoExtension<NChunkClient::NProto::TMiscExt>(chunkSpec.chunk_meta().extensions());
            YT_VERIFY(miscExt);
            // Note that misc extension for given chunk may already be present as same chunk may appear several times.
            MiscExtMap_.emplace(inputChunk->ChunkId(), New<TRefCountedMiscExt>(*miscExt));
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

        YT_VERIFY(static_cast<int>(lowerKey.GetCount()) >= *KeyColumnCount_);
        YT_VERIFY(static_cast<int>(upperKey.GetCount()) >= *KeyColumnCount_);

        DB::Field minKey[*KeyColumnCount_];
        DB::Field maxKey[*KeyColumnCount_];
        ConvertToFieldRow(lowerKey, *KeyColumnCount_, minKey);
        ConvertToFieldRow(upperKey, *KeyColumnCount_, maxKey);

        return BoolMask(KeyConditions_[tableIndex]->mayBeTrueInRange(*KeyColumnCount_, minKey, maxKey, KeyColumnDataTypes_), false);
    }
};

DEFINE_REFCOUNTED_TYPE(TDataSliceFetcher);

////////////////////////////////////////////////////////////////////////////////

TQueryInput FetchInput(
    TQueryContext* queryContext,
    const TQueryAnalysisResult& queryAnalysisResult,
    TSubquerySpec& specTemplate)
{
    auto dataSliceFetcher = New<TDataSliceFetcher>(queryContext, queryAnalysisResult);
    WaitFor(dataSliceFetcher->Fetch())
        .ThrowOnError();

    YT_VERIFY(!specTemplate.DataSourceDirectory);
    specTemplate.DataSourceDirectory = std::move(dataSliceFetcher->DataSourceDirectory());

    return TQueryInput{std::move(dataSliceFetcher->ResultStripeList()), std::move(dataSliceFetcher->MiscExtMap())};
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
    const DB::Context& context,
    const TSubqueryConfigPtr& config)
{
    auto* queryContext = GetQueryContext(context);
    const auto& Logger = queryContext->Logger;

    YT_LOG_INFO(
        "Building subqueries (TotalDataWeight: %v, TotalChunkCount: %v, TotalRowCount: %v, "
        "JobCount: %v, PoolKind: %v, SamplingRate: %v)",
        inputStripeList->TotalDataWeight,
        inputStripeList->TotalChunkCount,
        inputStripeList->TotalRowCount,
        jobCount,
        poolKind,
        samplingRate);

    std::vector<TSubquery> subqueries;

    auto dataWeightPerJob = inputStripeList->TotalDataWeight / jobCount;

    if (inputStripeList->TotalRowCount * samplingRate.value_or(1.0) < 1.0) {
        YT_LOG_INFO("Total row count times sampling rate is less than 1, returning empty subqueries");
        return subqueries;
    }

    if (samplingRate) {
        double rate = samplingRate.value();
        auto adjustedRate = std::max(rate, static_cast<double>(jobCount) / config->MaxJobCountForPool);
        auto adjustedJobCount = std::floor(jobCount / rate);
        YT_LOG_INFO("Adjusting job count and sampling rate (OldSamplingRate: %v, AdjustedSamplingRate: %v, OldJobCount: %v, AdjustedJobCount: %v)",
            rate,
            adjustedRate,
            jobCount,
            adjustedJobCount);
        rate = adjustedRate;
        jobCount = adjustedJobCount;
    } else {
        // Try not to form too small ranges when total data weight is small.
        auto maxJobCount = inputStripeList->TotalDataWeight / std::max<i64>(1, config->MinDataWeightPerThread) + 1;
        if (maxJobCount < jobCount) {
            jobCount = maxJobCount;
            dataWeightPerJob = std::max<i64>(1, inputStripeList->TotalDataWeight / maxJobCount);
            YT_LOG_INFO("Query is small and without sampling; forcing new contraints (JobCount: %v, DataWeightPerJob: %v)",
                jobCount,
                dataWeightPerJob);
        }
    }

    std::unique_ptr<IChunkPool> chunkPool;

    // TODO(max42): consider introducing new job size constraints to incapsulate all these heuristics.

    constexpr i64 MinSliceDataWeight = 1_MB;

    auto inputSliceDataWeight = std::max<i64>(1, dataWeightPerJob * 0.1);
    if (inputSliceDataWeight < MinSliceDataWeight) {
        inputSliceDataWeight = dataWeightPerJob;
    }

    auto jobSizeConstraints = CreateExplicitJobSizeConstraints(
        false /* canAdjustDataWeightPerJob */,
        true /* isExplicitJobCount */,
        jobCount,
        dataWeightPerJob,
        1024 * 1024 * 1_TB /* primaryDataWeightPerJob */,
        1'000'000'000'000ll /* maxDataSlicesPerJob */,
        1024 * 1024 * 1_TB /* maxDataWeightPerJob */,
        1024 * 1024 * 1_TB /* primaryMaxDataWeightPerJob */,
        inputSliceDataWeight /* inputSliceDataWeight */,
        std::max<i64>(1, inputStripeList->TotalRowCount / jobCount) /* inputSliceRowCount */,
        std::nullopt /* samplingRate */);

    if (poolKind == EPoolKind::Unordered) {
        chunkPool = CreateUnorderedChunkPool(
            TUnorderedChunkPoolOptions{
                .JobSizeConstraints = std::move(jobSizeConstraints),
                .OperationId = queryContext->QueryId,
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
        for (const auto& dataSlice : chunkStripe->DataSlices) {
            InferLimitsFromBoundaryKeys(dataSlice, queryContext->RowBuffer);
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
        subquery.Cookie = cookie;
        if (poolKind == EPoolKind::Sorted) {
            auto limits = static_cast<ISortedChunkPool*>(chunkPool.get())->GetLimits(cookie);
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
