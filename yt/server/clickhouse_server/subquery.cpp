#include "subquery.h"

#include "query_context.h"
#include "config.h"

#include "subquery_spec.h"
#include "table_schema.h"
#include "table.h"
#include "helpers.h"

#include "private.h"

#include <yt/server/lib/chunk_pools/chunk_stripe.h>
#include <yt/server/lib/chunk_pools/helpers.h>
#include <yt/server/lib/chunk_pools/unordered_chunk_pool.h>
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

#include <Storages/MergeTree/KeyCondition.h>
#include <DataTypes/IDataType.h>

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
using namespace DB;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

struct TInputTable
    : public TUserObject
{
    int ChunkCount = 0;
    bool IsDynamic = false;
    TTableSchema Schema;
};

TString GetDataSliceStatisticsDebugString(const std::vector<TInputDataSlicePtr>& dataSlices)
{
    i64 dataSliceCount = 0;
    i64 totalRowCount = 0;
    i64 totalDataWeight = 0;
    i64 totalCompressedDataSize = 0;
    for (const auto& dataSlice : dataSlices) {
        ++dataSliceCount;
        totalRowCount += dataSlice->GetRowCount();
        totalDataWeight += dataSlice->GetDataWeight();
        totalCompressedDataSize += dataSlice->GetSingleUnversionedChunkOrThrow()->GetCompressedDataSize();
    }
    return Format("{DataSliceCount: %v, TotalRowCount: %v, TotalDataWeight: %v, TotalCompressedDataSize: %v}",
        dataSliceCount,
        totalRowCount,
        totalDataWeight,
        totalCompressedDataSize);
}

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): rename
class TDataSliceFetcher
    : public TRefCounted
{
public:
    // TODO(max42): use from bootstrap?
    DEFINE_BYREF_RW_PROPERTY(TDataSourceDirectoryPtr, DataSourceDirectory, New<TDataSourceDirectory>());
    DEFINE_BYREF_RW_PROPERTY(TNodeDirectoryPtr, NodeDirectory, New<TNodeDirectory>());
    DEFINE_BYREF_RW_PROPERTY(std::vector<TInputDataSlicePtr>, DataSlices);

public:
    TDataSliceFetcher(
        NNative::IClientPtr client,
        IInvokerPtr invoker,
        std::vector<TRichYPath> inputTablePaths,
        std::optional<KeyCondition> keyCondition,
        TRowBufferPtr rowBuffer,
        TSubqueryConfigPtr config)
        : Client_(std::move(client))
        , Invoker_(std::move(invoker))
        , InputTablePaths_(std::move(inputTablePaths))
        , KeyCondition_(std::move(keyCondition))
        , RowBuffer_(std::move(rowBuffer))
        , Config_(std::move(config))
    {}

    TFuture<void> Fetch()
    {
        return BIND(&TDataSliceFetcher::DoFetch, MakeWeak(this))
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    const NLogging::TLogger& Logger = ServerLogger;

    NApi::NNative::IClientPtr Client_;

    IInvokerPtr Invoker_;

    std::vector<TRichYPath> InputTablePaths_;
    std::optional<KeyCondition> KeyCondition_;

    int KeyColumnCount_ = 0;
    TKeyColumns KeyColumns_;

    DataTypes KeyColumnDataTypes_;

    std::vector<TInputTable> InputTables_;

    std::vector<TInputChunkPtr> PartiallyNeededChunks_;

    TRowBufferPtr RowBuffer_;

    TSubqueryConfigPtr Config_;

    std::vector<TInputChunkPtr> InputChunks_;

    void DoFetch()
    {
        CollectTablesAttributes();
        ValidateSchema();
        FetchChunks();
        if (KeyCondition_) {
            FilterChunks();
            YT_LOG_INFO("Input chunks filtered (FinalDataSliceCount: %v, PartiallyNeededChunkCount: %v)",
                DataSlices_.size(),
                PartiallyNeededChunks_.size());
            if (!PartiallyNeededChunks_.empty() &&
                static_cast<int>(PartiallyNeededChunks_.size()) < Config_->MaxSlicedChunkCount)
            {
                FilterChunkSlices();
            } else {
                for (const auto& chunk : PartiallyNeededChunks_) {
                    DataSlices_.emplace_back(CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk)));
                }
            }
        } else {
            for (const auto& chunk : InputChunks_) {
                DataSlices_.emplace_back(CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk)));
            }
        }
    }

    // TODO(max42): get rid of duplicating code.
    void CollectBasicAttributes()
    {
        YT_LOG_DEBUG("Requesting basic object attributes");

        InputTables_.resize(InputTablePaths_.size());
        for (size_t i = 0; i < InputTablePaths_.size(); ++i) {
            InputTables_[i].Path = InputTablePaths_[i];
            if (InputTables_[i].Path.HasNontrivialRanges()) {
                for (const auto& range : InputTables_[i].Path.GetRanges()) {
                    if (range.LowerLimit().HasKey() || range.UpperLimit().HasKey()) {
                        THROW_ERROR_EXCEPTION("Keys in YPath ranges are not supported yet (CHYT-48)")
                            << TErrorAttribute("path", InputTables_[i].Path);
                    }
                }
            }
        }

        GetUserObjectBasicAttributes(
            Client_,
            MakeUserObjectList(InputTables_),
            NullTransactionId,
            Logger,
            EPermission::Read);

        for (const auto& table : InputTables_) {
            if (table.Type != NObjectClient::EObjectType::Table) {
                THROW_ERROR_EXCEPTION("Object %v has invalid type: expected %Qlv, actual %Qlv",
                    table.Path.GetPath(),
                    NObjectClient::EObjectType::Table,
                    table.Type);
            }
        }
    }

    void CollectTableSpecificAttributes()
    {
        // XXX(babenko): fetch from external cells
        YT_LOG_DEBUG("Requesting extended table attributes");

        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Follower);

        TObjectServiceProxy proxy(channel);
        auto batchReq = proxy.ExecuteBatch();

        for (const auto& table : InputTables_) {
            auto req = TTableYPathProxy::Get(table.GetObjectIdPath() + "/@");
            ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
                "dynamic",
                "chunk_count",
                "schema",
            });
            SetTransactionId(req, NullTransactionId);
            batchReq->AddRequest(req, "get_attributes");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error requesting extended attributes of tables");
        const auto& batchRsp = batchRspOrError.Value();

        auto getInAttributesRspsOrError = batchRsp->GetResponses<TTableYPathProxy::TRspGet>("get_attributes");
        for (size_t index = 0; index < InputTables_.size(); ++index) {
            auto& table = InputTables_[index];

            {
                const auto& rsp = getInAttributesRspsOrError[index].Value();
                auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

                table.ChunkCount = attributes->Get<int>("chunk_count");
                table.IsDynamic = attributes->Get<bool>("dynamic");
                table.Schema = attributes->Get<TTableSchema>("schema");
            }
        }
    }

    void CollectTablesAttributes()
    {
        YT_LOG_INFO("Collecting input tables attributes");

        CollectBasicAttributes();
        CollectTableSpecificAttributes();
    }

    void ValidateSchema()
    {
        if (InputTables_.empty()) {
            return;
        }

        const auto& representativeTable = InputTables_.front();
        for (size_t i = 1; i < InputTables_.size(); ++i) {
            const auto& table = InputTables_[i];
            if (table.IsDynamic != representativeTable.IsDynamic) {
                THROW_ERROR_EXCEPTION(
                    "Table dynamic flag mismatch: %v and %v",
                    representativeTable.Path.GetPath(),
                    table.Path.GetPath());
            }
        }

        KeyColumnCount_ = representativeTable.Schema.GetKeyColumnCount();
        KeyColumns_ = representativeTable.Schema.GetKeyColumns();
        KeyColumnDataTypes_ = TClickHouseTableSchema::From(TClickHouseTable("", representativeTable.Schema)).GetKeyDataTypes();
    }

    void LogStatistics(TStringBuf stage)
    {
        YT_LOG_INFO("Data slice statistics (Stage: %v, Statistics: %v)",
            stage,
            GetDataSliceStatisticsDebugString(DataSlices_));
    }

    void FetchChunks()
    {
        i64 totalChunkCount = 0;
        for (const auto& inputTable : InputTables_) {
            totalChunkCount += inputTable.ChunkCount;
        }
        YT_LOG_INFO("Fetching input chunks (InputTableCount: %v, TotalChunkCount: %v)",
            InputTables_.size(),
            totalChunkCount);

        auto chunkSpecFetcher = New<TChunkSpecFetcher>(
            Client_,
            NodeDirectory_,
            Invoker_,
            Config_->MaxChunksPerFetch,
            Config_->MaxChunksPerLocateRequest,
            [=] (const TChunkOwnerYPathProxy::TReqFetchPtr& req) {
                req->set_fetch_all_meta_extensions(false);
                req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
                req->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value);
                SetTransactionId(req, NullTransactionId);
                SetSuppressAccessTracking(req, true);
            },
            Logger);

        for (size_t tableIndex = 0; tableIndex < InputTables_.size(); ++tableIndex) {
            auto& table = InputTables_[tableIndex];

            if (table.IsDynamic) {
                THROW_ERROR_EXCEPTION("Dynamic tables are not supported yet (YT-9404)")
                    << TErrorAttribute("table", table.Path.GetPath());
            }

            auto dataSource = MakeUnversionedDataSource(
                std::nullopt, // Not used
                table.Schema,
                /* columns */ std::nullopt,
                // TODO(max42): YT-10402, omitted inaccessible columns
                {});

            DataSourceDirectory_->DataSources().push_back(std::move(dataSource));

            chunkSpecFetcher->Add(
                table.ObjectId,
                table.ExternalCellTag,
                table.ChunkCount,
                table.Path.GetRanges());
        }

        WaitFor(chunkSpecFetcher->Fetch())
            .ThrowOnError();

        YT_LOG_INFO("Chunks fetched (ChunkCount: %v)", chunkSpecFetcher->ChunkSpecs().size());

        for (const auto& chunkSpec : chunkSpecFetcher->ChunkSpecs()) {
            auto inputChunk = New<TInputChunk>(chunkSpec);
            InputChunks_.emplace_back(std::move(inputChunk));
        }

        LogStatistics("FetchChunks");
    }

    BoolMask GetRangeMask(TKey lowerKey, TKey upperKey)
    {
        YT_VERIFY(static_cast<int>(lowerKey.GetCount()) >= KeyColumnCount_);
        YT_VERIFY(static_cast<int>(upperKey.GetCount()) >= KeyColumnCount_);

        Field minKey[KeyColumnCount_];
        Field maxKey[KeyColumnCount_];
        ConvertToFieldRow(lowerKey, KeyColumnCount_, minKey);
        ConvertToFieldRow(upperKey, KeyColumnCount_, maxKey);

        return KeyCondition_->getMaskInRange(KeyColumnCount_, minKey, maxKey, KeyColumnDataTypes_);
    }

    void FilterChunks()
    {
        for (const auto& chunk : InputChunks_) {
            auto mask = GetRangeMask(chunk->BoundaryKeys()->MinKey, chunk->BoundaryKeys()->MaxKey);

            if (mask.can_be_true && mask.can_be_false) {
                PartiallyNeededChunks_.emplace_back(chunk);
            } else if (mask.can_be_true && !mask.can_be_false) {
                DataSlices_.emplace_back(CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk)));
            }
        }

        LogStatistics("FilterChunks");
    }

    void FilterChunkSlices()
    {
        auto chunkSliceFetcher = CreateChunkSliceFetcher(
            Config_->ChunkSliceFetcher,
            1 /* chunkSliceSize */,
            KeyColumns_,
            false /* sliceByKeys */,
            NodeDirectory_,
            Invoker_,
            nullptr /* fetcherChunkScraper */,
            Client_,
            RowBuffer_,
            Logger);

        for (const auto& chunk : PartiallyNeededChunks_) {
            chunkSliceFetcher->AddChunk(chunk);
        }

        WaitFor(chunkSliceFetcher->Fetch())
            .ThrowOnError();

        for (const auto& chunkSlice : chunkSliceFetcher->GetChunkSlices()) {
            auto mask = GetRangeMask(chunkSlice->LowerLimit().Key, chunkSlice->UpperLimit().Key);
            if (mask.can_be_true) {
                DataSlices_.emplace_back(CreateUnversionedInputDataSlice(chunkSlice));
            }
        }
        LogStatistics("FilterChunkSlices");
    }
};

DEFINE_REFCOUNTED_TYPE(TDataSliceFetcher);

////////////////////////////////////////////////////////////////////////////////

std::vector<TInputDataSlicePtr> FetchDataSlices(
    NNative::IClientPtr client,
    const IInvokerPtr& invoker,
    std::vector<TRichYPath> inputTablePaths,
    std::optional<KeyCondition> keyCondition,
    TRowBufferPtr rowBuffer,
    TSubqueryConfigPtr config,
    TSubquerySpec& specTemplate)
{
    auto dataSliceFetcher = New<TDataSliceFetcher>(
        std::move(client),
        invoker,
        std::move(inputTablePaths),
        std::move(keyCondition),
        std::move(rowBuffer),
        std::move(config));
    WaitFor(dataSliceFetcher->Fetch())
        .ThrowOnError();

    if (!specTemplate.NodeDirectory) {
        specTemplate.NodeDirectory = New<TNodeDirectory>();
    }
    specTemplate.NodeDirectory->MergeFrom(dataSliceFetcher->NodeDirectory());
    YT_VERIFY(!specTemplate.DataSourceDirectory);
    specTemplate.DataSourceDirectory = std::move(dataSliceFetcher->DataSourceDirectory());

    return std::move(dataSliceFetcher->DataSlices());
}

std::vector<NChunkPools::TChunkStripeListPtr> BuildSubqueries(
    const std::vector<TChunkStripePtr>& chunkStripes,
    int jobCount,
    std::optional<double> samplingRate,
    TQueryId queryId)
{
    if (jobCount == 0) {
        jobCount = 1;
    }

    std::vector<TChunkStripeListPtr> result;

    i64 totalDataWeight = 0;
    i64 totalRowCount = 0;
    for (const auto& chunkStripe : chunkStripes) {
        for (const auto& dataSlice : chunkStripe->DataSlices) {
            totalDataWeight += dataSlice->GetDataWeight();
            totalRowCount += dataSlice->GetRowCount();
        }
    }
    auto dataWeightPerJob = totalDataWeight / jobCount;

    if (totalRowCount * samplingRate.value_or(1.0) < 1.0) {
        return result;
    }

    i64 originalJobCount = jobCount;

    if (samplingRate) {
        constexpr int MaxJobCount = 1'000'000;
        double rate = samplingRate.value();
        rate = std::max(rate, static_cast<double>(jobCount) / MaxJobCount);
        jobCount = std::floor(jobCount / rate);
    }

    std::unique_ptr<IChunkPool> chunkPool;
    chunkPool = CreateUnorderedChunkPool(
        TUnorderedChunkPoolOptions{
            .JobSizeConstraints = CreateExplicitJobSizeConstraints(
                false /* canAdjustDataWeightPerJob */,
                true /* isExplicitJobCount */,
                jobCount,
                0 /* dataWeightPerJob */,
                1_TB /* primaryDataWeightPerJob */,
                dataWeightPerJob,
                10_GB /* maxDataWeightPerJob */,
                10_GB /* primaryMaxDataWeightPerJob */,
                std::max<i64>(1, dataWeightPerJob * 0.26) /* inputSliceDataWeight */,
                std::max<i64>(1, totalRowCount / jobCount) /* inputSliceRowCount */,
                std::nullopt /* samplingRate */),
            .OperationId = queryId
        },
        TInputStreamDirectory({TInputStreamDescriptor(false /* isTeleportable */, true /* isPrimary */, false /* isVersioned */)}));

    for (const auto& chunkStripe : chunkStripes) {
        chunkPool->Add(chunkStripe);
    }
    chunkPool->Finish();
    while (true) {
        auto cookie = chunkPool->Extract();
        if (cookie == IChunkPoolOutput::NullCookie) {
            break;
        }
        // Stripelists from unordered pool consist of lot of stripes; we expect a single
        // stripe with lots of data slices inside, so we flatten them.
        auto stripeList = chunkPool->GetStripeList(cookie);
        auto flattenedStripe = New<TChunkStripe>();
        for (const auto& stripe : stripeList->Stripes) {
            for (const auto& dataSlice : stripe->DataSlices) {
                flattenedStripe->DataSlices.emplace_back(dataSlice);
            }
        }
        auto flattenedStripeList = New<TChunkStripeList>();
        flattenedStripeList->Stripes.emplace_back(std::move(flattenedStripe));
        result.emplace_back(flattenedStripeList);
    }

    if (originalJobCount != jobCount) {
        std::mt19937 gen;
        std::shuffle(result.begin(), result.end(), gen);
        result.resize(std::min<int>(result.size(), originalJobCount));
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
