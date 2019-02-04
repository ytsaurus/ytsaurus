#include "job_input.h"

#include "convert_row.h"
#include "read_job_spec.h"
#include "table_schema.h"
#include "table.h"
#include "helpers.h"

#include "private.h"

#include <yt/server/controller_agent/chunk_pools/chunk_stripe.h>
#include <yt/server/controller_agent/chunk_pools/helpers.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/input_data_slice.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
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
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace DB;

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

class TDataSliceFetcher
{
public:
    // TODO(max42): use from bootstrap?
    DEFINE_BYREF_RW_PROPERTY(TDataSourceDirectoryPtr, DataSourceDirectory, New<TDataSourceDirectory>());
    DEFINE_BYREF_RW_PROPERTY(TNodeDirectoryPtr, NodeDirectory, New<TNodeDirectory>());
    DEFINE_BYREF_RW_PROPERTY(std::vector<TInputDataSlicePtr>, DataSlices);

public:
    TDataSliceFetcher(
        NNative::IClientPtr client,
        std::vector<TRichYPath> inputTablePaths,
        const KeyCondition* keyCondition)
        : Client(std::move(client))
        , InputTablePaths_(std::move(inputTablePaths))
        , KeyCondition_(keyCondition)
    {}

    void Fetch()
    {
        CollectTablesAttributes();
        ValidateSchema();
        FetchDataSlices();
        if (KeyCondition_) {
            FilterDataSlices();
        }
    }

private:
    const NLogging::TLogger& Logger = ServerLogger;

    NApi::NNative::IClientPtr Client;

    std::vector<TRichYPath> InputTablePaths_;
    const KeyCondition* KeyCondition_;

    int KeyColumnCount_ = 0;

    DataTypes KeyColumnDataTypes_;

    std::vector<TInputTable> InputTables_;



    // TODO(max42): get rid of duplicating code.
    void CollectBasicAttributes()
    {
        YT_LOG_DEBUG("Collecting basic object attributes");

        InputTables_.resize(InputTablePaths_.size());
        for (size_t i = 0; i < InputTablePaths_.size(); ++i) {
            InputTables_[i].Path = InputTablePaths_[i];
            if (InputTables_[i].Path.HasNontrivialRanges()) {
                THROW_ERROR_EXCEPTION("Non-trivial ypath ranges are not supported yet (YT-9323)")
                    << TErrorAttribute("path", InputTables_[i].Path);
            }
        }

        GetUserObjectBasicAttributes<TInputTable>(
            Client,
            InputTables_,
            NullTransactionId,
            Logger,
            EPermission::Read);

        for (const auto& table : InputTables_) {
            if (table.Type != EObjectType::Table) {
                THROW_ERROR_EXCEPTION("Object %v has invalid type: expected %Qlv, actual %Qlv",
                    table.GetPath(),
                    EObjectType::Table,
                    table.Type);
            }
        }
    }

    void CollectTableSpecificAttributes()
    {
        YT_LOG_DEBUG("Collecting table specific attributes");

        auto channel = Client->GetMasterChannelOrThrow(EMasterChannelKind::Follower);

        TObjectServiceProxy proxy(channel);
        auto batchReq = proxy.ExecuteBatch();

        for (const auto& table : InputTables_) {
            auto objectIdPath = FromObjectId(table.ObjectId);

            {
                auto req = TTableYPathProxy::Get(objectIdPath + "/@");
                std::vector<TString> attributeKeys{
                    "dynamic",
                    "chunk_count",
                    "schema",
                };
                NYT::ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                SetTransactionId(req, NullTransactionId);
                batchReq->AddRequest(req, "get_attributes");
            }
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error getting attributes of tables");
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
            if (table.Schema != representativeTable.Schema) {
                THROW_ERROR_EXCEPTION(
                    "YT schema mismatch: %Qlv and %Qlv", representativeTable.GetPath(), table.GetPath());
            }
            if (table.IsDynamic != representativeTable.IsDynamic) {
                THROW_ERROR_EXCEPTION(
                    "Table types mismatch: %Qlv and %Qlv", representativeTable.GetPath(), table.GetPath());
            }
        }

        KeyColumnCount_ = representativeTable.Schema.GetKeyColumnCount();
        KeyColumnDataTypes_ = TClickHouseTableSchema::From(*CreateTable("", representativeTable.Schema)).GetKeyDataTypes();
    }

    void LogStatistics(const TStringBuf& stage)
    {
        YT_LOG_INFO("Data slice statistics (Stage: %v, Statistics: %v)",
            stage,
            GetDataSliceStatisticsDebugString(DataSlices_));
    }
    
    void FetchDataSlices()
    {
        i64 totalChunkCount = 0;
        for (const auto& inputTable : InputTables_) {
            totalChunkCount += inputTable.ChunkCount;
        }
        YT_LOG_INFO("Fetching data slices (InputTableCount: %v, TotalChunkCount: %v)", InputTables_.size(), totalChunkCount);

        for (size_t tableIndex = 0; tableIndex < InputTables_.size(); ++tableIndex) {
            auto& table = InputTables_[tableIndex];
    
            if (table.IsDynamic) {
                THROW_ERROR_EXCEPTION("Dynamic tables are not supported yet (YT-9404)")
                    << TErrorAttribute("table", table.GetPath());
            }
    
            auto dataSource = MakeUnversionedDataSource(
                table.GetPath(),
                table.Schema,
                std::nullopt);
    
            DataSourceDirectory_->DataSources().push_back(std::move(dataSource));
    
            YT_LOG_DEBUG("Fetching input table (Path: %v)", table.GetPath());

            std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs;
            chunkSpecs.reserve(table.ChunkCount);
    
            auto objectIdPath = FromObjectId(table.ObjectId);
    
            FetchChunkSpecs(
                Client,
                NodeDirectory_,
                table.CellTag,
                objectIdPath,
                table.Path.GetRanges(),
                table.ChunkCount,
                100000, // MaxChunksPerFetch
                10000,  // MaxChunksPerLocateRequest
                [=] (const TChunkOwnerYPathProxy::TReqFetchPtr& req) {
                    req->set_fetch_all_meta_extensions(false);
                    req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
                    req->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value);
                    SetTransactionId(req, NullTransactionId);
                    SetSuppressAccessTracking(req, true);
                },
                Logger,
                &chunkSpecs);
    
            for (int i = 0; i < static_cast<int>(chunkSpecs.size()); ++i) {
                auto& chunk = chunkSpecs[i];
                chunk.set_table_index(static_cast<int>(tableIndex));
            }
    
            for (const auto& chunkSpec : chunkSpecs) {
                auto inputChunk = New<TInputChunk>(chunkSpec);
                DataSlices_.emplace_back(CreateUnversionedInputDataSlice(CreateInputChunkSlice(inputChunk)));
            }
        }
        
        LogStatistics("FetchDataSlices");
    }

    void FilterDataSlices()
    {
        auto removePredicate = [&] (const TInputDataSlicePtr& inputDataSlice) {
            const auto& chunk = inputDataSlice->GetSingleUnversionedChunkOrThrow();
            YCHECK(chunk->BoundaryKeys()->MinKey.GetCount() == chunk->BoundaryKeys()->MaxKey.GetCount());

            Field minKey[KeyColumnCount_];
            Field maxKey[KeyColumnCount_];
            ConvertToFieldRow(chunk->BoundaryKeys()->MinKey, minKey);
            ConvertToFieldRow(chunk->BoundaryKeys()->MaxKey, maxKey);

            return !KeyCondition_->mayBeTrueInRange(KeyColumnCount_, minKey, maxKey, KeyColumnDataTypes_);
        };

        DataSlices_.erase(std::remove_if(DataSlices_.begin(), DataSlices_.end(), removePredicate), DataSlices_.end());

        LogStatistics("FilterDataSlices");
    }
};

////////////////////////////////////////////////////////////////////////////////

TFetchResult FetchInput(
    NNative::IClientPtr client,
    std::vector<TString> inputTablePaths,
    const KeyCondition* keyCondition)
{
    std::vector<TRichYPath> inputTableRichPaths;
    for (const auto& path : inputTablePaths) {
        inputTableRichPaths.emplace_back(TRichYPath::Parse(path));
    }

    TDataSliceFetcher dataSliceFetcher(
        std::move(client),
        std::move(inputTableRichPaths),
        keyCondition);
    dataSliceFetcher.Fetch();

    return TFetchResult {
        std::move(dataSliceFetcher.DataSlices()),
        std::move(dataSliceFetcher.NodeDirectory()),
        std::move(dataSliceFetcher.DataSourceDirectory())
    };
}

NChunkPools::TChunkStripeListPtr BuildJobs(
    const std::vector<TInputDataSlicePtr>& dataSlices,
    int jobCount)
{
    if (jobCount == 0) {
        jobCount = 1;
    }

    TChunkStripeListPtr result = New<TChunkStripeList>();

    i64 totalDataWeight = 0;
    i64 totalRowCount = 0;
    for (const auto& dataSlice : dataSlices) {
        totalDataWeight += dataSlice->GetDataWeight();
        totalRowCount += dataSlice->GetRowCount();
    }

    int currentDataSliceIndex = 0;
    i64 currentLowerRowIndex = -1;
    i64 remainingStripeDataWeight = 0;

    for (int jobIndex = 0; jobIndex < jobCount; ++jobIndex) {
        auto currentStripe = New<TChunkStripe>();
        remainingStripeDataWeight += totalDataWeight / jobCount;
        if (jobIndex + 1 == jobCount) {
            // Make sure that the last job gets all of the remaining data.
            remainingStripeDataWeight = totalDataWeight + 1;
        }
        while (true) {
            if (currentDataSliceIndex == static_cast<int>(dataSlices.size())) {
                break;
            }
            const auto& dataSlice = dataSlices[currentDataSliceIndex];
            const auto& inputChunk = dataSlice->GetSingleUnversionedChunkOrThrow();
            const auto& lowerLimit = inputChunk->LowerLimit();
            const auto& upperLimit = inputChunk->UpperLimit();
            i64 dataSliceLowerRowIndex = lowerLimit && lowerLimit->HasRowIndex() ? lowerLimit->GetRowIndex() : 0;
            i64 dataSliceUpperRowIndex = upperLimit && upperLimit->HasRowIndex() ? upperLimit->GetRowIndex() : inputChunk->GetRowCount();
            if (currentLowerRowIndex == -1) {
                currentLowerRowIndex = dataSliceLowerRowIndex;
            }

            i64 upperRowIndex = std::min<i64>(
                dataSliceUpperRowIndex,
                currentLowerRowIndex + static_cast<double>(dataSlice->GetRowCount()) / dataSlice->GetDataWeight() * remainingStripeDataWeight);
            // Take at least one row into the job.
            upperRowIndex = std::max(upperRowIndex, currentLowerRowIndex + 1);

            i64 currentDataWeight =
                static_cast<double>(upperRowIndex - currentLowerRowIndex) / dataSlice->GetRowCount() * dataSlice->GetDataWeight();
            bool finalDataSlice = false;
            if (upperRowIndex < dataSlice->GetRowCount() ||
                (upperRowIndex == dataSlice->GetRowCount() && currentDataSliceIndex == static_cast<int>(dataSlices.size())))
            {
                currentDataWeight = remainingStripeDataWeight;
                finalDataSlice = true;
            }

            currentStripe->DataSlices.emplace_back(New<TInputDataSlice>(
                EDataSourceType::UnversionedTable, TInputDataSlice::TChunkSliceList{New<TInputChunkSlice>(
                    dataSlice->GetSingleUnversionedChunkOrThrow(),
                    DefaultPartIndex,
                    currentLowerRowIndex,
                    upperRowIndex,
                    currentDataWeight)}));
            remainingStripeDataWeight -= currentDataWeight;
            currentLowerRowIndex = upperRowIndex;

            if (currentLowerRowIndex == dataSliceUpperRowIndex) {
                ++currentDataSliceIndex;
                currentLowerRowIndex = -1;
            }

            if (finalDataSlice) {
                break;
            }
        }
        auto stat = currentStripe->GetStatistics();
        AddStripeToList(currentStripe, stat.DataWeight, stat.RowCount, result);
    }

    YCHECK(currentDataSliceIndex == static_cast<int>(dataSlices.size()));
    YCHECK(static_cast<int>(result->Stripes.size()) <= jobCount);
    YCHECK(result->GetAggregateStatistics().RowCount == totalRowCount);

    return result;
}

TTablePartList SerializeAsTablePartList(
    const TChunkStripeListPtr& chunkStripeList,
    const TNodeDirectoryPtr& nodeDirectory,
    const TDataSourceDirectoryPtr& dataSourceDirectory)
{
    TTablePartList tableParts;

    for (auto& chunkStripe : chunkStripeList->Stripes) {
        TReadJobSpec readJobSpec;
        {
            readJobSpec.DataSourceDirectory = dataSourceDirectory;
            readJobSpec.NodeDirectory = nodeDirectory;
            YCHECK(!readJobSpec.DataSourceDirectory->DataSources().empty());
            for (const auto& dataSlice : chunkStripe->DataSlices) {
                const auto& chunkSlice = dataSlice->ChunkSlices[0];
                auto chunk = dataSlice->GetSingleUnversionedChunkOrThrow();
                auto& chunkSpec = readJobSpec.DataSliceDescriptors.emplace_back().ChunkSpecs.emplace_back();
                ToProto(&chunkSpec, chunk, EDataSourceType::UnversionedTable);
                // TODO(max42): wtf?
                chunkSpec.set_row_count_override(dataSlice->GetRowCount());
                chunkSpec.set_data_weight_override(dataSlice->GetDataWeight());
                if (chunkSlice->LowerLimit().RowIndex) {
                    chunkSpec.mutable_lower_limit()->set_row_index(*chunkSlice->LowerLimit().RowIndex);
                }
                if (chunkSlice->UpperLimit().RowIndex) {
                    chunkSpec.mutable_upper_limit()->set_row_index(*chunkSlice->UpperLimit().RowIndex);
                }
                NChunkClient::NProto::TMiscExt miscExt;
                miscExt.set_row_count(chunk->GetTotalRowCount());
                miscExt.set_uncompressed_data_size(chunk->GetTotalUncompressedDataSize());
                miscExt.set_data_weight(chunk->GetTotalDataWeight());
                miscExt.set_compressed_data_size(chunk->GetCompressedDataSize());
                SetProtoExtension(chunkSpec.mutable_chunk_meta()->mutable_extensions(), miscExt);
            }
        }

        TTablePart tablePart;
        {
            tablePart.JobSpec = ConvertToYsonString(readJobSpec, EYsonFormat::Text).GetData();

            for (const auto& dataSlice : readJobSpec.DataSliceDescriptors) {
                for (const auto& chunkSpec : dataSlice.ChunkSpecs) {
                    tablePart.RowCount += chunkSpec.row_count_override();
                    tablePart.DataWeight += chunkSpec.data_weight_override();
                }
            }
        }

        tableParts.emplace_back(std::move(tablePart));
    }

    return tableParts;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
