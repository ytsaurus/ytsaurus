#include "partition_tables.h"

#include "convert_row.h"
#include "data_slice.h"
#include "read_job_spec.h"
#include "table_schema.h"
#include "table.h"
#include "helpers.h"

#include "private.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/logging/log.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/ytree/convert.h>

#include <Storages/MergeTree/KeyCondition.h>
#include <DataTypes/IDataType.h>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NChunkClient;
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

namespace {

////////////////////////////////////////////////////////////////////////////////

// TODO: remove copy-paste

struct TTableObject
    : public TUserObject
{
    int ChunkCount = 0;
    bool IsDynamic = false;
    TTableSchema Schema;
};

using TTables = std::vector<TTableObject>;

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TMultiTablesPartitioner
{
public:
    TMultiTablesPartitioner(
        NApi::NNative::IClientPtr client,
        std::vector<TString> tables,
        const KeyCondition* keyCondition,
        size_t numParts)
        : Client(std::move(client))
        , TableNames(std::move(tables))
        , KeyCondition_(keyCondition)
        , NumParts(numParts)
    {}

    // One-shot
    TTablePartList PartitionTables()
    {
        InitializeTables();
        CollectTablesAttributes();
        CheckTables();
        FetchAndFilterChunks();
        return PartitionChunks();
    }

private:
    const NLogging::TLogger& Logger = ServerLogger;

    NApi::NNative::IClientPtr Client;

    std::vector<TString> TableNames;
    const KeyCondition* KeyCondition_;

    // TODO
    TTransactionId TransactionId = NullTransactionId;

    int KeyColumnCount_ = 0;

    DataTypes KeyColumnDataTypes_;

    size_t NumParts;

    // Fields below are filled during execution of PartitionTables method

    TTables Tables;

    TDataSourceDirectoryPtr DataSourceDirectory;
    TNodeDirectoryPtr NodeDirectory;

    TChunkSpecList Chunks;

    void InitializeTables()
    {
        Tables.resize(TableNames.size());
        for (size_t i = 0; i < TableNames.size(); ++i) {
            Tables[i].Path = TRichYPath::Parse(TableNames[i]);
        }
    }

    void CollectBasicAttributes()
    {
        YT_LOG_DEBUG("Collecting basic object attributes");

        GetUserObjectBasicAttributes<TTableObject>(
            Client,
            Tables,
            TransactionId,
            Logger,
            EPermission::Read);

        for (const auto& table : Tables) {
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

        for (const auto& table : Tables) {
            auto objectIdPath = FromObjectId(table.ObjectId);

            {
                auto req = TTableYPathProxy::Get(objectIdPath + "/@");
                std::vector<TString> attributeKeys{
                    "dynamic",
                    "chunk_count",
                    "schema",
                };
                NYT::ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                SetTransactionId(req, TransactionId);
                batchReq->AddRequest(req, "get_attributes");
            }
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error getting attributes of tables");
        const auto& batchRsp = batchRspOrError.Value();

        auto getInAttributesRspsOrError = batchRsp->GetResponses<TTableYPathProxy::TRspGet>("get_attributes");
        for (size_t index = 0; index < Tables.size(); ++index) {
            auto& table = Tables[index];

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

    void CheckTables()
    {
        VerifySchemasAndTypesAreIdentical();
    }

    void VerifySchemasAndTypesAreIdentical()
    {
        if (Tables.empty()) {
            return;
        }

        const auto& representativeTable = Tables.front();
        for (size_t i = 1; i < Tables.size(); ++i) {
            const auto& table = Tables[i];
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

    TChunkSpecList FetchAndFilterTableChunks(const size_t tableIndex)
    {
        auto& table = Tables[tableIndex];

        // add table to data sources

        // TODO
        if (table.IsDynamic) {
            THROW_ERROR_EXCEPTION("Dynamic tables not supported")
                << TErrorAttribute("table", table.GetPath());
        }

        auto dataSource = MakeUnversionedDataSource(
            table.GetPath(),
            table.Schema,
            std::nullopt);

        DataSourceDirectory->DataSources().push_back(std::move(dataSource));


        YT_LOG_DEBUG("Fetching %Qlv chunks", table.GetPath());

        TChunkSpecList chunkSpecs;
        chunkSpecs.reserve(table.ChunkCount);

        auto objectIdPath = FromObjectId(table.ObjectId);

        FetchChunkSpecs(
            Client,
            NodeDirectory,
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
                SetTransactionId(req, TransactionId);
                SetSuppressAccessTracking(req, true);
            },
            Logger,
            &chunkSpecs);

        if (KeyCondition_) {
            FilterChunks(chunkSpecs);
        }

        YT_LOG_INFO("Selected %v/%v chunks for table %Qlv", chunkSpecs.size(), table.ChunkCount, table.Path);

        for (int i = 0; i < static_cast<int>(chunkSpecs.size()); ++i) {
            auto& chunk = chunkSpecs[i];
            chunk.set_table_index(static_cast<int>(tableIndex));
        }

        return chunkSpecs;
    }

    void FilterChunks(TChunkSpecList& chunkSpecs)
    {
        auto predicate = [&] (const NChunkClient::NProto::TChunkSpec& chunkSpec) {
            TOwningBoundaryKeys keys;
            if (FindBoundaryKeys(chunkSpec.chunk_meta(), &keys.MinKey, &keys.MaxKey)) {
                Field minKey[KeyColumnCount_];
                Field maxKey[KeyColumnCount_];
                ConvertToFieldRow(keys.MinKey, minKey);
                ConvertToFieldRow(keys.MaxKey, maxKey);

                YCHECK(keys.MinKey.GetCount() == keys.MaxKey.GetCount());
                if (!KeyCondition_->mayBeTrueInRange(KeyColumnCount_, minKey, maxKey, KeyColumnDataTypes_)) {
                    // could safely skip this chunk
                    return true;
                }
            }

            // will read this chunk
            return false;
        };

        EraseIf(chunkSpecs, predicate);
    }

    void FetchAndFilterChunks()
    {
        YT_LOG_DEBUG("Fetching chunks");

        DataSourceDirectory = New<TDataSourceDirectory>();
        NodeDirectory = New<TNodeDirectory>();

        for (size_t tableIndex = 0; tableIndex < Tables.size(); ++tableIndex) {
            auto tableChunks = FetchAndFilterTableChunks(tableIndex);
            Chunks.insert(Chunks.end(), tableChunks.begin(), tableChunks.end());
        }
    }

    TTablePartList PartitionChunks()
    {
        auto dataSlices = SplitUnversionedChunks(
            std::move(Chunks),
            NumParts);

        TTablePartList tableParts;

        for (auto& dataSliceDescriptors: dataSlices) {
            TReadJobSpec readJobSpec;
            {
                readJobSpec.DataSourceDirectory = DataSourceDirectory;
                readJobSpec.DataSliceDescriptors = std::move(dataSliceDescriptors);
                readJobSpec.NodeDirectory = NodeDirectory;
            }

            TTablePart tablePart;
            {
                tablePart.JobSpec = ConvertToYsonString(readJobSpec, EYsonFormat::Text).GetData();

                for (const auto& dataSlice: readJobSpec.DataSliceDescriptors) {
                    for (const auto& chunkSpec: dataSlice.ChunkSpecs) {
                        tablePart.RowCount += chunkSpec.row_count_override();
                        tablePart.DataWeight += chunkSpec.data_weight_override();
                    }
                }
            }

            tableParts.emplace_back(std::move(tablePart));
        }

        return tableParts;
    }
};

////////////////////////////////////////////////////////////////////////////////

TTablePartList PartitionTables(
    NApi::NNative::IClientPtr client,
    std::vector<TString> tables,
    const KeyCondition* keyCondition,
    size_t numParts)
{
    TMultiTablesPartitioner partitioner(
        std::move(client),
        std::move(tables),
        keyCondition,
        numParts);

    return partitioner.PartitionTables();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
