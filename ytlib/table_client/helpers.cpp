#include "helpers.h"
#include "chunk_meta_extensions.h"
#include "config.h"
#include "schemaless_multi_chunk_reader.h"
#include "schemaless_chunk_writer.h"
#include "private.h"

#include <yt/client/api/table_reader.h>
#include <yt/client/api/table_writer.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/input_chunk.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/formats/parser.h>

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/client/ypath/rich.h>

#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/table_client/unversioned_writer.h>
#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/name_table.h>

#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/periodic_yielder.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/node.h>
#include <yt/core/ytree/permission.h>
#include <yt/core/ytree/attributes.h>

namespace NYT::NTableClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NFormats;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NScheduler::NProto;
using namespace NYTree;
using namespace NYson;
using namespace NTabletClient;

using NChunkClient::NProto::TChunkSpec;

using NYPath::TRichYPath;
using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TApiFromSchemalessChunkReaderAdapter
    : public NApi::ITableReader
{
public:
    explicit TApiFromSchemalessChunkReaderAdapter(ISchemalessChunkReaderPtr underlyingReader)
        : UnderlyingReader_(std::move(underlyingReader))
        , StartRowIndex_(UnderlyingReader_->GetTableRowIndex())
    { }

    virtual i64 GetStartRowIndex() const override
    {
        return StartRowIndex_;
    }

    virtual i64 GetTotalRowCount() const override
    {
        YT_ABORT();
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return UnderlyingReader_->GetReadyEvent();
    }

    virtual bool Read(std::vector<NTableClient::TUnversionedRow>* rows) override
    {
        return UnderlyingReader_->Read(rows);
    }

    virtual const TNameTablePtr& GetNameTable() const override
    {
        return UnderlyingReader_->GetNameTable();
    }

    virtual const TKeyColumns& GetKeyColumns() const override
    {
        return UnderlyingReader_->GetKeyColumns();
    }

    virtual const TTableSchema& GetTableSchema() const override
    {
        YT_ABORT();
    }

    virtual const std::vector<TString>& GetOmittedInaccessibleColumns() const override
    {
        YT_ABORT();
    }

private:
    const ISchemalessChunkReaderPtr UnderlyingReader_;
    const i64 StartRowIndex_;
};

NApi::ITableReaderPtr CreateApiFromSchemalessChunkReaderAdapter(
    ISchemalessChunkReaderPtr underlyingReader)
{
    return New<TApiFromSchemalessChunkReaderAdapter>(std::move(underlyingReader));
}

////////////////////////////////////////////////////////////////////////////////

void PipeReaderToWriter(
    const ISchemalessChunkReaderPtr& reader,
    const IUnversionedRowsetWriterPtr& writer,
    const TPipeReaderToWriterOptions& options)
{
    PipeReaderToWriter(
        CreateApiFromSchemalessChunkReaderAdapter(reader),
        std::move(writer),
        options);
}

////////////////////////////////////////////////////////////////////////////////

// NB: not using TYsonString here to avoid copying.
TUnversionedValue MakeUnversionedValue(TStringBuf ysonString, int id, TStatelessLexer& lexer)
{
    TToken token;
    lexer.GetToken(ysonString, &token);
    YT_VERIFY(!token.IsEmpty());

    switch (token.GetType()) {
        case ETokenType::Int64:
            return MakeUnversionedInt64Value(token.GetInt64Value(), id);

        case ETokenType::Uint64:
            return MakeUnversionedUint64Value(token.GetUint64Value(), id);

        case ETokenType::String:
            return MakeUnversionedStringValue(token.GetStringValue(), id);

        case ETokenType::Double:
            return MakeUnversionedDoubleValue(token.GetDoubleValue(), id);

        case ETokenType::Boolean:
            return MakeUnversionedBooleanValue(token.GetBooleanValue(), id);

        case ETokenType::Hash:
            return MakeUnversionedSentinelValue(EValueType::Null, id);

        default:
            return MakeUnversionedAnyValue(ysonString, id);
    }
}

////////////////////////////////////////////////////////////////////////////////

int GetSystemColumnCount(const TChunkReaderOptionsPtr& options)
{
    int systemColumnCount = 0;
    if (options->EnableRowIndex) {
        ++systemColumnCount;
    }

    if (options->EnableRangeIndex) {
        ++systemColumnCount;
    }

    if (options->EnableTableIndex) {
        ++systemColumnCount;
    }

    if (options->EnableTabletIndex) {
        ++systemColumnCount;
    }

    return systemColumnCount;
}

void ValidateKeyColumnCount(
    int tableKeyColumnCount,
    int chunkKeyColumnCount,
    bool requireUniqueKeys)
{
    if (requireUniqueKeys && chunkKeyColumnCount > tableKeyColumnCount) {
        THROW_ERROR_EXCEPTION(EErrorCode::IncompatibleKeyColumns,
            "Chunk has more key columns than requested: chunk has %v key columns, request has %v key columns",
            chunkKeyColumnCount,
            tableKeyColumnCount);
    }
}

void ValidateKeyColumns(
    const TKeyColumns& tableKeyColumns,
    const TKeyColumns& chunkKeyColumns,
    bool requireUniqueKeys)
{
    ValidateKeyColumnCount(tableKeyColumns.size(), chunkKeyColumns.size(), requireUniqueKeys);

    for (int i = 0; i < std::min(tableKeyColumns.size(), chunkKeyColumns.size()); ++i) {
        if (chunkKeyColumns[i] != tableKeyColumns[i]) {
            THROW_ERROR_EXCEPTION(EErrorCode::IncompatibleKeyColumns,
                "Incompatible key columns: chunk key columns %v, table key columns %v",
                chunkKeyColumns,
                tableKeyColumns);
        }
    }
}

TColumnFilter CreateColumnFilter(
    const std::optional<std::vector<TString>>& columns,
    const TNameTablePtr& nameTable)
{
    if (!columns) {
        return TColumnFilter();
    }

    TColumnFilter::TIndexes columnFilterIndexes;
    for (const auto& column : *columns) {
        auto id = nameTable->GetIdOrRegisterName(column);
        columnFilterIndexes.push_back(id);
    }

    return TColumnFilter(std::move(columnFilterIndexes));
}

////////////////////////////////////////////////////////////////////////////////

TOutputResult GetWrittenChunksBoundaryKeys(ISchemalessMultiChunkWriterPtr writer)
{
    TOutputResult result;

    const auto& chunks = writer->GetWrittenChunksMasterMeta();
    result.set_empty(chunks.empty());

    if (chunks.empty()) {
        return result;
    }

    result.set_sorted(writer->GetSchema().IsSorted());

    if (!writer->GetSchema().IsSorted()) {
        return result;
    }

    result.set_unique_keys(writer->GetSchema().GetUniqueKeys());

    auto frontBoundaryKeys = GetProtoExtension<NProto::TBoundaryKeysExt>(chunks.front().chunk_meta().extensions());
    result.set_min(frontBoundaryKeys.min());
    auto backBoundaryKeys = GetProtoExtension<NProto::TBoundaryKeysExt>(chunks.back().chunk_meta().extensions());
    result.set_max(backBoundaryKeys.max());

    return result;
}

std::pair<TOwningKey, TOwningKey> GetChunkBoundaryKeys(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    int keyColumnCount)
{
    auto boundaryKeysExt = GetProtoExtension<NProto::TBoundaryKeysExt>(chunkMeta.extensions());
    auto minKey = WidenKey(FromProto<TOwningKey>(boundaryKeysExt.min()), keyColumnCount);
    auto maxKey = WidenKey(FromProto<TOwningKey>(boundaryKeysExt.max()), keyColumnCount);
    return std::make_pair(minKey, maxKey);
}

////////////////////////////////////////////////////////////////////////////////

void ValidateDynamicTableTimestamp(
    const TRichYPath& path,
    bool dynamic,
    const TTableSchema& schema,
    const IAttributeDictionary& attributes)
{
    auto nullableRequested = path.GetTimestamp();
    if (nullableRequested && !(dynamic && schema.IsSorted())) {
        THROW_ERROR_EXCEPTION("Invalid attribute %Qv: table %Qv is not sorted dynamic",
            "timestamp",
            path.GetPath());
    }

    auto requested = nullableRequested.value_or(AsyncLastCommittedTimestamp);
    if (requested != AsyncLastCommittedTimestamp) {
        auto retained = attributes.Get<TTimestamp>("retained_timestamp");
        auto unflushed = attributes.Get<TTimestamp>("unflushed_timestamp");
        auto enableDynamicStoreRead = attributes.Get<bool>("enable_dynamic_store_read", false);
        if (requested < retained || (!enableDynamicStoreRead && requested >= unflushed)) {
            THROW_ERROR_EXCEPTION(EErrorCode::TimestampOutOfRange,
                "Requested timestamp is out of range for table %v",
                path.GetPath())
                << TErrorAttribute("requested_timestamp", requested)
                << TErrorAttribute("retained_timestamp", retained)
                << TErrorAttribute("unflushed_timestamp", unflushed)
                << TErrorAttribute("enable_dynamic_store_read", enableDynamicStoreRead);
        }
    }

    if (auto nullableRetention = path.GetRetentionTimestamp()) {
        auto retention = *nullableRetention;
        if (retention >= requested) {
            THROW_ERROR_EXCEPTION("Retention timestamp for table %v should be less than read timestamp",
                path.GetPath())
                << TErrorAttribute("read_timestamp", requested)
                << TErrorAttribute("retention_timestamp", retention);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): unify with input chunk collection procedure in operation_controller_detail.cpp.
std::vector<TInputChunkPtr> CollectTableInputChunks(
    const TRichYPath& path,
    const NNative::IClientPtr& client,
    const TNodeDirectoryPtr& nodeDirectory,
    const TFetchChunkSpecConfigPtr& config,
    TTransactionId transactionId,
    const TLogger& logger)
{
    auto Logger = NLogging::TLogger(logger)
        .AddTag("Path: %v", path.GetPath());

    TUserObject userObject(path);

    GetUserObjectBasicAttributes(
        client,
        {&userObject},
        transactionId,
        Logger,
        EPermission::Read);

    if (userObject.Type != EObjectType::Table) {
        THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
            path.GetPath(),
            EObjectType::Table,
            userObject.Type);
    }

    YT_LOG_INFO("Requesting table chunk count");

    int chunkCount;
    // XXX(babenko): YT-11825
    bool dynamic;
    bool sorted;
    {
        auto channel = client->GetMasterChannelOrThrow(
            EMasterChannelKind::Follower,
            userObject.ExternalCellTag);
        TObjectServiceProxy proxy(channel);

        auto req = TYPathProxy::Get(userObject.GetObjectIdPath() + "/@");
        AddCellTagToSyncWith(req, userObject.ObjectId);
        SetTransactionId(req, userObject.ExternalTransactionId);
        ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
            "chunk_count",
            "dynamic",
            "sorted"
        });

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting chunk count of %v",
            path);

        const auto& rsp = rspOrError.Value();
        auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

        chunkCount = attributes->Get<int>("chunk_count");
        // XXX(babenko): YT-11825
        dynamic = attributes->Get<bool>("dynamic");
        sorted = attributes->Get<bool>("sorted");
    }

    YT_LOG_INFO("Fetching chunk specs (ChunkCount: %v)", chunkCount);

    auto chunkSpecs = FetchChunkSpecs(
        client,
        nodeDirectory,
        userObject,
        path.GetRanges(),
        // XXX(babenko): YT-11825
        dynamic && !sorted ? -1 : chunkCount,
        config->MaxChunksPerFetch,
        config->MaxChunksPerLocateRequest,
        [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& req) {
            req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
            req->set_fetch_all_meta_extensions(false);
            SetTransactionId(req, transactionId);
        },
        Logger);

    std::vector<TInputChunkPtr> inputChunks;
    for (const auto& chunkSpec : chunkSpecs) {
        inputChunks.push_back(New<TInputChunk>(chunkSpec));
    }

    return inputChunks;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

template <typename TValue>
void UpdateColumnarStatistics(NProto::TColumnarStatisticsExt& columnarStatisticsExt, TRange<TValue> values)
{
    for (const auto& value : values) {
        auto id = value.Id;
        if (id >= columnarStatisticsExt.data_weights().size()) {
            columnarStatisticsExt.mutable_data_weights()->Resize(id + 1, 0);
        }
        columnarStatisticsExt.set_data_weights(id, columnarStatisticsExt.data_weights(id) + GetDataWeight(value));
    }
}

} // namespace

void UpdateColumnarStatistics(NProto::TColumnarStatisticsExt& columnarStatisticsExt, TUnversionedRow row)
{
    UpdateColumnarStatistics(columnarStatisticsExt, MakeRange(row.Begin(), row.End()));
}

void UpdateColumnarStatistics(NProto::TColumnarStatisticsExt& columnarStatisticsExt, TVersionedRow row)
{
    UpdateColumnarStatistics(columnarStatisticsExt, MakeRange(row.BeginKeys(), row.EndKeys()));
    UpdateColumnarStatistics(columnarStatisticsExt, MakeRange(row.BeginValues(), row.EndValues()));
    columnarStatisticsExt.set_timestamp_weight(
        columnarStatisticsExt.timestamp_weight() +
        (row.GetWriteTimestampCount() + row.GetDeleteTimestampCount()) * sizeof(TTimestamp));
}

////////////////////////////////////////////////////////////////////////////////

void CheckUnavailableChunks(EUnavailableChunkStrategy strategy, std::vector<TChunkSpec>* chunkSpecs)
{
    std::vector<TChunkSpec> availableChunkSpecs;

    for (auto& chunkSpec : *chunkSpecs) {
        if (!IsUnavailable(chunkSpec)) {
            availableChunkSpecs.push_back(std::move(chunkSpec));
            continue;
        }

        auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
        auto throwUnavailable = [&] {
            THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::ChunkUnavailable,
                "Chunk %v is unavailable",
                chunkId);
        };

        switch (strategy) {
            case EUnavailableChunkStrategy::ThrowError:
                throwUnavailable();
                break;

            case EUnavailableChunkStrategy::Restore:
                if (IsErasureChunkId(chunkId)) {
                    availableChunkSpecs.push_back(std::move(chunkSpec));
                } else {
                    throwUnavailable();
                }
                break;

            case EUnavailableChunkStrategy::Skip:
                // Just skip this chunk.
                break;

            default:
                YT_ABORT();
        };
    }

    *chunkSpecs = std::move(availableChunkSpecs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient::NYT
