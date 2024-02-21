#include "helpers.h"
#include "config.h"
#include "private.h"
#include "schemaless_chunk_writer.h"
#include "schemaless_multi_chunk_reader.h"
#include "table_ypath_proxy.h"
#include "hunks.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/ytlib/object_client/helpers.h>

#include <yt/yt/ytlib/table_chunk_format/column_reader_detail.h>

#include <yt/yt/client/api/table_reader.h>
#include <yt/yt/client/api/table_writer.h>

#include <yt/yt/client/formats/parser.h>

#include <yt/yt/client/misc/io_tags.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_writer.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/core/concurrency/async_stream.h>
#include <yt/yt/core/concurrency/periodic_yielder.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/numeric_helpers.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/random.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/permission.h>

namespace NYT::NTableClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NFormats;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NControllerAgent::NProto;
using namespace NTabletClient;
using namespace NTableChunkFormat;
using namespace NYTree;
using namespace NYson;

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

    i64 GetStartRowIndex() const override
    {
        return StartRowIndex_;
    }

    i64 GetTotalRowCount() const override
    {
        YT_ABORT();
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
    }

    TFuture<void> GetReadyEvent() override
    {
        return UnderlyingReader_->GetReadyEvent();
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        return UnderlyingReader_->Read(options);
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return UnderlyingReader_->GetNameTable();
    }

    const TTableSchemaPtr& GetTableSchema() const override
    {
        YT_ABORT();
    }

    const std::vector<TString>& GetOmittedInaccessibleColumns() const override
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
        writer,
        options);
}

void PipeReaderToWriterByBatches(
    const ISchemalessChunkReaderPtr& reader,
    const NFormats::ISchemalessFormatWriterPtr& writer,
    const TRowBatchReadOptions& options,
    TDuration pipeDelay)
{
    PipeReaderToWriterByBatches(
        CreateApiFromSchemalessChunkReaderAdapter(reader),
        writer,
        options,
        pipeDelay);
}

////////////////////////////////////////////////////////////////////////////////

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

void ValidateSortColumns(
    const TSortColumns& tableSortColumns,
    const TSortColumns& chunkSortColumns,
    bool requireUniqueKeys)
{
    ValidateKeyColumnCount(tableSortColumns.size(), chunkSortColumns.size(), requireUniqueKeys);

    for (int i = 0; i < std::min(std::ssize(tableSortColumns), std::ssize(chunkSortColumns)); ++i) {
        if (chunkSortColumns[i] != tableSortColumns[i]) {
            THROW_ERROR_EXCEPTION(EErrorCode::IncompatibleKeyColumns,
                "Incompatible sort columns: chunk sort columns %v, table sort columns %v",
                chunkSortColumns,
                tableSortColumns);
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

TOutputResult GetWrittenChunksBoundaryKeys(const ISchemalessMultiChunkWriterPtr& writer, bool withChunkSpecs)
{
    TOutputResult result;

    const auto& chunks = writer->GetWrittenChunkSpecs();
    result.set_empty(chunks.empty());

    if (chunks.empty()) {
        return result;
    }

    result.set_sorted(writer->GetSchema()->IsSorted());

    if (!writer->GetSchema()->IsSorted()) {
        return result;
    }

    result.set_unique_keys(writer->GetSchema()->GetUniqueKeys());

    auto frontBoundaryKeys = GetProtoExtension<NProto::TBoundaryKeysExt>(chunks.front().chunk_meta().extensions());
    result.set_min(frontBoundaryKeys.min());

    auto backBoundaryKeys = GetProtoExtension<NProto::TBoundaryKeysExt>(chunks.back().chunk_meta().extensions());
    result.set_max(backBoundaryKeys.max());

    if (withChunkSpecs) {
        std::vector<TChunkSpec> lightweightChunks;

        for (const auto& chunkSpec : chunks) {
            auto& lightweightChunkSpec = lightweightChunks.emplace_back();

            lightweightChunkSpec.CopyFrom(chunkSpec);

            FilterProtoExtensions(
                lightweightChunkSpec.mutable_chunk_meta()->mutable_extensions(),
                {TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value});
        }

        ToProto(result.mutable_chunk_specs(), lightweightChunks);
    }

    return result;
}

std::pair<TLegacyOwningKey, TLegacyOwningKey> GetChunkBoundaryKeys(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    int keyColumnCount)
{
    return GetChunkBoundaryKeys(
        GetProtoExtension<NProto::TBoundaryKeysExt>(chunkMeta.extensions()),
        keyColumnCount);
}

std::pair<TLegacyOwningKey, TLegacyOwningKey> GetChunkBoundaryKeys(
    const NTableClient::NProto::TBoundaryKeysExt& boundaryKeysExt,
    int keyColumnCount)
{
    auto minKey = WidenKey(FromProto<TLegacyOwningKey>(boundaryKeysExt.min()), keyColumnCount);
    auto maxKey = WidenKey(FromProto<TLegacyOwningKey>(boundaryKeysExt.max()), keyColumnCount);
    return {std::move(minKey), std::move(maxKey)};
}

////////////////////////////////////////////////////////////////////////////////

void ValidateDynamicTableTimestamp(
    const TRichYPath& path,
    bool dynamic,
    const TTableSchema& schema,
    const IAttributeDictionary& attributes,
    bool forceDisableDynamicStoreRead)
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
        auto enableDynamicStoreRead =
            attributes.Get<bool>("enable_dynamic_store_read", false) &&
            !forceDisableDynamicStoreRead;
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
        if (retention > requested) {
            THROW_ERROR_EXCEPTION("Retention timestamp for table %v should not be greater than read timestamp",
                path.GetPath())
                << TErrorAttribute("read_timestamp", requested)
                << TErrorAttribute("retention_timestamp", retention);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): unify with input chunk collection procedure in operation_controller_detail.cpp.
std::tuple<std::vector<NChunkClient::TInputChunkPtr>, TTableSchemaPtr, bool> CollectTableInputChunks(
    const TRichYPath& path,
    const NNative::IClientPtr& client,
    const TNodeDirectoryPtr& nodeDirectory,
    const TFetchChunkSpecConfigPtr& config,
    TTransactionId transactionId,
    std::vector<i32> extensionTags,
    const TLogger& logger)
{
    auto Logger = logger.WithTag("Path: %v", path.GetPath());

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
    TTableSchemaPtr schema;
    {
        auto proxy = CreateObjectServiceReadProxy(
            client,
            EMasterChannelKind::Follower,
            userObject.ExternalCellTag);

        auto req = TYPathProxy::Get(userObject.GetObjectIdPath() + "/@");
        AddCellTagToSyncWith(req, userObject.ObjectId);
        SetTransactionId(req, userObject.ExternalTransactionId);
        ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
            "chunk_count",
            "dynamic",
            "schema"
        });

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting chunk count of %v",
            path);

        const auto& rsp = rspOrError.Value();
        auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

        chunkCount = attributes->Get<int>("chunk_count");
        // XXX(babenko): YT-11825
        dynamic = attributes->Get<bool>("dynamic");
        schema = attributes->Get<TTableSchemaPtr>("schema");
    }

    YT_LOG_INFO("Fetching chunk specs (ChunkCount: %v)", chunkCount);

    auto chunkSpecs = FetchChunkSpecs(
        client,
        nodeDirectory,
        userObject,
        path.GetNewRanges(schema->ToComparator(), schema->GetKeyColumnTypes()),
        // XXX(babenko): YT-11825
        dynamic && !schema->IsSorted() ? -1 : chunkCount,
        config->MaxChunksPerFetch,
        config->MaxChunksPerLocateRequest,
        [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& req) {
            req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
            for (auto extensionTag : extensionTags) {
                req->add_extension_tags(extensionTag);
            }
            req->set_fetch_all_meta_extensions(false);
            SetTransactionId(req, userObject.ExternalTransactionId);
        },
        Logger);

    std::vector<TInputChunkPtr> inputChunks;
    for (const auto& chunkSpec : chunkSpecs) {
        inputChunks.push_back(New<TInputChunk>(chunkSpec, schema->GetKeyColumnCount()));
    }

    return {std::move(inputChunks), std::move(schema), dynamic};
}

////////////////////////////////////////////////////////////////////////////////

void CheckUnavailableChunks(
    EUnavailableChunkStrategy strategy,
    EChunkAvailabilityPolicy policy,
    std::vector<TChunkSpec>* chunkSpecs)
{
    std::vector<TChunkSpec> availableChunkSpecs;

    if (strategy == EUnavailableChunkStrategy::ThrowError) {
        policy = EChunkAvailabilityPolicy::DataPartsAvailable;
    }

    for (auto& chunkSpec : *chunkSpecs) {
        if (!IsUnavailable(chunkSpec, policy)) {
            availableChunkSpecs.push_back(std::move(chunkSpec));
            continue;
        }

        switch (strategy) {
            case EUnavailableChunkStrategy::ThrowError:
            case EUnavailableChunkStrategy::Restore: {
                auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
                THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::ChunkUnavailable,
                    "Chunk %v is unavailable",
                    chunkId);
                break;
            }

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

ui32 GetHeavyColumnStatisticsHash(ui32 salt, const TColumnStableName& stableName)
{
    size_t hash = 0;
    HashCombine(hash, salt);
    HashCombine(hash, stableName.Underlying());

    return static_cast<ui32>(hash ^ (hash >> 32));
}

TColumnarStatistics GetColumnarStatistics(
    const NProto::THeavyColumnStatisticsExt& statistics,
    const std::vector<TColumnStableName>& columnNames,
    i64 chunkRowCount)
{
    YT_VERIFY(statistics.version() == 1);

    auto salt = statistics.salt();

    THashMap<ui32, i64> columnNameHashToDataWeight;
    i64 minHeavyColumnWeight = std::numeric_limits<i64>::max();

    for (int columnIndex = 0; columnIndex < statistics.column_name_hashes_size(); ++columnIndex) {
        auto columnDataWeight = statistics.data_weight_unit() * static_cast<ui8>(statistics.column_data_weights()[columnIndex]);
        auto columnNameHash = statistics.column_name_hashes(columnIndex);
        minHeavyColumnWeight = std::min<i64>(minHeavyColumnWeight, columnDataWeight);
        columnNameHashToDataWeight[columnNameHash] = std::max<i64>(columnNameHashToDataWeight[columnNameHash], columnDataWeight);
    }

    TColumnarStatistics columnarStatistics;
    columnarStatistics.ColumnDataWeights.reserve(columnNames.size());
    for (const auto& columnName : columnNames) {
        auto columnNameHash = GetHeavyColumnStatisticsHash(salt, columnName);
        auto it = columnNameHashToDataWeight.find(columnNameHash);
        if (it == columnNameHashToDataWeight.end()) {
            columnarStatistics.ColumnDataWeights.push_back(minHeavyColumnWeight);
        } else {
            columnarStatistics.ColumnDataWeights.push_back(it->second);
        }
    }
    columnarStatistics.ChunkRowCount = chunkRowCount;

    return columnarStatistics;
}

////////////////////////////////////////////////////////////////////////////////

const ui64 TReaderVirtualValues::Zero_ = 0;

void TReaderVirtualValues::AddValue(TUnversionedValue value, TLogicalTypePtr logicalType)
{
    Values_.push_back(value);
    LogicalTypes_.emplace_back(std::move(logicalType));
}

int TReaderVirtualValues::GetBatchColumnCount(int /*virtualColumnIndex*/) const
{
    return 2;
}

int TReaderVirtualValues::GetTotalColumnCount() const
{
    return 2 * Values_.size();
}

void TReaderVirtualValues::FillRleColumn(IUnversionedColumnarRowBatch::TColumn* rleColumn, int virtualColumnIndex) const
{
    const auto& value = Values_[virtualColumnIndex];
    const auto& logicalType = LogicalTypes_[virtualColumnIndex];
    rleColumn->Id = value.Id;
    if (IsIntegralType(value.Type)) {
        ReadColumnarIntegerValues(
            rleColumn,
            /*startIndex*/ 0,
            /*valueCount*/ 1,
            value.Type,
            /*baseValue*/ 0,
            MakeRange(&value.Data.Uint64, 1));
        rleColumn->Values->ZigZagEncoded = false;
    } else if (IsStringLikeType(value.Type)) {
        ReadColumnarStringValues(
            rleColumn,
            /*startIndex*/ 0,
            /*valueCount*/ 1,
            value.Length,
            MakeRange<ui32>(reinterpret_cast<const ui32*>(&Zero_), 1),
            TRef(value.Data.String, value.Length));
    } else if (value.Type == EValueType::Double) {
        ReadColumnarFloatingPointValues(
            rleColumn,
            /*startIndex*/ 0,
            /*valueCount*/ 1,
            MakeRange(&value.Data.Double, 1));
    } else if (value.Type == EValueType::Boolean) {
        ReadColumnarBooleanValues(
            rleColumn,
            /*startIndex*/ 0,
            /*valueCount*/ 1,
            TRef(&value.Data.Boolean, 1));
    } else if (value.Type == EValueType::Null) {
        rleColumn->StartIndex = 0;
        rleColumn->ValueCount = 1;
    } else {
        Y_UNREACHABLE();
    }

    if (logicalType->IsNullable() && value.Type != EValueType::Null) {
        rleColumn->NullBitmap.emplace().Data = TRef(&Zero_, sizeof(ui64));
    }

    rleColumn->Type = std::move(logicalType);
}

void TReaderVirtualValues::FillMainColumn(
    IUnversionedColumnarRowBatch::TColumn* mainColumn,
    const IUnversionedColumnarRowBatch::TColumn* rleColumn,
    int virtualColumnIndex,
    ui64 startIndex,
    ui64 valueCount) const
{
    mainColumn->StartIndex = startIndex;
    mainColumn->ValueCount = valueCount;
    mainColumn->Id = Values_[virtualColumnIndex].Id;
    mainColumn->Type = rleColumn->Type;
    mainColumn->Rle.emplace().ValueColumn = rleColumn;
    auto& rleIndexes = mainColumn->Values.emplace();
    rleIndexes.BitWidth = 64;
    rleIndexes.Data = TRef(&Zero_, sizeof(ui64));
}

void TReaderVirtualValues::FillColumns(
    TMutableRange<IUnversionedColumnarRowBatch::TColumn> columnRange,
    int virtualColumnIndex,
    ui64 startIndex,
    ui64 valueCount) const
{
    YT_VERIFY(std::ssize(columnRange) == GetBatchColumnCount(virtualColumnIndex));
    FillRleColumn(&columnRange[1], virtualColumnIndex);
    FillMainColumn(&columnRange[0], &columnRange[1], virtualColumnIndex, startIndex, valueCount);
}

////////////////////////////////////////////////////////////////////////////////

NProto::THeavyColumnStatisticsExt GetHeavyColumnStatisticsExt(
    const TColumnarStatistics& columnarStatistics,
    const std::function<TColumnStableName(int index)>& geTColumnStableNameByIndex,
    int columnCount,
    int maxHeavyColumns)
{
    YT_VERIFY(columnCount == columnarStatistics.GetColumnCount());

    // Column weights here are measured in units, which are equal to 1/255 of the weight
    // of heaviest column.
    constexpr int DataWeightGranularity = 256;

    auto salt = RandomNumber<ui32>();

    struct TColumnStatistics
    {
        i64 DataWeight;
        TColumnStableName StableName;
    };
    std::vector<TColumnStatistics> columnStatistics;
    columnStatistics.reserve(columnCount);

    auto heavyColumnCount = std::min<int>(columnCount, maxHeavyColumns);
    i64 maxColumnDataWeight = 0;

    for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
        auto dataWeight = columnarStatistics.ColumnDataWeights[columnIndex];
        maxColumnDataWeight = std::max<i64>(maxColumnDataWeight, dataWeight);
        columnStatistics.push_back(TColumnStatistics{
            .DataWeight = dataWeight,
            .StableName = geTColumnStableNameByIndex(columnIndex),
        });
    }

    auto dataWeightUnit = std::max<i64>(1, DivCeil<i64>(maxColumnDataWeight, DataWeightGranularity - 1));

    if (heavyColumnCount > 0) {
        std::nth_element(
            columnStatistics.begin(),
            columnStatistics.begin() + heavyColumnCount - 1,
            columnStatistics.end(),
            [] (const TColumnStatistics& lhs, const TColumnStatistics& rhs) {
                return lhs.DataWeight > rhs.DataWeight;
            });
    }

    NProto::THeavyColumnStatisticsExt heavyColumnStatistics;
    heavyColumnStatistics.set_version(1);
    heavyColumnStatistics.set_salt(salt);

    TBuffer dataWeightBuffer(heavyColumnCount);
    heavyColumnStatistics.set_data_weight_unit(dataWeightUnit);
    for (int columnIndex = 0; columnIndex < heavyColumnCount; ++columnIndex) {
        heavyColumnStatistics.add_column_name_hashes(GetHeavyColumnStatisticsHash(salt, columnStatistics[columnIndex].StableName));

        auto dataWeight = DivCeil<i64>(columnStatistics[columnIndex].DataWeight, dataWeightUnit);
        YT_VERIFY(dataWeight >= 0 && dataWeight < DataWeightGranularity);
        dataWeightBuffer.Append(static_cast<ui8>(dataWeight));

        // All other columns have weight 0.
        if (dataWeight == 0) {
            break;
        }
    }
    heavyColumnStatistics.set_column_data_weights(dataWeightBuffer.data(), dataWeightBuffer.size());

    return heavyColumnStatistics;
}

////////////////////////////////////////////////////////////////////////////////

TExtraChunkTags MakeExtraChunkTags(const NChunkClient::NProto::TMiscExt& miscExt)
{
    return TExtraChunkTags{
        .CompressionCodec = NCompression::ECodec(miscExt.compression_codec()),
        .ErasureCodec = NErasure::ECodec(miscExt.erasure_codec()),
    };
}

void AddTagsFromDataSource(const NYTree::IAttributeDictionaryPtr& baggage, const NChunkClient::TDataSource& dataSource)
{
    if (dataSource.GetPath()) {
        AddTagToBaggage(baggage, ERawIOTag::ObjectPath, *dataSource.GetPath());
    }
    if (dataSource.GetObjectId()) {
        AddTagToBaggage(baggage, ERawIOTag::ObjectId, ToString(dataSource.GetObjectId()));
    }
    if (dataSource.GetAccount()) {
        AddTagToBaggage(baggage, EAggregateIOTag::Account, *dataSource.GetAccount());
    }
}

void AddTagsFromDataSink(const NYTree::IAttributeDictionaryPtr& baggage, const NChunkClient::TDataSink& dataSink)
{
    if (dataSink.GetPath()) {
        AddTagToBaggage(baggage, ERawIOTag::ObjectPath, *dataSink.GetPath());
    }
    if (dataSink.GetObjectId()) {
        AddTagToBaggage(baggage, ERawIOTag::ObjectId, ToString(dataSink.GetObjectId()));
    }
    if (dataSink.GetAccount()) {
        AddTagToBaggage(baggage, EAggregateIOTag::Account, *dataSink.GetAccount());
    }
}

void AddExtraChunkTags(const NYTree::IAttributeDictionaryPtr& baggage, const TExtraChunkTags& extraTags)
{
    if (extraTags.CompressionCodec) {
        AddTagToBaggage(baggage, EAggregateIOTag::CompressionCodec, FormatEnum(*extraTags.CompressionCodec));
    }
    if (extraTags.ErasureCodec) {
        AddTagToBaggage(baggage, EAggregateIOTag::ErasureCodec, FormatEnum(*extraTags.ErasureCodec));
    }
}

void PackBaggageFromDataSource(const NTracing::TTraceContextPtr& context, const NChunkClient::TDataSource& dataSource)
{
    auto baggage = context->UnpackOrCreateBaggage();
    AddTagsFromDataSource(baggage, dataSource);
    context->PackBaggage(baggage);
}

void PackBaggageFromExtraChunkTags(const NTracing::TTraceContextPtr& context, const TExtraChunkTags& extraTags)
{
    auto baggage = context->UnpackOrCreateBaggage();
    AddExtraChunkTags(baggage, extraTags);
    context->PackBaggage(baggage);
}

void PackBaggageForChunkReader(
    const NTracing::TTraceContextPtr& context,
    const NChunkClient::TDataSource& dataSource,
    const TExtraChunkTags& extraTags)
{
    auto baggage = context->UnpackOrCreateBaggage();
    AddTagsFromDataSource(baggage, dataSource);
    AddExtraChunkTags(baggage, extraTags);
    context->PackBaggage(baggage);
}

void PackBaggageForChunkWriter(
    const NTracing::TTraceContextPtr& context,
    const NChunkClient::TDataSink& dataSink,
    const TExtraChunkTags& extraTags)
{
    auto baggage = context->UnpackOrCreateBaggage();
    AddTagsFromDataSink(baggage, dataSink);
    AddExtraChunkTags(baggage, extraTags);
    context->PackBaggage(baggage);
}

////////////////////////////////////////////////////////////////////////////////

IAttributeDictionaryPtr ResolveExternalTable(
    const NNative::IClientPtr& client,
    const TYPath& path,
    TTableId* tableId,
    TCellTag* externalCellTag,
    const std::vector<TString>& extraAttributeKeys)
{
    TMasterReadOptions options;
    auto proxy = std::make_unique<TObjectServiceProxy>(CreateObjectServiceReadProxy(
        client,
        options.ReadFrom,
        PrimaryMasterCellTagSentinel,
        client->GetNativeConnection()->GetStickyGroupSizeCache()));

    {
        auto req = TObjectYPathProxy::GetBasicAttributes(path);
        auto rspOrError = WaitFor(proxy->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting basic attributes of table %v", path);
        const auto& rsp = rspOrError.Value();
        *tableId = FromProto<TTableId>(rsp->object_id());
        *externalCellTag = FromProto<TCellTag>(rsp->external_cell_tag());
    }

    if (!IsTabletOwnerType(TypeFromId(*tableId))) {
        THROW_ERROR_EXCEPTION("%v is not a tablet owner", path);
    }

    IAttributeDictionaryPtr extraAttributes;
    {
        auto req = TTableYPathProxy::Get(FromObjectId(*tableId) + "/@");
        ToProto(req->mutable_attributes()->mutable_keys(), extraAttributeKeys);
        auto rspOrError = WaitFor(proxy->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting extended attributes of table %v", path);
        const auto& rsp = rspOrError.Value();
        extraAttributes = ConvertToAttributes(TYsonString(rsp->value()));
    }

    return extraAttributes;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TColumnarStatisticsExt* protoStatisticsExt,
    const TColumnarStatistics& statistics)
{
    protoStatisticsExt->Clear();

    ToProto(protoStatisticsExt->mutable_column_data_weights(), statistics.ColumnDataWeights);
    if (statistics.TimestampTotalWeight) {
        protoStatisticsExt->set_timestamp_total_weight(*statistics.TimestampTotalWeight);
    }
    YT_VERIFY(statistics.LegacyChunkDataWeight == 0);

    ToProto(protoStatisticsExt->mutable_column_min_values(), statistics.ColumnMinValues);
    ToProto(protoStatisticsExt->mutable_column_max_values(), statistics.ColumnMaxValues);
    ToProto(protoStatisticsExt->mutable_column_non_null_value_counts(), statistics.ColumnNonNullValueCounts);

    if (statistics.ChunkRowCount) {
        protoStatisticsExt->set_chunk_row_count(*statistics.ChunkRowCount);
    }
    YT_VERIFY(statistics.LegacyChunkRowCount == 0);
}

void FromProto(
    TColumnarStatistics* statistics,
    const NProto::TColumnarStatisticsExt& protoStatisticsExt,
    i64 chunkRowCount)
{
    FromProto(&statistics->ColumnDataWeights, protoStatisticsExt.column_data_weights());
    if (protoStatisticsExt.has_timestamp_total_weight()) {
        statistics->TimestampTotalWeight = protoStatisticsExt.timestamp_total_weight();
    } else {
        statistics->TimestampTotalWeight.reset();
    }
    statistics->LegacyChunkDataWeight = 0;

    FromProto(&statistics->ColumnMinValues, protoStatisticsExt.column_min_values());
    FromProto(&statistics->ColumnMaxValues, protoStatisticsExt.column_max_values());
    FromProto(&statistics->ColumnNonNullValueCounts, protoStatisticsExt.column_non_null_value_counts());

    // COMPAT(dakovalkov): Value statistics in chunks written by 23.2+ may be incorrectly merged
    // by 23.1 version. Ignore value statistics from such chunks, because it doesn't make sense.
    if (!protoStatisticsExt.has_chunk_row_count() || protoStatisticsExt.chunk_row_count() != chunkRowCount) {
        statistics->ClearValueStatistics();
    }
    statistics->ChunkRowCount = chunkRowCount;
    statistics->LegacyChunkRowCount = 0;
}

////////////////////////////////////////////////////////////////////////////////

void EnsureAnyValueEncoded(
    TUnversionedValue* value,
    const TTableSchema& schema,
    TChunkedMemoryPool* memoryPool,
    bool ignoreRequired)
{
    auto id = value->Id;
    auto expectedType = schema.Columns()[id].GetWireType();
    auto actualType = value->Type;

    if (actualType == EValueType::Null) {
        YT_VERIFY(None(value->Flags & EValueFlags::Hunk));
        return;
    }

    if (Any(value->Flags & EValueFlags::Hunk)) {
        if (!TryDecodeInlineHunkValue(value)) {
            if (actualType != expectedType) {
                THROW_ERROR_EXCEPTION(
                    "Non-inline hunk value #%v is of wrong type: expected %Qlv, actual %Qlv",
                    id,
                    expectedType,
                    actualType);
            }
            return;
        }
    }

    ValidateDataValue(*value);

    // The underlying schemaless reader may produce typed scalar values even if the schema has "any" type.
    // For schemaful reader, this is not an expected behavior
    // so we have to convert such values back into YSON.
    // Cf. YT-5396
    if (expectedType == EValueType::Any && actualType != EValueType::Any) {
        *value = EncodeUnversionedAnyValue(*value, memoryPool);
    } else {
        ValidateValueType(*value, schema, id, /*typeAnyAcceptsAllValues*/ false, ignoreRequired);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
