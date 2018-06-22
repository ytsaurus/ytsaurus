#include "helpers.h"
#include "chunk_meta_extensions.h"
#include "config.h"
#include "schemaless_chunk_reader.h"
#include "schemaless_chunk_writer.h"
#include "private.h"
#include "schemaless_reader.h"
#include "schemaless_writer.h"
#include "name_table.h"

#include <yt/ytlib/api/native_client.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/input_chunk.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/formats/parser.h>

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/ytlib/ypath/rich.h>

#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/periodic_yielder.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/node.h>
#include <yt/core/ytree/permission.h>


namespace NYT {
namespace NTableClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NFormats;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NProto;
using namespace NScheduler::NProto;
using namespace NYTree;
using namespace NYson;

using NYPath::TRichYPath;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TTableOutput::TTableOutput(const TFormat& format, IYsonConsumer* consumer)
    : Parser_(CreateParserForFormat(format, EDataType::Tabular, consumer))
{ }

TTableOutput::TTableOutput(std::unique_ptr<IParser> parser)
    : Parser_(std::move(parser))
{ }

TTableOutput::~TTableOutput() throw() = default;

void TTableOutput::DoWrite(const void* buf, size_t len)
{
    YCHECK(IsParserValid_);
    try {
        Parser_->Read(TStringBuf(static_cast<const char*>(buf), len));
    } catch (const std::exception& ex) {
        IsParserValid_ = false;
        throw;
    }
}

void TTableOutput::DoFinish()
{
    if (IsParserValid_) {
        // Dump everything into consumer.
        Parser_->Finish();
    }
}

////////////////////////////////////////////////////////////////////////////////

void PipeReaderToWriter(
    ISchemalessReaderPtr reader,
    ISchemalessWriterPtr writer,
    TPipeReaderToWriterOptions options)
{
    TPeriodicYielder yielder(TDuration::Seconds(1));
    int bufferRowCount = options.BufferRowCount;

    std::vector<TUnversionedRow> rows;
    rows.reserve(bufferRowCount);

    while (reader->Read(&rows)) {
        yielder.TryYield();
        if (rows.empty()) {
            WaitFor(reader->GetReadyEvent())
                .ThrowOnError();
            continue;
        }

        if (options.ValidateValues) {
            for (const auto row : rows) {
                for (const auto& value : row) {
                    ValidateStaticValue(value);
                }
            }
        }

        if (options.Throttler) {
            i64 dataWeight = 0;
            for (const auto row : rows) {
                dataWeight += GetDataWeight(row);
            }
            WaitFor(options.Throttler->Throttle(dataWeight))
                .ThrowOnError();
        }

        if (!rows.empty() && options.PipeDelay) {
            WaitFor(TDelayedExecutor::MakeDelayed(options.PipeDelay))
                .ThrowOnError();
        }

        if (!writer->Write(rows)) {
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError();
        }
    }

    WaitFor(writer->Close())
        .ThrowOnError();

    YCHECK(rows.empty());
}

void PipeInputToOutput(
    IInputStream* input,
    IOutputStream* output,
    i64 bufferBlockSize)
{
    struct TWriteBufferTag { };
    TBlob buffer(TWriteBufferTag(), bufferBlockSize);

    TPeriodicYielder yielder(TDuration::Seconds(1));

    while (true) {
        yielder.TryYield();

        size_t length = input->Read(buffer.Begin(), buffer.Size());
        if (length == 0)
            break;

        output->Write(buffer.Begin(), length);
    }

    output->Finish();
}

void PipeInputToOutput(
    NConcurrency::IAsyncInputStreamPtr input,
    IOutputStream* output,
    i64 bufferBlockSize)
{
    struct TWriteBufferTag { };
    auto buffer = TSharedMutableRef::Allocate<TWriteBufferTag>(bufferBlockSize);

    while (true) {
        auto length = WaitFor(input->Read(buffer))
            .ValueOrThrow();

        if (length == 0) {
            break;
        }

        output->Write(buffer.Begin(), length);
    }

    output->Finish();
}


////////////////////////////////////////////////////////////////////////////////

// NB: not using TYsonString here to avoid copying.
TUnversionedValue MakeUnversionedValue(TStringBuf ysonString, int id, TStatelessLexer& lexer)
{
    TToken token;
    lexer.GetToken(ysonString, &token);
    YCHECK(!token.IsEmpty());

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

int GetSystemColumnCount(TChunkReaderOptionsPtr options)
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

    return systemColumnCount;
}

void ValidateKeyColumns(const TKeyColumns& keyColumns, const TKeyColumns& chunkKeyColumns, bool requireUniqueKeys)
{
    if (requireUniqueKeys) {
        if (chunkKeyColumns.size() > keyColumns.size()) {
            THROW_ERROR_EXCEPTION("Chunk has more key columns than requested: actual %v, expected %v",
                chunkKeyColumns,
                keyColumns);
        }
    } else {
        if (chunkKeyColumns.size() < keyColumns.size()) {
            THROW_ERROR_EXCEPTION("Chunk has less key columns than requested: actual %v, expected %v",
                chunkKeyColumns,
                keyColumns);
        }
    }

    for (int i = 0; i < std::min(keyColumns.size(), chunkKeyColumns.size()); ++i) {
        if (chunkKeyColumns[i] != keyColumns[i]) {
            THROW_ERROR_EXCEPTION("Incompatible key columns: actual %v, expected %v",
                chunkKeyColumns,
                keyColumns);
        }
    }
}

TColumnFilter CreateColumnFilter(const TNullable<std::vector<TString>>& columns, TNameTablePtr nameTable)
{
    TColumnFilter columnFilter;
    if (!columns) {
        return columnFilter;
    }

    columnFilter.All = false;
    for (auto column : *columns) {
        auto id = nameTable->GetIdOrRegisterName(column);
        columnFilter.Indexes.push_back(id);
    }

    return columnFilter;
}

////////////////////////////////////////////////////////////////////////////////

void TTableUploadOptions::Persist(NPhoenix::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, UpdateMode);
    Persist(context, LockMode);
    Persist(context, TableSchema);
    Persist(context, SchemaMode);
    Persist(context, OptimizeFor);
    Persist(context, CompressionCodec);
    Persist(context, ErasureCodec);
}

////////////////////////////////////////////////////////////////////////////////

void ValidateKeyColumnsEqual(const TKeyColumns& keyColumns, const TTableSchema& schema)
{
     if (keyColumns != schema.GetKeyColumns()) {
         THROW_ERROR_EXCEPTION("YPath attribute \"sorted_by\" must be compatible with table schema for a \"strong\" schema mode")
             << TErrorAttribute("key_columns", keyColumns)
             << TErrorAttribute("table_schema", schema);
     }
}

void ValidateAppendKeyColumns(const TKeyColumns& keyColumns, const TTableSchema& schema, i64 rowCount)
{
    ValidateKeyColumns(keyColumns);

    if (rowCount == 0) {
        return;
    }

    auto tableKeyColumns = schema.GetKeyColumns();
    bool areKeyColumnsCompatible = true;
    if (tableKeyColumns.size() < keyColumns.size()) {
        areKeyColumnsCompatible = false;
    } else {
        for (int i = 0; i < keyColumns.size(); ++i) {
            if (tableKeyColumns[i] != keyColumns[i]) {
                areKeyColumnsCompatible = false;
                break;
            }
        }
    }

    if (!areKeyColumnsCompatible) {
        THROW_ERROR_EXCEPTION("Key columns mismatch while trying to append sorted data into a non-empty table")
            << TErrorAttribute("append_key_columns", keyColumns)
            << TErrorAttribute("current_key_columns", tableKeyColumns);
    }
}

TTableUploadOptions GetTableUploadOptions(
    const TRichYPath& path,
    const IAttributeDictionary& cypressTableAttributes,
    i64 rowCount)
{
    auto schema = cypressTableAttributes.Get<TTableSchema>("schema");
    auto schemaMode = cypressTableAttributes.Get<ETableSchemaMode>("schema_mode");
    auto optimizeFor = cypressTableAttributes.Get<EOptimizeFor>("optimize_for", EOptimizeFor::Lookup);
    auto compressionCodec = cypressTableAttributes.Get<NCompression::ECodec>("compression_codec");
    auto erasureCodec = cypressTableAttributes.Get<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);

    // Some ypath attributes are not compatible with attribute "schema".
    if (path.GetAppend() && path.GetSchema()) {
        THROW_ERROR_EXCEPTION("YPath attributes \"append\" and \"schema\" are not compatible")
            << TErrorAttribute("path", path);
    }

    if (!path.GetSortedBy().empty() && path.GetSchema()) {
        THROW_ERROR_EXCEPTION("YPath attributes \"sorted_by\" and \"schema\" are not compatible")
            << TErrorAttribute("path", path);
    }

    TTableUploadOptions result;
    if (path.GetAppend() && !path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Strong)) {
        ValidateKeyColumnsEqual(path.GetSortedBy(), schema);

        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Append;
        result.SchemaMode = ETableSchemaMode::Strong;
        result.TableSchema = schema;
    } else if (path.GetAppend() && !path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Weak)) {
        // Old behaviour.
        ValidateAppendKeyColumns(path.GetSortedBy(), schema, rowCount);

        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Append;
        result.SchemaMode = ETableSchemaMode::Weak;
        result.TableSchema = TTableSchema::FromKeyColumns(path.GetSortedBy());
    } else if (path.GetAppend() && path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Strong)) {
        result.LockMode = schema.IsSorted() ? ELockMode::Exclusive : ELockMode::Shared;
        result.UpdateMode = EUpdateMode::Append;
        result.SchemaMode = ETableSchemaMode::Strong;
        result.TableSchema = schema;
    } else if (path.GetAppend() && path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Weak)) {
        // Old behaviour - reset key columns if there were any.
        result.LockMode = ELockMode::Shared;
        result.UpdateMode = EUpdateMode::Append;
        result.SchemaMode = ETableSchemaMode::Weak;
        result.TableSchema = TTableSchema();
    } else if (!path.GetAppend() && !path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Strong)) {
        ValidateKeyColumnsEqual(path.GetSortedBy(), schema);

        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Overwrite;
        result.SchemaMode = ETableSchemaMode::Strong;
        result.TableSchema = schema;
    } else if (!path.GetAppend() && !path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Weak)) {
        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Overwrite;
        result.SchemaMode = ETableSchemaMode::Weak;
        result.TableSchema = TTableSchema::FromKeyColumns(path.GetSortedBy());
    } else if (!path.GetAppend() && path.GetSchema() && (schemaMode == ETableSchemaMode::Strong)) {
        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Overwrite;
        result.SchemaMode = ETableSchemaMode::Strong;
        result.TableSchema = *path.GetSchema();
    } else if (!path.GetAppend() && path.GetSchema() && (schemaMode == ETableSchemaMode::Weak)) {
        // Change from Weak to Strong schema mode.
        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Overwrite;
        result.SchemaMode = ETableSchemaMode::Strong;
        result.TableSchema = *path.GetSchema();
    } else if (!path.GetAppend() && path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Strong)) {
        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Overwrite;
        result.SchemaMode = ETableSchemaMode::Strong;
        result.TableSchema = schema;
    } else if (!path.GetAppend() && path.GetSortedBy().empty() && (schemaMode == ETableSchemaMode::Weak)) {
        result.LockMode = ELockMode::Exclusive;
        result.UpdateMode = EUpdateMode::Overwrite;
        result.SchemaMode = ETableSchemaMode::Weak;
        result.TableSchema = TTableSchema();
    } else {
        // Do not use Y_UNREACHABLE here, since this code is executed inside scheduler.
        THROW_ERROR_EXCEPTION("Failed to define upload parameters")
            << TErrorAttribute("path", path)
            << TErrorAttribute("schema_mode", schemaMode)
            << TErrorAttribute("schema", schema);
    }

    if (path.GetAppend() && path.GetOptimizeFor()) {
        THROW_ERROR_EXCEPTION("YPath attributes \"append\" and \"optimize_for\" are not compatible")
            << TErrorAttribute("path", path);
    }

    if (path.GetOptimizeFor()) {
        result.OptimizeFor = *path.GetOptimizeFor();
    } else {
        result.OptimizeFor = optimizeFor;
    }

    if (path.GetAppend() && path.GetCompressionCodec()) {
        THROW_ERROR_EXCEPTION("YPath attributes \"append\" and \"compression_codec\" are not compatible")
            << TErrorAttribute("path", path);
    }

    if (path.GetCompressionCodec()) {
        result.CompressionCodec = *path.GetCompressionCodec();
    } else {
        result.CompressionCodec = compressionCodec;
    }

    if (path.GetAppend() && path.GetErasureCodec()) {
        THROW_ERROR_EXCEPTION("YPath attributes \"append\" and \"erasure_codec\" are not compatible")
            << TErrorAttribute("path", path);
    }

    if (path.GetErasureCodec()) {
        result.ErasureCodec = *path.GetErasureCodec();
    } else {
        result.ErasureCodec = erasureCodec;
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void YTreeNodeToUnversionedValue(TUnversionedOwningRowBuilder* builder, const INodePtr& value, int id, bool aggregate)
{
    switch (value->GetType()) {
        case ENodeType::Entity:
            builder->AddValue(MakeUnversionedSentinelValue(EValueType::Null, id, aggregate));
            break;
        case ENodeType::Int64:
            builder->AddValue(MakeUnversionedInt64Value(value->GetValue<i64>(), id, aggregate));
            break;
        case ENodeType::Uint64:
            builder->AddValue(MakeUnversionedUint64Value(value->GetValue<ui64>(), id, aggregate));
            break;
        case ENodeType::Double:
            builder->AddValue(MakeUnversionedDoubleValue(value->GetValue<double>(), id, aggregate));
            break;
        case ENodeType::String:
            builder->AddValue(MakeUnversionedStringValue(value->GetValue<TString>(), id, aggregate));
            break;
        default:
            builder->AddValue(MakeUnversionedAnyValue(ConvertToYsonString(value).GetData(), id, aggregate));
            break;
    }
}

TUnversionedOwningRow YsonToSchemafulRow(
    const TString& yson,
    const TTableSchema& tableSchema,
    bool treatMissingAsNull)
{
    auto nameTable = TNameTable::FromSchema(tableSchema);

    auto rowParts = ConvertTo<THashMap<TString, INodePtr>>(
        TYsonString(yson, EYsonType::MapFragment));

    TUnversionedOwningRowBuilder rowBuilder;
    auto addValue = [&] (int id, INodePtr value) {
        if (value->GetType() == ENodeType::Entity) {
            rowBuilder.AddValue(MakeUnversionedSentinelValue(
                value->Attributes().Get<EValueType>("type", EValueType::Null), id));
            return;
        }

        switch (tableSchema.Columns()[id].GetPhysicalType()) {
            case EValueType::Boolean:
                rowBuilder.AddValue(MakeUnversionedBooleanValue(value->GetValue<bool>(), id));
                break;
            case EValueType::Int64:
                rowBuilder.AddValue(MakeUnversionedInt64Value(value->GetValue<i64>(), id));
                break;
            case EValueType::Uint64:
                rowBuilder.AddValue(MakeUnversionedUint64Value(value->GetValue<ui64>(), id));
                break;
            case EValueType::Double:
                rowBuilder.AddValue(MakeUnversionedDoubleValue(value->GetValue<double>(), id));
                break;
            case EValueType::String:
                rowBuilder.AddValue(MakeUnversionedStringValue(value->GetValue<TString>(), id));
                break;
            case EValueType::Any:
                rowBuilder.AddValue(MakeUnversionedAnyValue(ConvertToYsonString(value).GetData(), id));
                break;
            default:
                Y_UNREACHABLE();
        }
    };

    const auto& keyColumns = tableSchema.GetKeyColumns();

    // Key
    for (int id = 0; id < static_cast<int>(keyColumns.size()); ++id) {
        auto it = rowParts.find(nameTable->GetName(id));
        if (it == rowParts.end()) {
            rowBuilder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, id));
        } else {
            addValue(id, it->second);
        }
    }

    // Fixed values
    for (int id = static_cast<int>(keyColumns.size()); id < static_cast<int>(tableSchema.Columns().size()); ++id) {
        auto it = rowParts.find(nameTable->GetName(id));
        if (it != rowParts.end()) {
            addValue(id, it->second);
        } else if (treatMissingAsNull) {
            rowBuilder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, id));
        }
    }

    // Variable values
    for (const auto& pair : rowParts) {
        int id = nameTable->GetIdOrRegisterName(pair.first);
        if (id >= tableSchema.Columns().size()) {
            YTreeNodeToUnversionedValue(&rowBuilder, pair.second, id, false);
        }
    }

    return rowBuilder.FinishRow();
}

TUnversionedOwningRow YsonToSchemalessRow(const TString& valueYson)
{
    TUnversionedOwningRowBuilder builder;

    auto values = ConvertTo<std::vector<INodePtr>>(TYsonString(valueYson, EYsonType::ListFragment));
    for (const auto& value : values) {
        int id = value->Attributes().Get<int>("id");
        bool aggregate = value->Attributes().Find<bool>("aggregate").Get(false);
        YTreeNodeToUnversionedValue(&builder, value, id, aggregate);
    }

    return builder.FinishRow();
}

TVersionedRow YsonToVersionedRow(
    const TRowBufferPtr& rowBuffer,
    const TString& keyYson,
    const TString& valueYson,
    const std::vector<TTimestamp>& deleteTimestamps)
{
    TVersionedRowBuilder builder(rowBuffer);

    auto keys = ConvertTo<std::vector<INodePtr>>(TYsonString(keyYson, EYsonType::ListFragment));

    for (auto key : keys) {
        int id = key->Attributes().Get<int>("id");
        switch (key->GetType()) {
            case ENodeType::Int64:
                builder.AddKey(MakeUnversionedInt64Value(key->GetValue<i64>(), id));
                break;
            case ENodeType::Uint64:
                builder.AddKey(MakeUnversionedUint64Value(key->GetValue<ui64>(), id));
                break;
            case ENodeType::Double:
                builder.AddKey(MakeUnversionedDoubleValue(key->GetValue<double>(), id));
                break;
            case ENodeType::String:
                builder.AddKey(MakeUnversionedStringValue(key->GetValue<TString>(), id));
                break;
            default:
                Y_UNREACHABLE();
                break;
        }
    }

    auto values = ConvertTo<std::vector<INodePtr>>(TYsonString(valueYson, EYsonType::ListFragment));
    for (auto value : values) {
        int id = value->Attributes().Get<int>("id");
        auto timestamp = value->Attributes().Get<TTimestamp>("ts");
        bool aggregate = value->Attributes().Find<bool>("aggregate").Get(false);
        switch (value->GetType()) {
            case ENodeType::Entity:
                builder.AddValue(MakeVersionedSentinelValue(EValueType::Null, timestamp, id, aggregate));
                break;
            case ENodeType::Int64:
                builder.AddValue(MakeVersionedInt64Value(value->GetValue<i64>(), timestamp, id, aggregate));
                break;
            case ENodeType::Uint64:
                builder.AddValue(MakeVersionedUint64Value(value->GetValue<ui64>(), timestamp, id, aggregate));
                break;
            case ENodeType::Double:
                builder.AddValue(MakeVersionedDoubleValue(value->GetValue<double>(), timestamp, id, aggregate));
                break;
            case ENodeType::String:
                builder.AddValue(MakeVersionedStringValue(value->GetValue<TString>(), timestamp, id, aggregate));
                break;
            default:
                builder.AddValue(MakeVersionedAnyValue(ConvertToYsonString(value).GetData(), timestamp, id, aggregate));
                break;
        }
    }

    for (auto timestamp : deleteTimestamps) {
        builder.AddDeleteTimestamp(timestamp);
    }

    return builder.FinishRow();
}

TUnversionedOwningRow YsonToKey(const TString& yson)
{
    TUnversionedOwningRowBuilder keyBuilder;
    auto keyParts = ConvertTo<std::vector<INodePtr>>(
        TYsonString(yson, EYsonType::ListFragment));

    for (int id = 0; id < keyParts.size(); ++id) {
        const auto& keyPart = keyParts[id];
        switch (keyPart->GetType()) {
            case ENodeType::Int64:
                keyBuilder.AddValue(MakeUnversionedInt64Value(
                    keyPart->GetValue<i64>(),
                    id));
                break;
            case ENodeType::Uint64:
                keyBuilder.AddValue(MakeUnversionedUint64Value(
                    keyPart->GetValue<ui64>(),
                    id));
                break;
            case ENodeType::Double:
                keyBuilder.AddValue(MakeUnversionedDoubleValue(
                    keyPart->GetValue<double>(),
                    id));
                break;
            case ENodeType::String:
                keyBuilder.AddValue(MakeUnversionedStringValue(
                    keyPart->GetValue<TString>(),
                    id));
                break;
            case ENodeType::Entity:
                keyBuilder.AddValue(MakeUnversionedSentinelValue(
                    keyPart->Attributes().Get<EValueType>("type", EValueType::Null),
                    id));
                break;
            default:
                keyBuilder.AddValue(MakeUnversionedAnyValue(
                    ConvertToYsonString(keyPart).GetData(),
                    id));
                break;
        }
    }

    return keyBuilder.FinishRow();
}

TString KeyToYson(TUnversionedRow row)
{
    return ConvertToYsonString(row, EYsonFormat::Text).GetData();
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

    auto frontBoundaryKeys = GetProtoExtension<TBoundaryKeysExt>(chunks.front().chunk_meta().extensions());
    result.set_min(frontBoundaryKeys.min());
    auto backBoundaryKeys = GetProtoExtension<TBoundaryKeysExt>(chunks.back().chunk_meta().extensions());
    result.set_max(backBoundaryKeys.max());

    return result;
}

std::pair<TOwningKey, TOwningKey> GetChunkBoundaryKeys(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    int keyColumnCount)
{
    auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(chunkMeta.extensions());
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

    auto requested = nullableRequested.Get(AsyncLastCommittedTimestamp);
    if (requested != AsyncLastCommittedTimestamp) {
        auto retained = attributes.Get<TTimestamp>("retained_timestamp");
        auto unflushed = attributes.Get<TTimestamp>("unflushed_timestamp");
        if (requested < retained || requested >= unflushed) {
            THROW_ERROR_EXCEPTION("Requested timestamp is out of range for table %v",
                path.GetPath())
                << TErrorAttribute("requested_timestamp", requested)
                << TErrorAttribute("retained_timestamp", retained)
                << TErrorAttribute("unflushed_timestamp", unflushed);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): unify with input chunk collection procedure in operation_controller_detail.cpp.
std::vector<TInputChunkPtr> CollectTableInputChunks(
    const TRichYPath& path,
    const INativeClientPtr& client,
    const TNodeDirectoryPtr& nodeDirectory,
    const TFetchChunkSpecConfigPtr& config,
    const TTransactionId& transactionId,
    const TLogger& logger)
{
    const auto& Logger = logger;

    LOG_INFO("Getting table attributes (Path: %v)", path);

    TYPath objectIdPath;
    TCellTag tableCellTag;
    {
        TUserObject userObject;
        userObject.Path = path;
        GetUserObjectBasicAttributes(
            client,
            TMutableRange<TUserObject>(&userObject, 1),
            transactionId,
            Logger,
            EPermission::Read,
            false /* suppressAccessTracking */);

        const auto& objectId = userObject.ObjectId;
        tableCellTag = userObject.CellTag;
        objectIdPath = FromObjectId(objectId);
        if (userObject.Type != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                path,
                EObjectType::Table,
                userObject.Type);
        }
    }

    LOG_INFO("Requesting table chunk count (TableCellTag: %v, ObjectIdPath: %v)", tableCellTag, objectIdPath);
    int chunkCount;
    {
        auto channel = client->GetMasterChannelOrThrow(EMasterChannelKind::Follower);
        TObjectServiceProxy proxy(channel);

        auto req = TYPathProxy::Get(objectIdPath + "/@");
        SetTransactionId(req, transactionId);
        std::vector<TString> attributeKeys{"chunk_count"};
        NYT::ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting chunk count of %v",
            path);

        const auto& rsp = rspOrError.Value();
        auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

        chunkCount = attributes->Get<int>("chunk_count");
    }

    LOG_INFO("Fetching chunk specs (ChunkCount: %v)", chunkCount);

    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs;
    FetchChunkSpecs(
        client,
        nodeDirectory,
        tableCellTag,
        objectIdPath,
        path.GetRanges(),
        chunkCount,
        config->MaxChunksPerFetch,
        config->MaxChunksPerLocateRequest,
        [&] (TChunkOwnerYPathProxy::TReqFetchPtr req) {
            req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
            req->set_fetch_all_meta_extensions(false);
            SetTransactionId(req, transactionId);
        },
        Logger,
        &chunkSpecs);

    std::vector<TInputChunkPtr> inputChunks;
    for (const auto& chunkSpec : chunkSpecs) {
        inputChunks.emplace_back(New<TInputChunk>(chunkSpec));
    }

    return inputChunks;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TValue>
void UpdateColumnarStatistics(TColumnarStatisticsExt& columnarStatisticsExt, const TRange<TValue>& values)
{
    for (const auto& value : values) {
        auto id = value.Id;
        if (id >= columnarStatisticsExt.data_weights().size()) {
            columnarStatisticsExt.mutable_data_weights()->Resize(id + 1, 0);
        }
        columnarStatisticsExt.set_data_weights(id, columnarStatisticsExt.data_weights(id) + GetDataWeight(value));
    }
}

void UpdateColumnarStatistics(TColumnarStatisticsExt& columnarStatisticsExt, const TUnversionedRow& row)
{
    UpdateColumnarStatistics(columnarStatisticsExt, MakeRange(row.Begin(), row.End()));
}

void UpdateColumnarStatistics(TColumnarStatisticsExt& columnarStatisticsExt, const TVersionedRow& row)
{
    UpdateColumnarStatistics(columnarStatisticsExt, MakeRange(row.BeginKeys(), row.EndKeys()));
    UpdateColumnarStatistics(columnarStatisticsExt, MakeRange(row.BeginValues(), row.EndValues()));
    columnarStatisticsExt.set_timestamp_weight(
        columnarStatisticsExt.timestamp_weight() +
        (row.GetWriteTimestampCount() + row.GetDeleteTimestampCount()) * sizeof(TTimestamp));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTableClient
