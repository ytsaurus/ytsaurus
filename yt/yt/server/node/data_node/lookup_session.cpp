#include "lookup_session.h"

#include "bootstrap.h"
#include "chunk.h"
#include "chunk_registry.h"
#include "local_chunk_reader.h"
#include "private.h"
#include "table_schema_cache.h"
#include "chunk_meta_manager.h"

#include <yt/yt/server/node/tablet_node/versioned_chunk_meta_manager.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>

#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NDataNode {

using namespace NYT::NChunkClient;
using namespace NYT::NChunkClient::NProto;
using namespace NYT::NConcurrency;
using namespace NTableClient;
using namespace NTabletNode;
using namespace NObjectClient;
using namespace NClusterNode;
using namespace NHydra;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

struct TKeyReaderBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

TLookupSession::TLookupSession(
    IBootstrap* bootstrap,
    IChunkPtr chunk,
    TReadSessionId readSessionId,
    TWorkloadDescriptor workloadDescriptor,
    TColumnFilter columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    TTableSchemaPtr tableSchema,
    const std::vector<TSharedRef>& keyRefs,
    NCompression::ECodec codecId,
    TTimestamp overrideTimestamp,
    bool populateCache)
    : Bootstrap_(bootstrap)
    , Chunk_(std::move(chunk))
    , ChunkId_(Chunk_->GetId())
    , ColumnFilter_(std::move(columnFilter))
    , Timestamp_(timestamp)
    , ProduceAllVersions_(produceAllVersions)
    , TableSchema_(std::move(tableSchema))
    , OverrideTimestamp_(overrideTimestamp)
    , CodecId_(codecId)
    , Logger(DataNodeLogger.WithTag("ChunkId: %v, ReadSessionId: %v",
        ChunkId_,
        readSessionId))
{
    Options_.ChunkReaderStatistics = ChunkReaderStatistics_;
    Options_.ReadSessionId = readSessionId;
    Options_.WorkloadDescriptor = std::move(workloadDescriptor);
    Options_.PopulateCache = populateCache;

    // May be slow because of chunk meta cache misses.
    YT_ASSERT(CheckKeyColumnCompatibility());

    // NB: TableSchema is assumed to be fetched upon calling LookupSession.
    YT_VERIFY(TableSchema_);
    if (!TableSchema_->GetUniqueKeys()) {
        THROW_ERROR_EXCEPTION("Table schema for chunk %v must have unique keys", ChunkId_)
            << TErrorAttribute("read_session_id", readSessionId);
    }
    if (!TableSchema_->GetStrict()) {
        THROW_ERROR_EXCEPTION("Table schema for chunk %v must be strict", ChunkId_)
            << TErrorAttribute("read_session_id", readSessionId);
    }

    auto serializedKeys = keyRefs.size() == 1
        ? keyRefs[0]
        : MergeRefsToRef<TKeyReaderBufferTag>(keyRefs);
    auto wireReader = CreateWireProtocolReader(
        std::move(serializedKeys),
        New<TRowBuffer>(TKeyReaderBufferTag()));
    Keys_ = wireReader->ReadUnversionedRowset(/*captureValues*/ false);
    KeyCount_ = Keys_.Size();

    YT_VERIFY(KeyCount_ != 0);

    UnderlyingChunkReader_ = CreateLocalChunkReader(
        New<TReplicationReaderConfig>(),
        Chunk_,
        Bootstrap_->GetBlockCache(),
        Bootstrap_->GetChunkMetaManager()->GetBlockMetaCache());

    YT_LOG_DEBUG("Local chunk reader is created for lookup request (KeyCount: %v)",
        KeyCount_);
}

TFuture<TSharedRef> TLookupSession::Run()
{
    NProfiling::TWallTimer metaWaitTimer;
    const auto& chunkMetaManager = Bootstrap_->GetVersionedChunkMetaManager();

    auto metaFuture = chunkMetaManager->GetMeta(UnderlyingChunkReader_, TableSchema_, Options_);
    if (metaFuture.IsSet()) {
        const auto& metaOrError = metaFuture.Get();
        return metaOrError.IsOK()
            ? OnGotMeta(/*timer*/ std::nullopt, metaOrError.Value())
            : MakeFuture<TSharedRef>(TError(metaOrError));
    }

    return metaFuture.Apply(BIND(
        &TLookupSession::OnGotMeta,
        MakeStrong(this),
        std::move(metaWaitTimer))
        .AsyncVia(Bootstrap_->GetStorageLookupInvoker()));
}

const TChunkReaderStatisticsPtr& TLookupSession::GetChunkReaderStatistics()
{
    return ChunkReaderStatistics_;
}

std::tuple<TTableSchemaPtr, bool> TLookupSession::FindTableSchema(
    TChunkId chunkId,
    TReadSessionId readSessionId,
    const TReqLookupRows::TTableSchemaData& schemaData,
    const TTableSchemaCachePtr& tableSchemaCache)
{
    const auto& Logger = DataNodeLogger;

    auto tableId = FromProto<TObjectId>(schemaData.table_id());
    auto revision = schemaData.revision();
    i64 schemaSize = schemaData.has_schema_size() ? schemaData.schema_size() : 1_MB;

    auto tableSchemaWrapper = tableSchemaCache->GetOrCreate(TSchemaCacheKey{tableId, revision}, schemaSize);
    YT_VERIFY(tableSchemaWrapper);
    if (tableSchemaWrapper->IsSet()) {
        return {tableSchemaWrapper->GetValue(), false};
    }

    if (!schemaData.has_schema()) {
        bool isSchemaRequested = tableSchemaWrapper->TryRequestSchema();

        YT_LOG_DEBUG("Schema for lookup request is missing"
            "(ChunkId: %v, ReadSessionId: %v, TableId: %v, Revision: %x, SchemaSize: %v, IsSchemaRequested: %v)",
            chunkId,
            readSessionId,
            tableId,
            revision,
            schemaSize,
            isSchemaRequested);

        return {nullptr, isSchemaRequested};
    }

    auto tableSchema = FromProto<TTableSchemaPtr>(schemaData.schema());
    tableSchemaWrapper->SetValue(tableSchema);

    YT_LOG_DEBUG("Inserted schema to schema cache for lookup request"
        "(ChunkId: %v, ReadSessionId: %v, TableId: %v, Revision: %x, SchemaSize: %v)",
        chunkId,
        readSessionId,
        tableId,
        revision,
        schemaSize);

    return {tableSchema, false};
}

bool TLookupSession::CheckKeyColumnCompatibility()
{
    auto chunkMeta = WaitFor(Chunk_->ReadMeta(Options_))
        .ValueOrThrow();
    auto type = CheckedEnumCast<EChunkType>(chunkMeta->type());
    if (type != EChunkType::Table) {
        THROW_ERROR_EXCEPTION("Chunk %v is of invalid type", ChunkId_)
            << TErrorAttribute("expected_chunk_type", EChunkType::Table)
            << TErrorAttribute("chunk_type", type);
    }

    const auto& tableKeyColumns = TableSchema_->GetKeyColumns();
    for (auto key : Keys_) {
        YT_VERIFY(key.GetCount() == tableKeyColumns.size());
    }

    TKeyColumns chunkKeyColumns;
    auto optionalKeyColumnsExt = FindProtoExtension<NTableClient::NProto::TKeyColumnsExt>(chunkMeta->extensions());
    // COMPAT(akozhikhov)
    if (optionalKeyColumnsExt) {
        chunkKeyColumns = FromProto<TKeyColumns>(*optionalKeyColumnsExt);
    } else {
        const auto& schemaExt = GetProtoExtension<NTableClient::NProto::TTableSchemaExt>(chunkMeta->extensions());
        chunkKeyColumns = FromProto<TTableSchema>(schemaExt).GetKeyColumns();
    }

    bool isCompatibleKeyColumns =
        tableKeyColumns.size() >= chunkKeyColumns.size() &&
        std::equal(
            chunkKeyColumns.begin(),
            chunkKeyColumns.end(),
            tableKeyColumns.begin());
    if (!isCompatibleKeyColumns) {
        THROW_ERROR_EXCEPTION("Chunk %v has incompatible key columns", ChunkId_)
            << TErrorAttribute("table_key_columns", tableKeyColumns)
            << TErrorAttribute("chunk_key_columns", chunkKeyColumns);
    }

    return true;
}

TFuture<TSharedRef> TLookupSession::OnGotMeta(
    std::optional<NProfiling::TWallTimer> timer,
    const TVersionedChunkMetaCacheEntryPtr& entry)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetStorageLookupInvoker());

    if (timer) {
        Options_.ChunkReaderStatistics->MetaWaitTime.fetch_add(
            timer->GetElapsedValue(),
            std::memory_order_relaxed);
    }

    const auto& chunkMeta = entry->Meta();

    TChunkSpec chunkSpec;
    ToProto(chunkSpec.mutable_chunk_id(), ChunkId_);

    auto chunkState = New<TChunkState>(
        Bootstrap_->GetBlockCache(),
        std::move(chunkSpec),
        chunkMeta,
        OverrideTimestamp_,
        /*lookupHashTable*/ nullptr,
        New<TChunkReaderPerformanceCounters>(),
        GetKeyComparer(),
        /*virtualValueDirectory*/ nullptr,
        TableSchema_);

    // TODO(akozhikhov): Maybe cache this reader somehow?
    return DoLookup(CreateVersionedChunkReader(
        TChunkReaderConfig::GetDefault(),
        UnderlyingChunkReader_,
        chunkState,
        chunkMeta,
        Options_,
        Keys_,
        ColumnFilter_,
        Timestamp_,
        ProduceAllVersions_));
}

TFuture<TSharedRef> TLookupSession::DoLookup(IVersionedReaderPtr reader) const
{
    return New<TVersionedRowsetReader>(
        KeyCount_,
        std::move(reader),
        CodecId_,
        Bootstrap_->GetStorageLookupInvoker())
        ->ReadRowset();
}

TKeyComparer TLookupSession::GetKeyComparer() const
{
    const auto& comparerProvider = Bootstrap_->GetRowComparerProvider();
    return TKeyComparer(comparerProvider->Get(TableSchema_->GetKeyColumnTypes()).UUComparer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
