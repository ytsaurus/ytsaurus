#include "lookup_session.h"

#include "chunk.h"
#include "chunk_registry.h"
#include "local_chunk_reader.h"
#include "private.h"
#include "table_schema_cache.h"

#include <yt/server/node/cell_node/bootstrap.h>

#include <yt/server/node/tablet_node/versioned_chunk_meta_manager.h>

#include <yt/client/misc/workload.h>
#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/ytlib/table_client/chunk_state.h>
#include <yt/client/table_client/config.h>
#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/client/table_client/row_buffer.h>
#include <yt/ytlib/table_client/versioned_chunk_reader.h>

#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/proto/data_node_service.pb.h>

namespace NYT::NDataNode {

using namespace NYT::NChunkClient;
using namespace NYT::NChunkClient::NProto;
using namespace NYT::NConcurrency;
using namespace NTableClient;
using namespace NTabletNode;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TLookupSession::TLookupSession(
    TBootstrap* bootstrap,
    TChunkId chunkId,
    TReadSessionId readSessionId,
    TWorkloadDescriptor workloadDescriptor,
    TColumnFilter columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    TCachedTableSchemaPtr tableSchema,
    const TString& requestedKeysString,
    NCompression::ECodec codecId)
    : Bootstrap_(bootstrap)
    , ChunkId_(chunkId)
    , ReadSessionId_(readSessionId)
    , WorkloadDescriptor_(std::move(workloadDescriptor))
    , ColumnFilter_(std::move(columnFilter))
    , Timestamp_(timestamp)
    , ProduceAllVersions_(produceAllVersions)
    , TableSchema_(std::move(tableSchema))
    , Codec_(NCompression::GetCodec(codecId))
{
    // NB: TableSchema is assumed to be set upon calling LookupSession.
    Chunk_ = Bootstrap_->GetChunkRegistry()->GetChunkOrThrow(chunkId);

    Verify();

    UnderlyingChunkReader_ = CreateLocalChunkReader(
        New<TReplicationReaderConfig>(),
        Chunk_,
        Bootstrap_->GetChunkBlockManager(),
        Bootstrap_->GetBlockCache(),
        Bootstrap_->GetBlockMetaCache());

    YT_LOG_DEBUG("Local chunk reader is created for lookup request (ChunkId: %v)",
        ChunkId_);

    TWireProtocolReader keysReader(
        TSharedRef::FromString<TKeyReaderBufferTag>(requestedKeysString),
        KeyReaderRowBuffer_);
    RequestedKeys_ = keysReader.ReadUnversionedRowset(true);

    YT_VERIFY(!RequestedKeys_.Empty());
}

TSharedRef TLookupSession::Run()
{
    return DoRun();
}

const TChunkReaderStatisticsPtr& TLookupSession::GetChunkReaderStatistics()
{
    return ChunkReaderStatistics_;
}

std::tuple<TCachedTableSchemaPtr, bool> TLookupSession::FindTableSchema(
    TChunkId chunkId,
    TReadSessionId readSessionId,
    const TReqLookupRows::TTableSchemaData& schemaData,
    const TTableSchemaCachePtr& tableSchemaCache)
{
    auto tableId = FromProto<TObjectId>(schemaData.table_id());
    auto revision = FromProto<TRevision>(schemaData.revision());

    YT_LOG_DEBUG("Trying to find schema for lookup request (ChunkId: %v, ReadSessionId: %v, TableId: %v, Revision: %llx)",
        chunkId,
        readSessionId,
        tableId,
        revision);

    auto tableSchemaWrapper = tableSchemaCache->GetOrCreate(TSchemaCacheKey{tableId, revision});
    YT_VERIFY(tableSchemaWrapper);
    if (tableSchemaWrapper->IsSet()) {
        return {tableSchemaWrapper->GetValue(), false};
    }

    if (!schemaData.has_schema()) {
        bool isSchemaRequested = tableSchemaWrapper->TryRequestSchema();

        YT_LOG_DEBUG("Schema for lookup request is missing (ChunkId: %v, ReadSessionId: %v, TableId: %v, Revision: %llx, IsSchemaRequested: %v)",
            chunkId,
            readSessionId,
            FromProto<TObjectId>(schemaData.table_id()),
            FromProto<TRevision>(schemaData.revision()),
            isSchemaRequested);

        return {nullptr, isSchemaRequested};
    }

    auto tableSchema = FromProto<TTableSchema>(schemaData.schema());
    auto rowKeyComparer = TSortedDynamicRowKeyComparer::Create(
        tableSchema.GetKeyColumns().size(),
        tableSchema);

    auto cachedTableSchema = New<TCachedTableSchema>(std::move(tableSchema), std::move(rowKeyComparer));
    tableSchemaWrapper->SetValue(cachedTableSchema);

    YT_LOG_DEBUG("Inserted schema to schema cache for lookup request (ChunkId: %v, ReadSessionId: %v, TableId: %v, Revision: %llx)",
        chunkId,
        readSessionId,
        tableId,
        revision);

    return {cachedTableSchema, false};
}

void TLookupSession::Verify()
{
    const auto& tableKeyColumns = TableSchema_->TableSchema.GetKeyColumns();
    for (const auto& key : RequestedKeys_) {
        YT_VERIFY(key.GetCount() == tableKeyColumns.size());
    }

    Options_.WorkloadDescriptor = WorkloadDescriptor_;
    Options_.ChunkReaderStatistics = ChunkReaderStatistics_;
    Options_.ReadSessionId = ReadSessionId_;

    auto chunkMeta = WaitFor(Chunk_->ReadMeta(Options_))
        .ValueOrThrow();
    auto type = CheckedEnumCast<EChunkType>(chunkMeta->type());
    if (type != EChunkType::Table) {
        THROW_ERROR_EXCEPTION("Chunk %v is of invalid type", ChunkId_)
            << TErrorAttribute("read_session_id", ReadSessionId_)
            << TErrorAttribute("expected_chunk_type", EChunkType::Table)
            << TErrorAttribute("chunk_type", type);
    }

    if (!TableSchema_->TableSchema.GetUniqueKeys()) {
        THROW_ERROR_EXCEPTION("Chunk %v must have unique keys", ChunkId_)
            << TErrorAttribute("read_session_id", ReadSessionId_);
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
            << TErrorAttribute("read_session_id", ReadSessionId_)
            << TErrorAttribute("table_key_columns", tableKeyColumns)
            << TErrorAttribute("chunk_key_columns", chunkKeyColumns);
    }

    YT_LOG_DEBUG("Lookup session is verified (ChunkId: %v, ReadSessionId: %v)",
        ChunkId_,
        ReadSessionId_);
}

TSharedRef TLookupSession::DoRun()
{
    const auto& chunkMetaManager = Bootstrap_->GetVersionedChunkMetaManager();
    auto versionedChunkMeta = WaitFor(
        chunkMetaManager->GetMeta(UnderlyingChunkReader_, TableSchema_->TableSchema, Options_))
        .ValueOrThrow();

    TChunkSpec chunkSpec;
    ToProto(chunkSpec.mutable_chunk_id(), ChunkId_);

    auto chunkState = New<TChunkState>(
        Bootstrap_->GetBlockCache(),
        std::move(chunkSpec),
        versionedChunkMeta,
        NullTimestamp,
        nullptr /* lookupHashTable */,
        New<TChunkReaderPerformanceCounters>(),
        TableSchema_->RowKeyComparer);

    YT_LOG_DEBUG("Starting to read requested keys in lookup session (ChunkId: %v, ReadSessionId: %v, KeyCount: %v)",
        ChunkId_,
        ReadSessionId_,
        RequestedKeys_.size());

    TWireProtocolWriter writer;

    auto onRow = [&] (TVersionedRow row) {
        writer.WriteVersionedRow(row);
    };

    auto rowReaderAdapter = New<TRowReaderAdapter>(
        New<TChunkReaderConfig>(),
        UnderlyingChunkReader_,
        chunkState,
        chunkState->ChunkMeta,
        Options_,
        RequestedKeys_,
        ColumnFilter_,
        Timestamp_,
        ProduceAllVersions_);
    rowReaderAdapter->ReadRowset(onRow);

    // TODO(akozhikhov): update compression statistics.
    return Codec_->Compress(writer.Finish());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
