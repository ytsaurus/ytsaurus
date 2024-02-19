#include "offloaded_chunk_read_session.h"

#include "bootstrap.h"
#include "chunk.h"
#include "chunk_registry.h"
#include "local_chunk_reader.h"
#include "private.h"
#include "table_schema_cache.h"
#include "chunk_meta_manager.h"

#include <yt/yt/server/node/tablet_node/sorted_dynamic_comparer.h>
#include <yt/yt/server/node/tablet_node/versioned_chunk_meta_manager.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/chunk_index_read_controller.h>
#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/indexed_versioned_chunk_reader.h>
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

class TOffloadedChunkReadSession
    : public IOffloadedChunkReadSession
{
public:
    TOffloadedChunkReadSession(
        IBootstrap* bootstrap,
        IChunkPtr chunk,
        TReadSessionId readSessionId,
        TWorkloadDescriptor workloadDescriptor,
        TColumnFilter columnFilter,
        TTimestamp timestamp,
        bool produceAllVersions,
        TTableSchemaPtr tableSchema,
        NCompression::ECodec codecId,
        TTimestamp overrideTimestamp,
        bool populateCache,
        bool enableHashChunkIndex,
        bool useDirectIO)
        : Bootstrap_(bootstrap)
        , Chunk_(std::move(chunk))
        , ChunkId_(Chunk_->GetId())
        , ColumnFilter_(std::move(columnFilter))
        , Timestamp_(timestamp)
        , ProduceAllVersions_(produceAllVersions)
        , TableSchema_(std::move(tableSchema))
        , OverrideTimestamp_(overrideTimestamp)
        , EnableHashChunkIndex_(enableHashChunkIndex)
        , UseDirectIO_(useDirectIO)
        , CodecId_(codecId)
        , Logger(DataNodeLogger.WithTag("ChunkId: %v, ReadSessionId: %v",
            ChunkId_,
            readSessionId))
    {
        Options_.ChunkReaderStatistics = ChunkReaderStatistics_;
        Options_.ReadSessionId = readSessionId;
        Options_.WorkloadDescriptor = std::move(workloadDescriptor);
        Options_.PopulateCache = populateCache;

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

        // TODO(akozhikhov): Do not create it when reading fragments (now we need it to read meta).
        UnderlyingChunkReader_ = CreateLocalChunkReader(
            New<TReplicationReaderConfig>(),
            Chunk_,
            Bootstrap_->GetBlockCache(),
            Bootstrap_->GetChunkMetaManager()->GetBlockMetaCache());

        YT_LOG_DEBUG("Local chunk reader is created for offloaded chunk read session");
    }

    TFuture<TSharedRef> Lookup(const std::vector<TSharedRef>& keyRefs) override
    {
        auto serializedKeys = keyRefs.size() == 1
            ? keyRefs[0]
            : MergeRefsToRef<TKeyReaderBufferTag>(keyRefs);
        auto wireReader = CreateWireProtocolReader(
            std::move(serializedKeys),
            New<TRowBuffer>(TKeyReaderBufferTag()));
        auto keys = wireReader->ReadUnversionedRowset(/*captureValues*/ false);
        YT_VERIFY(!keys.Empty());

        // May be slow because of chunk meta cache misses.
        YT_ASSERT(CheckKeyColumnCompatibility(keys));

        NProfiling::TWallTimer metaWaitTimer;
        const auto& chunkMetaManager = Bootstrap_->GetVersionedChunkMetaManager();

        auto metaFuture = chunkMetaManager->GetMeta(UnderlyingChunkReader_, TableSchema_, Options_);
        if (metaFuture.IsSet()) {
            const auto& metaOrError = metaFuture.Get();
            return metaOrError.IsOK()
                ? OnGotMeta(/*timer*/ std::nullopt, std::move(keys), metaOrError.Value())
                : MakeFuture<TSharedRef>(TError(metaOrError));
        }

        return metaFuture.Apply(BIND(
            &TOffloadedChunkReadSession::OnGotMeta,
            MakeStrong(this),
            std::move(metaWaitTimer),
            Passed(std::move(keys)))
            .AsyncVia(Bootstrap_->GetStorageLookupInvoker()));
    }

    const TChunkReaderStatisticsPtr& GetChunkReaderStatistics() const override
    {
        return ChunkReaderStatistics_;
    }

private:
    IBootstrap* const Bootstrap_;
    const IChunkPtr Chunk_;
    const TChunkId ChunkId_;
    const TColumnFilter ColumnFilter_;
    const NTransactionClient::TTimestamp Timestamp_;
    const bool ProduceAllVersions_;
    const TTableSchemaPtr TableSchema_;
    const NTransactionClient::TTimestamp OverrideTimestamp_;
    const bool EnableHashChunkIndex_;
    const bool UseDirectIO_;
    const NCompression::ECodec CodecId_;
    const NLogging::TLogger Logger;
    const NChunkClient::TChunkReaderStatisticsPtr ChunkReaderStatistics_ = New<NChunkClient::TChunkReaderStatistics>();

    TChunkReadOptions Options_;
    NChunkClient::IChunkReaderPtr UnderlyingChunkReader_;


    bool CheckKeyColumnCompatibility(const TSharedRange<TUnversionedRow>& keys)
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
        for (auto key : keys) {
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

    TFuture<TSharedRef> OnGotMeta(
        std::optional<NProfiling::TWallTimer> timer,
        TSharedRange<TUnversionedRow> keys,
        const TVersionedChunkMetaCacheEntryPtr& entry)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetStorageLookupInvoker());

        if (timer) {
            Options_.ChunkReaderStatistics->RecordMetaWaitTime(
                timer->GetElapsedTime());
        }

        auto chunkMeta = entry->Meta();

        TChunkSpec chunkSpec;
        ToProto(chunkSpec.mutable_chunk_id(), ChunkId_);

        if (EnableHashChunkIndex_ && chunkMeta->HashTableChunkIndexMeta()) {
            return LookupWithChunkIndex(std::move(chunkMeta), std::move(keys));
        }

        auto chunkState = New<TChunkState>(TChunkState{
            .BlockCache = Bootstrap_->GetBlockCache(),
            .ChunkSpec = std::move(chunkSpec),
            .ChunkMeta = chunkMeta,
            .OverrideTimestamp = OverrideTimestamp_,
            .KeyComparer = GetKeyComparer(),
            .TableSchema = TableSchema_,
        });

        // TODO(akozhikhov): Cache this reader and chunk state with chunk column mapping.
        int keyCount = keys.Size();
        return DoLookup(
            keyCount,
            CreateVersionedChunkReader(
                TChunkReaderConfig::GetDefault(),
                UnderlyingChunkReader_,
                chunkState,
                std::move(chunkMeta),
                Options_,
                std::move(keys),
                ColumnFilter_,
                Timestamp_,
                ProduceAllVersions_));
    }

    TFuture<TSharedRef> LookupWithChunkIndex(
        TCachedVersionedChunkMetaPtr chunkMeta,
        TSharedRange<TUnversionedRow> keys)
    {
        auto keyCount = keys.Size();

        YT_LOG_DEBUG("Creating local chunk index read session (KeyCount: %v)",
            keyCount);

        auto controller = CreateChunkIndexReadController(
            Chunk_->GetId(),
            ColumnFilter_,
            std::move(chunkMeta),
            std::move(keys),
            GetKeyComparer(),
            TableSchema_,
            Timestamp_,
            ProduceAllVersions_,
            Bootstrap_->GetBlockCache(),
            /*testingOptions*/ std::nullopt,
            Logger);

        ILocalChunkFragmentReaderPtr chunkFragmentReader;
        try {
            chunkFragmentReader = CreateLocalChunkFragmentReader(
                std::move(Chunk_),
                UseDirectIO_);
        } catch (const std::exception& ex) {
            return MakeFuture<TSharedRef>(TError(ex));
        }

        if (auto future = chunkFragmentReader->PrepareToReadChunkFragments(Options_)) {
            YT_LOG_DEBUG("Will wait for chunk reader to become prepared");

            return future.Apply(BIND([
                =,
                this,
                this_ = MakeStrong(this),
                controller = std::move(controller),
                chunkFragmentReader = std::move(chunkFragmentReader)
            ] () mutable {
                return DoLookup(
                    keyCount,
                    CreateIndexedVersionedChunkReader(
                        Options_,
                        std::move(controller),
                        UnderlyingChunkReader_,
                        std::move(chunkFragmentReader)));
            })
                .AsyncVia(Bootstrap_->GetStorageLookupInvoker()));
        }

        return DoLookup(
            keyCount,
            CreateIndexedVersionedChunkReader(
                Options_,
                std::move(controller),
                UnderlyingChunkReader_,
                std::move(chunkFragmentReader)));
    }

    TFuture<TSharedRef> DoLookup(int keyCount, IVersionedReaderPtr reader) const
    {
        return New<TVersionedRowsetReader>(
            keyCount,
            std::move(reader),
            CodecId_,
            Bootstrap_->GetStorageLookupInvoker())
            ->ReadRowset();
    }

    TKeyComparer GetKeyComparer() const
    {
        const auto& comparerProvider = Bootstrap_->GetRowComparerProvider();
        return TKeyComparer(comparerProvider->Get(TableSchema_->GetKeyColumnTypes()).UUComparer);
    }
};

////////////////////////////////////////////////////////////////////////////////

IOffloadedChunkReadSessionPtr CreateOffloadedChunkReadSession(
    IBootstrap* bootstrap,
    IChunkPtr chunk,
    NChunkClient::TReadSessionId readSessionId,
    TWorkloadDescriptor workloadDescriptor,
    TColumnFilter columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    TTableSchemaPtr tableSchema,
    NCompression::ECodec codecId,
    TTimestamp overrideTimestamp,
    bool populateCache,
    bool enableHashChunkIndex,
    bool useDirectIO)
{
    return New<TOffloadedChunkReadSession>(
        bootstrap,
        std::move(chunk),
        readSessionId,
        workloadDescriptor,
        columnFilter,
        timestamp,
        produceAllVersions,
        std::move(tableSchema),
        codecId,
        overrideTimestamp,
        populateCache,
        enableHashChunkIndex,
        useDirectIO);
}

////////////////////////////////////////////////////////////////////////////////

std::tuple<TTableSchemaPtr, bool> FindTableSchemaForOffloadedReadSession(
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
