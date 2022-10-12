#pragma once

#include "chunk.h"
#include "private.h"

#include <yt/yt/server/node/tablet_node/sorted_dynamic_comparer.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TLookupSession
    : public TRefCounted
{
public:
    TLookupSession(
        IBootstrap* bootstrap,
        IChunkPtr chunk,
        NChunkClient::TReadSessionId readSessionId,
        TWorkloadDescriptor workloadDescriptor,
        NTableClient::TColumnFilter columnFilter,
        NTransactionClient::TTimestamp timestamp,
        bool produceAllVersions,
        NTableClient::TTableSchemaPtr tableSchema,
        const std::vector<TSharedRef>& keyRefs,
        NCompression::ECodec codecId,
        NTransactionClient::TTimestamp overrideTimestamp,
        bool populateCache);

    TFuture<TSharedRef> Run();

    const NChunkClient::TChunkReaderStatisticsPtr& GetChunkReaderStatistics();

    //! Second value in tuple indicates whether we request schema from remote node.
    static std::tuple<NTableClient::TTableSchemaPtr, bool> FindTableSchema(
        TChunkId chunkId,
        NChunkClient::TReadSessionId readSessionId,
        const NChunkClient::NProto::TReqLookupRows::TTableSchemaData& schemaData,
        const TTableSchemaCachePtr& tableSchemaCache);

private:
    IBootstrap const* Bootstrap_;
    const IChunkPtr Chunk_;
    const TChunkId ChunkId_;
    const NTableClient::TColumnFilter ColumnFilter_;
    const NTransactionClient::TTimestamp Timestamp_;
    const bool ProduceAllVersions_;
    const NTableClient::TTableSchemaPtr TableSchema_;
    const NTransactionClient::TTimestamp OverrideTimestamp_;
    const NCompression::ECodec CodecId_;
    const NLogging::TLogger Logger;
    const NChunkClient::TChunkReaderStatisticsPtr ChunkReaderStatistics_ = New<NChunkClient::TChunkReaderStatistics>();

    TChunkReadOptions Options_;
    TSharedRange<NTableClient::TUnversionedRow> Keys_;
    int KeyCount_;
    NChunkClient::IChunkReaderPtr UnderlyingChunkReader_;


    bool CheckKeyColumnCompatibility();

    TFuture<TSharedRef> OnGotMeta(
        std::optional<NProfiling::TWallTimer> timer,
        const NTabletNode::TVersionedChunkMetaCacheEntryPtr& chunkMeta);
    TFuture<TSharedRef> DoLookup(NTableClient::IVersionedReaderPtr reader) const;

    NTableClient::TKeyComparer GetKeyComparer() const;
};

DEFINE_REFCOUNTED_TYPE(TLookupSession)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
