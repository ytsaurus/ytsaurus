#pragma once

#include "chunk.h"
#include "private.h"

#include <yt/yt/server/node/tablet_node/sorted_dynamic_comparer.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>

#include <yt/yt/client/misc/workload.h>

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
        TCachedTableSchemaPtr tableSchema,
        const std::vector<TSharedRef>& serializedKeys,
        NCompression::ECodec codecId,
        NTransactionClient::TTimestamp chunkTimestamp,
        bool populateCache);

    TFuture<TSharedRef> Run();

    const NChunkClient::TChunkReaderStatisticsPtr& GetChunkReaderStatistics();

    //! Second value in tuple indicates whether we request schema from remote node.
    static std::tuple<TCachedTableSchemaPtr, bool> FindTableSchema(
        TChunkId chunkId,
        NChunkClient::TReadSessionId readSessionId,
        const NChunkClient::NProto::TReqLookupRows::TTableSchemaData& schemaData,
        const TTableSchemaCachePtr& tableSchemaCache);

private:
    struct TKeyReaderBufferTag { };

    IBootstrap const* Bootstrap_;
    const IChunkPtr Chunk_;
    const TChunkId ChunkId_;
    const NChunkClient::TReadSessionId ReadSessionId_;
    const NTableClient::TColumnFilter ColumnFilter_;
    const NTransactionClient::TTimestamp Timestamp_;
    const bool ProduceAllVersions_;
    const TCachedTableSchemaPtr TableSchema_;
    NCompression::ICodec* const Codec_;
    const NTransactionClient::TTimestamp ChunkTimestamp_;

    TChunkReadOptions Options_;
    NChunkClient::IChunkReaderPtr UnderlyingChunkReader_;
    TSharedRange<NTableClient::TUnversionedRow> RequestedKeys_;
    const NTableClient::TRowBufferPtr KeyReaderRowBuffer_ = New<NTableClient::TRowBuffer>(TKeyReaderBufferTag());
    const NChunkClient::TChunkReaderStatisticsPtr ChunkReaderStatistics_ = New<NChunkClient::TChunkReaderStatistics>();


    bool CheckKeyColumnCompatibility();

    TSharedRef DoRun(NTableClient::TCachedVersionedChunkMetaPtr chunkMeta);
};

DEFINE_REFCOUNTED_TYPE(TLookupSession)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
