#pragma once

#include "chunk.h"
#include "private.h"

#include <yt/server/node/cluster_node/bootstrap.h>

#include <yt/server/node/tablet_node/sorted_dynamic_comparer.h>

#include <yt/client/table_client/unversioned_row.h>

#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/proto/data_node_service.pb.h>

#include <yt/client/misc/workload.h>

namespace NYT::NDataNode {

using namespace NClusterNode;
using namespace NChunkClient;
using namespace NTabletNode;
using namespace NQueryClient;
using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TLookupSession
{
public:
    TLookupSession(
        TBootstrap* bootstrap,
        TChunkId chunkId,
        TReadSessionId readSessionId,
        TWorkloadDescriptor workloadDescriptor,
        TColumnFilter columnFilter,
        TTimestamp timestamp,
        bool produceAllVersions,
        TCachedTableSchemaPtr tableSchema,
        const TString& requestedKeysString,
        NCompression::ECodec codecId);

    TSharedRef Run();

    const TChunkReaderStatisticsPtr& GetChunkReaderStatistics();

    //! Second value in tuple indicates whether we request schema from remote node.
    static std::tuple<TCachedTableSchemaPtr, bool> FindTableSchema(
        TChunkId chunkId,
        TReadSessionId readSessionId,
        const NChunkClient::NProto::TReqLookupRows::TTableSchemaData& schemaData,
        const TTableSchemaCachePtr& tableSchemaCache);

private:
    struct TKeyReaderBufferTag { };

    TBootstrap const* Bootstrap_;
    const TChunkId ChunkId_;
    const TReadSessionId ReadSessionId_;
    const TWorkloadDescriptor WorkloadDescriptor_;
    const TColumnFilter ColumnFilter_;
    const TTimestamp Timestamp_;
    const bool ProduceAllVersions_;
    const TCachedTableSchemaPtr TableSchema_;
    NCompression::ICodec* const Codec_;

    IChunkPtr Chunk_;
    TBlockReadOptions Options_;
    IChunkReaderPtr UnderlyingChunkReader_;
    TSharedRange<TUnversionedRow> RequestedKeys_;
    const TRowBufferPtr KeyReaderRowBuffer_ = New<TRowBuffer>(TKeyReaderBufferTag());
    const TChunkReaderStatisticsPtr ChunkReaderStatistics_ = New<TChunkReaderStatistics>();

    void Verify();

    TSharedRef DoRun();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
