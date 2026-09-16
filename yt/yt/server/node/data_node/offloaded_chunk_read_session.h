#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/misc/public.h>

#include <yt/yt/core/compression/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! It processes lookups offloaded from tablet node.
struct IOffloadedChunkReadSession
    : public TRefCounted
{
    virtual TFuture<TSharedRef> Lookup(const std::vector<TSharedRef>& keyRefs) = 0;

    virtual const NChunkClient::TChunkReaderStatisticsPtr& GetChunkReaderStatistics() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IOffloadedChunkReadSession)

////////////////////////////////////////////////////////////////////////////////

IOffloadedChunkReadSessionPtr CreateOffloadedChunkReadSession(
    IBootstrap* bootstrap,
    IChunkPtr chunk,
    NChunkClient::TReadSessionId readSessionId,
    TWorkloadDescriptor workloadDescriptor,
    NTableClient::TColumnFilter columnFilter,
    NTransactionClient::TTimestamp timestamp,
    bool produceAllVersions,
    NTableClient::TTableSchemaPtr tableSchema,
    NCompression::ECodec codecId,
    NTransactionClient::TTimestamp overrideTimestamp,
    bool populateCache,
    bool enableHashChunkIndex,
    bool useDirectIO);

////////////////////////////////////////////////////////////////////////////////

std::tuple<NTableClient::TTableSchemaPtr, bool> FindTableSchemaForOffloadedReadSession(
    TChunkId chunkId,
    NChunkClient::TReadSessionId readSessionId,
    const NChunkClient::NProto::TReqLookupRows::TTableSchemaData& schemaData,
    const TTableSchemaCachePtr& tableSchemaCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
