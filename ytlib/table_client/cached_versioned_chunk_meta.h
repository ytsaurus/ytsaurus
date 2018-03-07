#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"
#include "columnar_chunk_meta.h"
#include "schema.h"
#include "unversioned_row.h"

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/public.h>

#include <yt/core/actions/future.h>

#include <memory>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TCachedVersionedChunkMeta
    : public TColumnarChunkMeta
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::TChunkId, ChunkId);
    DEFINE_BYREF_RO_PROPERTY(TOwningKey, MinKey);
    DEFINE_BYREF_RO_PROPERTY(TOwningKey, MaxKey);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TColumnIdMapping>, SchemaIdMapping);
    DEFINE_BYVAL_RO_PROPERTY(int, ChunkKeyColumnCount);
    DEFINE_BYVAL_RO_PROPERTY(int, KeyColumnCount);
    DEFINE_BYREF_RO_PROPERTY(TTableSchema, Schema);

    static TCachedVersionedChunkMetaPtr Create(
        const NChunkClient::TChunkId& chunkId,
        const NChunkClient::NProto::TChunkMeta& chunkMeta,
        const TTableSchema& schema,
        NNodeTrackerClient::TNodeMemoryTracker* memoryTracker = nullptr);

    static TFuture<TCachedVersionedChunkMetaPtr> Load(
        NChunkClient::IChunkReaderPtr chunkReader,
        const TWorkloadDescriptor& workloadDescriptor,
        const NChunkClient::TReadSessionId& readSessionId,
        const TTableSchema& schema,
        NNodeTrackerClient::TNodeMemoryTracker* memoryTracker = nullptr);

private:
    NNodeTrackerClient::TNodeMemoryTrackerGuard MemoryTrackerGuard_;

    TCachedVersionedChunkMeta();

    void Init(
        const NChunkClient::TChunkId& chunkId,
        const NChunkClient::NProto::TChunkMeta& chunkMeta,
        const TTableSchema& schema,
        NNodeTrackerClient::TNodeMemoryTracker* memoryTracker);

    void ValidateChunkMeta();
    void ValidateSchema(const TTableSchema& readerSchema);

    DECLARE_NEW_FRIEND();
};

DEFINE_REFCOUNTED_TYPE(TCachedVersionedChunkMeta)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
