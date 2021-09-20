#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"
#include "columnar_chunk_meta.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>
#include <yt/yt/ytlib/new_table_client/prepared_meta.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/column_rename_descriptor.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>
#include <yt/yt/core/misc/atomic_ptr.h>

#include <yt/yt/core/actions/future.h>

#include <memory>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TCachedVersionedChunkMeta
    : public TColumnarChunkMeta
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::TChunkId, ChunkId);
    DEFINE_BYREF_RO_PROPERTY(TLegacyOwningKey, MinKey);
    DEFINE_BYREF_RO_PROPERTY(TLegacyOwningKey, MaxKey);
    DEFINE_BYVAL_RO_PROPERTY(int, ChunkKeyColumnCount);
    DEFINE_BYVAL_RO_PROPERTY(int, KeyColumnCount);
    DEFINE_BYREF_RO_PROPERTY(NTableClient::NProto::THunkChunkRefsExt, HunkChunkRefsExt);

    static TCachedVersionedChunkMetaPtr Create(
        NChunkClient::TChunkId chunkId,
        const NChunkClient::NProto::TChunkMeta& chunkMeta,
        const TTableSchemaPtr& schema,
        const TColumnRenameDescriptors& renameDescriptors = {},
        const IMemoryUsageTrackerPtr& memoryTracker = nullptr);

    static TFuture<TCachedVersionedChunkMetaPtr> Load(
        const NChunkClient::IChunkReaderPtr& chunkReader,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
        const TTableSchemaPtr& schema,
        const TColumnRenameDescriptors& renameDescriptors = {},
        const IMemoryUsageTrackerPtr& memoryTracker = nullptr);

    virtual i64 GetMemoryUsage() const override;

    TIntrusivePtr<NNewTableClient::TPreparedChunkMeta> GetPreparedChunkMeta();

private:
    TMemoryUsageTrackerGuard MemoryTrackerGuard_;

    TAtomicPtr<NNewTableClient::TPreparedChunkMeta> PreparedMeta_;

    TCachedVersionedChunkMeta();

    void Init(
        NChunkClient::TChunkId chunkId,
        const NChunkClient::NProto::TChunkMeta& chunkMeta,
        const TTableSchemaPtr& schema,
        const TColumnRenameDescriptors& renameDescriptors,
        const IMemoryUsageTrackerPtr& memoryTracker);

    void ValidateChunkMeta();
    void ValidateSchema(const TTableSchema& readerSchema);

    DECLARE_NEW_FRIEND();
};

DEFINE_REFCOUNTED_TYPE(TCachedVersionedChunkMeta)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
