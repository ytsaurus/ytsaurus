#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"
#include "columnar_chunk_meta.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/node_tracker_client/public.h>
#include <yt/yt/ytlib/new_table_client/prepared_meta.h>

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
    DEFINE_BYREF_RO_PROPERTY(NTableClient::NProto::THunkChunkRefsExt, HunkChunkRefsExt);

    explicit TCachedVersionedChunkMeta(const NChunkClient::NProto::TChunkMeta& chunkMeta);
    static TCachedVersionedChunkMetaPtr Create(const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta);

    i64 GetMemoryUsage() const override;

    TIntrusivePtr<NNewTableClient::TPreparedChunkMeta> GetPreparedChunkMeta();
    void PrepareColumnarMeta();

    int GetChunkKeyColumnCount() const;
    void TrackMemory(const IMemoryUsageTrackerPtr& memoryTracker);

private:
    TMemoryUsageTrackerGuard MemoryTrackerGuard_;

    TAtomicPtr<NNewTableClient::TPreparedChunkMeta> PreparedMeta_;
    size_t PreparedMetaSize_ = 0;

    DECLARE_NEW_FRIEND();
};

DEFINE_REFCOUNTED_TYPE(TCachedVersionedChunkMeta)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
