#include "hunks_serialization.h"

#include "serialize.h"

#include <yt/yt/ytlib/table_client/hunks.h>

namespace NYT {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

template <>
void Save(NTabletNode::TSaveContext& context, const THunkChunkRef& hunkChunkRef)
{
    using NYT::Save;

    Save(context, hunkChunkRef.ChunkId);
    Save(context, hunkChunkRef.ErasureCodec);
    Save(context, hunkChunkRef.HunkCount);
    Save(context, hunkChunkRef.TotalHunkLength);
    Save(context, hunkChunkRef.CompressionDictionaryId);
}

template <>
void Load<THunkChunkRef, NTabletNode::TLoadContext>(
    NTabletNode::TLoadContext& context,
    THunkChunkRef& hunkChunkRef)
{
    using NYT::Load;

    Load(context, hunkChunkRef.ChunkId);
    Load(context, hunkChunkRef.ErasureCodec);
    Load(context, hunkChunkRef.HunkCount);
    Load(context, hunkChunkRef.TotalHunkLength);
    if (context.GetVersion() >= NTabletNode::ETabletReign::HunkValueDictionaryCompression ||
        (context.GetVersion() >= NTabletNode::ETabletReign::HunkValueDictionaryCompression_23_2 &&
         context.GetVersion() < NTabletNode::ETabletReign::NoMountRevisionCheckInBulkInsert))
    {
        Load(context, hunkChunkRef.CompressionDictionaryId);
    }
}

template <>
void Save(NTabletNode::TSaveContext& context, const THunkChunksInfo& hunkChunkInfo)
{
    using NYT::Save;

    Save(context, hunkChunkInfo.CellId);
    Save(context, hunkChunkInfo.HunkTabletId);
    Save(context, hunkChunkInfo.MountRevision);
    Save(context, hunkChunkInfo.HunkChunkRefs);
}

template <>
void Load<THunkChunksInfo, NTabletNode::TLoadContext>(
    NTabletNode::TLoadContext& context,
    THunkChunksInfo& hunkChunkInfo)
{
    using NYT::Load;

    Load(context, hunkChunkInfo.CellId);
    Load(context, hunkChunkInfo.HunkTabletId);
    Load(context, hunkChunkInfo.MountRevision);
    Load(context, hunkChunkInfo.HunkChunkRefs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
