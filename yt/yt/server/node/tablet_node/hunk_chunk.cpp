#include "hunk_chunk.h"

#include "serialize.h"

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

THunkChunk::THunkChunk(
    TChunkId id,
    const NTabletNode::NProto::TAddHunkChunkDescriptor* descriptor)
    : Id_(id)
    , LockingState_(id)
{
    if (descriptor) {
        ChunkMeta_ = descriptor->chunk_meta();
    }
}

void THunkChunk::Initialize()
{
    if (auto miscExt = FindProtoExtension<NTableClient::NProto::THunkChunkMiscExt>(ChunkMeta_.extensions())) {
        HunkCount_ = miscExt->hunk_count();
        TotalHunkLength_ = miscExt->total_hunk_length();
    }
}

void THunkChunk::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, State_);
    Save(context, ChunkMeta_);
    Save(context, ReferencedHunkCount_);
    Save(context, ReferencedTotalHunkLength_);
    Save(context, StoreRefCount_);
    Save(context, PreparedStoreRefCount_);
    Save(context, Committed_);
}

void THunkChunk::Load(TLoadContext& context)
{
    using NYT::Load;
    Load(context, State_);
    Load(context, ChunkMeta_);
    Load(context, ReferencedHunkCount_);
    Load(context, ReferencedTotalHunkLength_);
    Load(context, StoreRefCount_);
    Load(context, PreparedStoreRefCount_);
    if (context.GetVersion() >= ETabletReign::JournalHunksCommitted) {
        Load(context, Committed_);
    } else {
        Committed_ = true;
    }
}

void THunkChunk::Lock(TTransactionId transactionId, EObjectLockMode lockMode)
{
    LockingState_.Lock(transactionId, lockMode);
}

void THunkChunk::Unlock(TTransactionId transactionId, EObjectLockMode lockMode)
{
    LockingState_.Unlock(transactionId, lockMode);
}

bool THunkChunk::IsDangling() const
{
    return StoreRefCount_ == 0 && PreparedStoreRefCount_ == 0 && !LockingState_.IsLocked();
}

////////////////////////////////////////////////////////////////////////////////

void THunkChunkIdFormatter::operator()(TStringBuilderBase* builder, const THunkChunkPtr& hunkChunk) const
{
    FormatValue(builder, hunkChunk->GetId(), TStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
