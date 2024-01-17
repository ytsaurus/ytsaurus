#include "hunk_chunk.h"

#include "serialize.h"

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/hydra/mutation_context.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NHydra;

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
    if (auto hunkChunkMiscExt = FindProtoExtension<NTableClient::NProto::THunkChunkMiscExt>(ChunkMeta_.extensions())) {
        HunkCount_ = hunkChunkMiscExt->hunk_count();
        TotalHunkLength_ = hunkChunkMiscExt->total_hunk_length();
    }
    if (auto miscExt = FindProtoExtension<NChunkClient::NProto::TMiscExt>(ChunkMeta_.extensions())) {
        CreationTime_ = TInstant::MicroSeconds(miscExt->creation_time());
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
    Save(context, LockingState_);
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
    if (context.GetVersion() >= ETabletReign::RestoreHunkLocks) {
        Load(context, LockingState_);
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
    return StoreRefCount_ == 0 &&
        PreparedStoreRefCount_ <= 0 &&
        !LockingState_.IsLocked() &&
        !AttachedCompressionDictionary_;
}

int THunkChunk::GetLockCount() const
{
    return LockingState_.GetLockCount();
}

////////////////////////////////////////////////////////////////////////////////

void THunkChunkIdFormatter::operator()(TStringBuilderBase* builder, const THunkChunkPtr& hunkChunk) const
{
    FormatValue(builder, hunkChunk->GetId(), TStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
