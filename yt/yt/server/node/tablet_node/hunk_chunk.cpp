#include "hunk_chunk.h"

#include "private.h"
#include "serialize.h"

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/hydra/mutation_context.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = TabletNodeLogger;

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
    Load(context, Committed_);
    Load(context, LockingState_);
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
        !IsAttachedCompressionDictionary();
}

int THunkChunk::GetLockCount() const
{
    return LockingState_.GetLockCount();
}

void THunkChunk::PopulateAddHunkChunkDescriptor(NProto::TAddHunkChunkDescriptor* descriptor) const
{
    if (IsAttachedCompressionDictionary()) {
        THROW_ERROR_EXCEPTION("Cannot replicate hunk chunk %v which is an attached compression dictionary",
            Id_);
    }

    if (PreparedStoreRefCount_ > 0) {
        YT_LOG_ALERT("Hunk chunk has nonzero ref count during replication "
            "(HunkChunkId: %v, PreparedStoreRefCount: %v)",
            Id_,
            PreparedStoreRefCount_);

        THROW_ERROR_EXCEPTION("Cannot replicate hunk chunk %v with nonzero prepared store ref count",
            Id_)
            << TErrorAttribute("prepared_store_ref_count", PreparedStoreRefCount_);
    }

    ToProto(descriptor->mutable_chunk_id(), Id_);
    ToProto(descriptor->mutable_chunk_meta(), ChunkMeta_);
}

////////////////////////////////////////////////////////////////////////////////

void THunkChunkIdFormatter::operator()(TStringBuilderBase* builder, const THunkChunkPtr& hunkChunk) const
{
    FormatValue(builder, hunkChunk->GetId(), TStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
