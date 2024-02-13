#pragma once

#include "public.h"
#include "locking_state.h"

#include <yt/yt/client/chunk_client/public.h>

#include <yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/property.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class THunkChunk
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::TChunkId, Id);
    DEFINE_BYVAL_RW_PROPERTY(EHunkChunkState, State, EHunkChunkState::Active);
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::NProto::TChunkMeta, ChunkMeta);
    DEFINE_BYVAL_RO_PROPERTY(i64, HunkCount);
    DEFINE_BYVAL_RO_PROPERTY(i64, TotalHunkLength);
    DEFINE_BYVAL_RW_PROPERTY(i64, ReferencedHunkCount);
    DEFINE_BYVAL_RW_PROPERTY(i64, ReferencedTotalHunkLength);
    // Includes references from prepared tablet stores updates.
    DEFINE_BYVAL_RW_PROPERTY(int, StoreRefCount);
    // COMPAT(aleksandra-zh)
    DEFINE_BYVAL_RW_PROPERTY(int, PreparedStoreRefCount);
    DEFINE_BYVAL_RW_PROPERTY(EHunkChunkSweepState, SweepState, EHunkChunkSweepState::None);
    DEFINE_BYVAL_RW_PROPERTY(bool, Committed);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, CreationTime);
    // Set to |true| for compression dictionaries that reside in tablet list of fresh dictionaries.
    // Such hunk chunks will not be sweeped. Transient.
    DEFINE_BYVAL_RW_PROPERTY(bool, AttachedCompressionDictionary);

public:
    THunkChunk(
        NChunkClient::TChunkId id,
        const NTabletNode::NProto::TAddHunkChunkDescriptor* descriptor);

    int GetLockCount() const;

    void Initialize();

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    void Lock(TTransactionId transactionId, EObjectLockMode lockMode);
    void Unlock(TTransactionId transactionId, EObjectLockMode lockMode);

    //! Returns |true| iff store ref count and prepared store ref count are both zero
    //! and there are no locks and this is not an attached compression dictionary.
    bool IsDangling() const;

private:
    TLockingState LockingState_;
};

DEFINE_REFCOUNTED_TYPE(THunkChunk)

////////////////////////////////////////////////////////////////////////////////

struct THunkChunkIdFormatter
{
    void operator()(TStringBuilderBase* builder, const THunkChunkPtr& hunkChunk) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
