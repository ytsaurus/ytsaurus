#pragma once

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {

class TNodeIdAllocator {
public:
    struct TRestoreClaimResult {
        bool Valid = false;
        TVector<ui32> ConflictingNodeIds;
    };

    TNodeIdAllocator(ui32 minNodeId, ui32 maxNodeId);

    // Restored claims may be outside the allocation range.
    TRestoreClaimResult RestoreClaim(const TString& owner, const TVector<ui32>& nodeIds);
    bool Allocate(const TString& owner, i64 count, TVector<ui32>* result);
    bool Release(const TString& owner);

    // Counts (owner, node ID) pairs.
    i64 GetClaimCount() const;
    // Counts distinct claimed node IDs.
    i64 GetClaimedNodeIdCount() const;

    void Clear();

private:
    using TNodeIdSet = THashSet<ui32>;
    using TOwnerSet = THashSet<TString>;

    const ui32 MinNodeId_;
    const ui32 MaxNodeId_;

    THashMap<TString, TNodeIdSet> NodeIdsByOwner_;
    THashMap<ui32, TOwnerSet> OwnersByNodeId_;

    bool CheckClaim(const TString& owner, const TVector<ui32>& nodeIds) const;
    void AddClaim(const TString& owner, const TVector<ui32>& nodeIds);
};

} // namespace NYql
