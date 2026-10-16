#include "node_id_allocator.h"

#include <util/generic/algorithm.h>

#include <util/system/yassert.h>

#include <utility>

namespace NYql {

////////////////////////////////////////////////////////////////////////////////

TNodeIdAllocator::TNodeIdAllocator(ui32 minNodeId, ui32 maxNodeId)
    : MinNodeId_(minNodeId)
    , MaxNodeId_(maxNodeId)
{ }

TNodeIdAllocator::TRestoreClaimResult TNodeIdAllocator::RestoreClaim(
    const TString& owner,
    const TVector<ui32>& nodeIds)
{
    Y_ABORT_UNLESS(!owner.empty());

    if (!CheckClaim(owner, nodeIds)) {
        return {};
    }

    TRestoreClaimResult result{
        .Valid = true,
    };
    for (auto nodeId : nodeIds) {
        const auto nodeIt = OwnersByNodeId_.find(nodeId);
        if (nodeIt != OwnersByNodeId_.end() &&
            AnyOf(nodeIt->second, [&] (const auto& existingOwner) {
                return existingOwner != owner;
            }))
        {
            result.ConflictingNodeIds.push_back(nodeId);
        }
    }

    AddClaim(owner, nodeIds);
    return result;
}

bool TNodeIdAllocator::Allocate(const TString& owner, i64 count, TVector<ui32>* result)
{
    Y_ABORT_UNLESS(!owner.empty());
    Y_ABORT_UNLESS(result);

    result->clear();
    if (count <= 0 || NodeIdsByOwner_.contains(owner)) {
        return false;
    }

    TVector<ui32> nodeIds;
    nodeIds.reserve(count);
    for (ui32 nodeId = MinNodeId_; nodeId < MaxNodeId_ && ssize(nodeIds) < count; ++nodeId) {
        if (!OwnersByNodeId_.contains(nodeId)) {
            nodeIds.push_back(nodeId);
        }
    }
    if (ssize(nodeIds) != count) {
        return false;
    }

    AddClaim(owner, nodeIds);
    *result = std::move(nodeIds);
    return true;
}

bool TNodeIdAllocator::Release(const TString& owner)
{
    Y_ABORT_UNLESS(!owner.empty());

    const auto ownerIt = NodeIdsByOwner_.find(owner);
    if (ownerIt == NodeIdsByOwner_.end()) {
        return false;
    }

    for (auto nodeId : ownerIt->second) {
        auto nodeIt = OwnersByNodeId_.find(nodeId);
        Y_ABORT_UNLESS(nodeIt != OwnersByNodeId_.end());
        const auto erasedCount = nodeIt->second.erase(owner);
        Y_ABORT_UNLESS(erasedCount == 1);
        if (nodeIt->second.empty()) {
            OwnersByNodeId_.erase(nodeIt);
        }
    }
    NodeIdsByOwner_.erase(ownerIt);
    return true;
}

i64 TNodeIdAllocator::GetClaimCount() const
{
    i64 result = 0;
    for (const auto& item : NodeIdsByOwner_) {
        result += ssize(item.second);
    }
    return result;
}

i64 TNodeIdAllocator::GetClaimedNodeIdCount() const
{
    return ssize(OwnersByNodeId_);
}

void TNodeIdAllocator::Clear()
{
    NodeIdsByOwner_.clear();
    OwnersByNodeId_.clear();
}

bool TNodeIdAllocator::CheckClaim(const TString& owner, const TVector<ui32>& nodeIds) const
{
    if (nodeIds.empty()) {
        return false;
    }

    TNodeIdSet uniqueNodeIds;
    uniqueNodeIds.reserve(nodeIds.size());
    for (auto nodeId : nodeIds) {
        if (!uniqueNodeIds.insert(nodeId).second) {
            return false;
        }
    }

    const auto ownerIt = NodeIdsByOwner_.find(owner);
    return ownerIt == NodeIdsByOwner_.end() || ownerIt->second == uniqueNodeIds;
}

void TNodeIdAllocator::AddClaim(const TString& owner, const TVector<ui32>& nodeIds)
{
    auto ownerResult = NodeIdsByOwner_.emplace(owner, TNodeIdSet(nodeIds.begin(), nodeIds.end()));
    if (!ownerResult.second) {
        // CheckClaim guarantees that an existing claim is identical.
        return;
    }

    for (auto nodeId : nodeIds) {
        OwnersByNodeId_[nodeId].insert(owner);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYql
