#pragma once

#include "public.h"
#include "chunk_replica.h"
#include "medium.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/node_tracker_server/data_center.h>
#include <yt/server/master/node_tracker_server/node_tracker.h>
#include <yt/server/master/node_tracker_server/node.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/small_set.h>

#include <util/generic/map.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TPlacementDomain
{
    // Here, nullptr signifies 'null' data center.
    const NNodeTrackerServer::TDataCenter* DataCenter;
    const TMedium* Medium;

    bool operator==(const TPlacementDomain& rhs) const;
    size_t GetHash() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

template <>
struct THash<NYT::NChunkServer::TPlacementDomain>
{
    size_t operator()(const NYT::NChunkServer::TPlacementDomain& domain) const
    {
        return domain.GetHash();
    }
};

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! An iterator-like device that merges (sorted) iterator ranges on the fly (i.e. lazily).
/*!
 *  The iterator is reusable and non-copyable to avoid unnecessary allocations.
 *
 *  NB: this may require some rewriting when ranges make it to the C++ standard.
 *  However, keep in mind that the number of allocations should be kept to the
 *  minimum here. This may be hard to achieve with the classic view-iterator approach.
 */
template <class T, class TCompare>
class TReusableMergeIterator
{
private:
    struct TRange
    {
        TRange(const T& begin, const T& end)
            : Begin(begin)
            , End(end)
        { }

        T Begin;
        T End;
    };

    struct TRangeCompare;

public:
    TReusableMergeIterator() = default;

    TReusableMergeIterator(const TReusableMergeIterator&) = delete;
    TReusableMergeIterator& operator=(const TReusableMergeIterator&) = delete;

    // U's begin and end should be T.
    template <class U>
    void AddRange(U&& range);

    void Reset();

    decltype(*std::declval<T>()) operator*();
    decltype(&*std::declval<T>()) operator->();
    bool IsValid();
    TReusableMergeIterator& operator++();

private:
    // The ranges are arranged (no pun intended) into a heap (by the their first elements).
    // The front of the heap holds the smallest range (according to TCompare).
    // Empty ranges are immediately removed.
    std::vector<TRange> Ranges_;
};

struct TFillFactorToNodeMapItemComparator
{
    TFillFactorToNodeMapItemComparator() = default;

    bool operator()(TFillFactorToNodeMap::const_reference a, TFillFactorToNodeMap::const_reference b)
    {
        return a.first < b.first;
    }
};

using TLoadFactorToNodeMapItemComparator = TFillFactorToNodeMapItemComparator;

////////////////////////////////////////////////////////////////////////////////

class TChunkPlacement
    : public TRefCounted
{
public:
    using TDataCenterSet = SmallSet<const NNodeTrackerServer::TDataCenter*, NNodeTrackerServer::TypicalInterDCEdgeCount>;

    TChunkPlacement(
        TChunkManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void OnNodeUnregistered(TNode* node);
    void OnNodeUpdated(TNode* node);
    void OnNodeDataCenterChanged(TNode* node, const NNodeTrackerServer::TDataCenter* oldDataCenter);
    void OnNodeDisposed(TNode* node);

    TNodeList AllocateWriteTargets(
        TMedium* medium,
        TChunk* chunk,
        int desiredCount,
        int minCount,
        std::optional<int> replicationFactorOverride,
        const TNodeList* forbiddenNodes,
        const std::optional<TString>& preferredHostName,
        NChunkClient::ESessionType sessionType);

    TNodeList AllocateWriteTargets(
        TMedium* medium,
        TChunk* chunk,
        int desiredCount,
        int minCount,
        std::optional<int> replicationFactorOverride,
        const TDataCenterSet& dataCenters,
        NChunkClient::ESessionType sessionType);

    TNode* GetRemovalTarget(TChunkPtrWithIndexes chunkWithIndexes);

    bool HasBalancingTargets(const TDataCenterSet& dataCenters, TMedium* medium, double maxFillFactor);

    std::vector<TChunkPtrWithIndexes> GetBalancingChunks(
        TMedium* medium,
        TNode* node,
        int replicaCount);

    TNode* AllocateBalancingTarget(
        TMedium* medium,
        TChunk* chunk,
        double maxFillFactor,
        const TDataCenterSet& dataCenters);

    int GetMaxReplicasPerRack(
        const TMedium* medium,
        const TChunk* chunk,
        std::optional<int> replicationFactorOverride = std::nullopt);
    int GetMaxReplicasPerRack(
        int mediumIndex,
        const TChunk* chunk,
        std::optional<int> replicationFactorOverride = std::nullopt);

private:
    class TTargetCollector;

    const TChunkManagerConfigPtr Config_;
    NCellMaster::TBootstrap* const Bootstrap_;

    TReusableMergeIterator<TFillFactorToNodeIterator, TFillFactorToNodeMapItemComparator> FillFactorToNodeIterator_;
    TReusableMergeIterator<TLoadFactorToNodeIterator, TLoadFactorToNodeMapItemComparator> LoadFactorToNodeIterator_;

    using TFillFactorToNodeMaps = THashMap<TPlacementDomain, TFillFactorToNodeMap>;
    using TLoadFactorToNodeMaps = THashMap<TPlacementDomain, TLoadFactorToNodeMap>;

    //! Nodes listed here must pass #IsValidBalancingTargetToInsert test.
    TFillFactorToNodeMaps DomainToFillFactorToNode_;
    //! Nodes listed here must pass #IsValidWriteTargetToInsert test.
    TLoadFactorToNodeMaps DomainToLoadFactorToNode_;

    void OnNodeRegistered(TNode* node);

    void InsertToFillFactorMaps(TNode* node);
    void RemoveFromFillFactorMaps(
        TNode* node,
        std::optional<const NNodeTrackerServer::TDataCenter*> overrideDataCenter = std::nullopt);

    void InsertToLoadFactorMaps(TNode* node);
    void RemoveFromLoadFactorMaps(
        TNode* node,
        std::optional<const NNodeTrackerServer::TDataCenter*> overrideDataCenter = std::nullopt);

    TNodeList GetWriteTargets(
        TMedium* medium,
        TChunk* chunk,
        int desiredCount,
        int minCount,
        bool forceRackAwareness,
        std::optional<int> replicationFactorOverride,
        const TDataCenterSet* dataCenters,
        const TNodeList* forbiddenNodes = nullptr,
        const std::optional<TString>& preferredHostName = std::nullopt);

    TNode* GetBalancingTarget(
        TMedium* medium,
        const TDataCenterSet* dataCenters,
        TChunk* chunk,
        double maxFillFactor);

    bool IsValidWriteTargetToInsert(TMedium* medium, TNode* node);
    bool IsValidWriteTargetToAllocate(TNode* node, TTargetCollector* collector, bool enableRackAwareness);
    bool IsValidWriteTargetCore(TNode* node);
    // Preferred nodes are special: they don't come from load-factor maps and
    // thus may not have been vetted by #IsValidWriteTargetToInsert. Thus,
    // additional checking of their media and DC is required.
    bool IsValidPreferredWriteTargetToAllocate(TNode* node, TMedium* medium, const TDataCenterSet* dataCenters);

    bool IsValidBalancingTargetToInsert(TMedium* medium, TNode* node);
    bool IsValidBalancingTargetToAllocate(TNode* node, TTargetCollector* collector, bool enableRackAwareness);
    bool IsValidBalancingTargetCore(TNode* node);

    bool IsValidRemovalTarget(TNode* node);

    void AddSessionHint(
        TNode* node,
        int mediumIndex,
        NChunkClient::ESessionType sessionType);

    void PrepareFillFactorIterator(const TDataCenterSet* dataCenters, const TMedium* medium);
    void PrepareLoadFactorIterator(const TDataCenterSet* dataCenters, const TMedium* medium);

    template <class T>
    void ForEachDataCenter(const TDataCenterSet* dataCenters, T callback);

    const TDynamicChunkManagerConfigPtr& GetDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TChunkPlacement)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

#define CHUNK_PLACEMENT_INL_H_
#include "chunk_placement-inl.h"
#undef CHUNK_PLACEMENT_INL_H_
