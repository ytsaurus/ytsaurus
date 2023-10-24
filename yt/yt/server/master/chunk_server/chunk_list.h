#pragma once

#include "public.h"
#include "chunk_tree.h"
#include "chunk_tree_statistics.h"
#include "cumulative_statistics.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/client/table_client/key_bound.h>

#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/ref_tracked.h>
#include <yt/yt/core/misc/indexed_vector.h>
#include <yt/yt/core/misc/range.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkListDynamicData
    : public NObjectServer::TObjectDynamicData
{
    //! Used to mark visited chunk lists with "unique" marks.
    ui64 VisitMark = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkList
    : public TChunkTree
    , public TRefTracked<TChunkList>
{
public:
    //! This many starting children are null.
    DEFINE_BYVAL_RW_PROPERTY(int, TrimmedChildCount);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TChunkTree*>, Children);

    //! Chunk list kind: static, dynamic table root, tablet etc.
    DEFINE_BYVAL_RO_PROPERTY(EChunkListKind, Kind);

    using TChildToIndexMap = THashMap<TChunkTree*, int>;
    DEFINE_BYREF_RW_PROPERTY(TChildToIndexMap, ChildToIndex);

    //! The i-th value is equal to the sum of statistics for children 0..i
    //! for all i in [0..Children.size() - 1]
    DEFINE_BYREF_RW_PROPERTY(TCumulativeStatistics, CumulativeStatistics);

    DEFINE_BYREF_RW_PROPERTY(TChunkTreeStatistics, Statistics);

    //! Min key for sorted dynamic tablet chunk lists.
    DEFINE_BYVAL_RW_PROPERTY(NTableClient::TLegacyOwningKey, PivotKey);

    //! Increases each time the list changes.
    //! Enables optimistic locking during chunk tree traversing.
    DEFINE_BYVAL_RO_PROPERTY(int, Version);

public:
    explicit TChunkList(TChunkListId id);

    TChunkListDynamicData* GetDynamicData() const;

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;
    TString GetObjectPath() const override;

    void CheckInvariants(NCellMaster::TBootstrap* bootstrap) const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    TRange<TChunkList*> Parents() const;
    void AddParent(TChunkList* parent);
    void RemoveParent(TChunkList* parent);

    TRange<TChunkOwnerBase*> TrunkOwningNodes() const;
    TRange<TChunkOwnerBase*> BranchedOwningNodes() const;
    void AddOwningNode(TChunkOwnerBase* node);
    void RemoveOwningNode(TChunkOwnerBase* node);

    void IncrementVersion();

    void ValidateLastChunkSealed();
    void ValidateUniqueAncestors();

    ui64 GetVisitMark() const;
    void SetVisitMark(ui64 value);
    static ui64 GenerateVisitMark();

    int GetGCWeight() const override;

    void SetKind(EChunkListKind kind);

    bool IsSealed() const;

    bool HasCumulativeStatistics() const;
    bool HasAppendableCumulativeStatistics() const;
    bool HasModifiableCumulativeStatistics() const;
    bool HasTrimmableCumulativeStatistics() const;
    bool HasChildToIndexMapping() const;

    //! Checks if the chunk list already contains #child.
    //! Only supported for chunk lists with child to index mapping (\see #HasChildToIndexMapping).
    bool HasChild(TChunkTree* child) const;

    NTableClient::TKeyBound GetPivotKeyBound() const;

private:
    TIndexedVector<TChunkList*> Parents_;
    TIndexedVector<TChunkOwnerBase*> TrunkOwningNodes_;
    TIndexedVector<TChunkOwnerBase*> BranchedOwningNodes_;
};

DEFINE_MASTER_OBJECT_TYPE(TChunkList)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

#define CHUNK_LIST_INL_H_
#include "chunk_list-inl.h"
#undef CHUNK_LIST_INL_H_
