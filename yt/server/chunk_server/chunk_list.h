#pragma once

#include "public.h"
#include "chunk_tree.h"
#include "chunk_tree_statistics.h"

#include <yt/server/cell_master/public.h>

#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/indexed_vector.h>
#include <yt/core/misc/range.h>

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

    //! Chunk list kind: static, dynamic table root or tablet.
    DEFINE_BYVAL_RO_PROPERTY(EChunkListKind, Kind);

    using TChildToIndexMap = THashMap<TChunkTree*, int>;
    DEFINE_BYREF_RW_PROPERTY(TChildToIndexMap, ChildToIndex);

    struct TCumulativeStatisticsEntry
    {
        i64 RowCount;
        i64 ChunkCount;
        i64 DataSize;

        void Persist(NCellMaster::TPersistenceContext& context);
    };

    // The i-th value is equal to the sum of statistics for children 0..i
    // for all i in [0..Children.size() - 2]
    // NB: Cumulative statistics for the last child (which is equal to the total chunk list statistics)
    // is stored in #Statistics field.
    DEFINE_BYREF_RW_PROPERTY(std::vector<TCumulativeStatisticsEntry>, CumulativeStatistics);

    DEFINE_BYREF_RW_PROPERTY(TChunkTreeStatistics, Statistics);

    // Min key for sorted dynamic tablet chunk lists.
    DEFINE_BYVAL_RW_PROPERTY(NTableClient::TOwningKey, PivotKey);

    // Increases each time the list changes.
    // Enables optimistic locking during chunk tree traversing.
    DEFINE_BYVAL_RO_PROPERTY(int, Version);

public:
    explicit TChunkList(TChunkListId id);

    TChunkListDynamicData* GetDynamicData() const;

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

    void ValidateSealed();
    void ValidateUniqueAncestors();

    ui64 GetVisitMark() const;
    void SetVisitMark(ui64 value);
    static ui64 GenerateVisitMark();

    virtual int GetGCWeight() const override;

    void SetKind(EChunkListKind kind);

    bool IsOrdered() const;

private:
    TIndexedVector<TChunkList*> Parents_;
    TIndexedVector<TChunkOwnerBase*> TrunkOwningNodes_;
    TIndexedVector<TChunkOwnerBase*> BranchedOwningNodes_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer


#define CHUNK_LIST_INL_H_
#include "chunk_list-inl.h"
#undef CHUNK_LIST_INL_H_
