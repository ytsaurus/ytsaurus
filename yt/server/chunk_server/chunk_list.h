#pragma once

#include "public.h"
#include "chunk_tree.h"
#include "chunk_tree_statistics.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/ref_tracked.h>

#include <server/cell_master/public.h>

#include <server/object_server/object_detail.h>

#include <server/cypress_server/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkList
    : public TChunkTree
    , public TRefTracked<TChunkList>
{
    DEFINE_BYREF_RW_PROPERTY(std::vector<TChunkTree*>, Children);

    // Accumulated sums of children row counts.
    // The i-th value is equal to the sum of row counts of children 0..i
    // for all i in [0..Children.size() - 2]
    // Accumulated statistics for the last child (which is equal to the total chunk list statistics)
    // is stored in #Statistics field.
    DEFINE_BYREF_RW_PROPERTY(std::vector<i64>, RowCountSums);
    // Same as above but for chunk count sums.
    DEFINE_BYREF_RW_PROPERTY(std::vector<i64>, ChunkCountSums);
    DEFINE_BYREF_RW_PROPERTY(yhash_multiset<TChunkList*>, Parents);
    DEFINE_BYREF_RW_PROPERTY(TChunkTreeStatistics, Statistics);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<NCypressServer::TCypressNodeBase*>, OwningNodes);

    // A tuple of key columns. If empty then the chunk list is not sorted.
    DEFINE_BYREF_RW_PROPERTY(std::vector<Stroka>, SortedBy);

    // Increases each time the list changes.
    // Enables optimistic locking during chunk tree traversing.
    DEFINE_BYVAL_RO_PROPERTY(int, Version);

    // Used to mark visited chunk lists with "unique" marks.
    DEFINE_BYVAL_RW_PROPERTY(TAtomic, VisitMark);

    static TAtomic GenerateVisitMark();

public:
    explicit TChunkList(const TChunkListId& id);

    void Save(const NCellMaster::TSaveContext& context) const;
    void Load(const NCellMaster::TLoadContext& context);

    void IncrementVersion();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
