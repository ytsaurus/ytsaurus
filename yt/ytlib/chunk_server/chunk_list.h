#pragma once

#include "public.h"
#include "chunk_tree_statistics.h"
#include "chunk_tree_ref.h"

#include <ytlib/cell_master/public.h>
#include <ytlib/misc/property.h>
#include <ytlib/object_server/object_detail.h>
#include <ytlib/cypress/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkList
    : public NObjectServer::TObjectWithIdBase
{
    DEFINE_BYREF_RW_PROPERTY(std::vector<TChunkTreeRef>, Children);
    DEFINE_BYREF_RW_PROPERTY(std::vector<i64>, RowCountSums);
    DEFINE_BYREF_RW_PROPERTY(yhash_multiset<TChunkList*>, Parents);
    DEFINE_BYREF_RW_PROPERTY(TChunkTreeStatistics, Statistics);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<NCypress::ICypressNode*>, OwningNodes);
    
    // This is a pessimistic estimate.
    // In particular, this flag is True for root chunk lists of sorted tables.
    // However other chunk lists in such a table may have it false.
    DEFINE_BYVAL_RW_PROPERTY(bool, Sorted);

    // Indicates if the subtree of this chunk list can be rebalanced.
    // Rebalancing affects the root, i.e. changes the set of children.
    // For some chunk lists (e.g. those corresponding to roots of branched tables)
    // such changes are not allowed since they would break the invariants.
    DEFINE_BYVAL_RW_PROPERTY(bool, RebalancingEnabled);

public:
    explicit TChunkList(const TChunkListId& id);

    void Save(TOutputStream* output) const;
    void Load(const NCellMaster::TLoadContext& context, TInputStream* input);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
