#include "stdafx.h"
#include "chunk_list.h"
#include "chunk_owner_base.h"

#include <core/actions/invoker.h>

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NChunkServer {

using namespace NObjectServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TChunkList::TChunkList(const TChunkListId& id)
    : TChunkTree(id)
    , Version_(0)
    , VisitMark_(0)
{
    Statistics_.ChunkListCount = 1;
    Statistics_.Rank = 1;
}

void TChunkList::IncrementVersion()
{
    ++Version_;
}

void TChunkList::Save(NCellMaster::TSaveContext& context) const
{
    TChunkTree::Save(context);

    using NYT::Save;
    SaveObjectRefs(context, Children_);
    SaveObjectRefs(context, Parents_);
    SaveObjectRefs(context, OwningNodes_);
    Save(context, Statistics_);
    Save(context, SortedBy_);
    Save(context, RowCountSums_);
    Save(context, ChunkCountSums_);
    Save(context, DataSizeSums_);
}

void TChunkList::Load(NCellMaster::TLoadContext& context)
{
    TChunkTree::Load(context);

    using NYT::Load;
    LoadObjectRefs(context, Children_);
    LoadObjectRefs(context, Parents_);
    LoadObjectRefs(context, OwningNodes_);
    Load(context, Statistics_);

    // COMPAT(ignat)
    if (context.GetVersion() < 46) {
        Statistics_.Rank = std::max(Statistics_.Rank, 1);
    }

    Load(context, SortedBy_);
    Load(context, RowCountSums_);
    Load(context, ChunkCountSums_);
    // COMPAT(psushin)
    if (context.GetVersion() > 10) {
        Load(context, DataSizeSums_);
    }
}

TAtomic TChunkList::GenerateVisitMark()
{
    static TAtomic result = 0;
    return AtomicIncrement(result);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
