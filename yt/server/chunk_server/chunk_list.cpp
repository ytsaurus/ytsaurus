#include "stdafx.h"
#include "chunk_list.h"
#include "chunk_owner_base.h"
#include "helpers.h"

#include <core/actions/invoker.h>

#include <server/cell_master/serialize.h>

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
    ResetChunkListStatistics(this);
}

void TChunkList::IncrementVersion()
{
    ++Version_;
}

void TChunkList::Save(NCellMaster::TSaveContext& context) const
{
    TChunkTree::Save(context);
    TStagedObject::Save(context);

    using NYT::Save;
    Save(context, Children_);
    Save(context, Parents_);
    Save(context, OwningNodes_);
    Save(context, Statistics_);
    Save(context, RowCountSums_);
    Save(context, ChunkCountSums_);
    Save(context, DataSizeSums_);
    Save(context, RecordCountSums_);
}

void TChunkList::Load(NCellMaster::TLoadContext& context)
{
    TChunkTree::Load(context);
    // COMPAT(babenko)
    if (context.GetVersion() >= 100) {
        TStagedObject::Load(context);
    }

    using NYT::Load;
    Load(context, Children_);
    Load(context, Parents_);
    Load(context, OwningNodes_);
    Load(context, Statistics_);
    if (context.GetVersion() < 100) {
        Load(context, LegacySortedBy_);
    }
    Load(context, RowCountSums_);
    Load(context, ChunkCountSums_);
    Load(context, DataSizeSums_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 100) {
        Load(context, RecordCountSums_);
    }
}

ui64 TChunkList::GenerateVisitMark()
{
    static std::atomic<ui64> counter(0);
    return ++counter;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
