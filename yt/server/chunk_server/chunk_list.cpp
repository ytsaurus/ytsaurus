#include "stdafx.h"
#include "chunk_list.h"

#include <ytlib/actions/invoker.h>

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
}

void TChunkList::IncrementVersion()
{
    ++Version_;
}

void TChunkList::Save(const NCellMaster::TSaveContext& context) const
{
    TChunkTree::Save(context);

    auto* output = context.GetOutput();
    SaveObjectRefs(context, Children_);
    SaveObjectRefs(context, Parents_);
    SaveObjectRefs(context, OwningNodes_);
    NChunkServer::Save(context, Statistics_);
    ::Save(output, SortedBy_);
    ::Save(output, RowCountSums_);
    ::Save(output, ChunkCountSums_);
}

void TChunkList::Load(const NCellMaster::TLoadContext& context)
{
    TChunkTree::Load(context);

    auto* input = context.GetInput();
    LoadObjectRefs(context, Children_);
    LoadObjectRefs(context, Parents_);
    LoadObjectRefs(context, OwningNodes_);
    NChunkServer::Load(context, Statistics_);
    ::Load(input, SortedBy_);
    ::Load(input, RowCountSums_);
    // COMPAT(ignat)
    if (context.GetVersion() >= 8) {
        ::Load(input, ChunkCountSums_);
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
