#include "stdafx.h"
#include "chunk_list.h"

#include <ytlib/actions/invoker.h>

#include <server/cell_master/load_context.h>

namespace NYT {
namespace NChunkServer {

using namespace NObjectServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TVersionedChunkListId::TVersionedChunkListId(const TChunkListId& id, int version)
    : Id(id)
    , Version(version)
{ }

////////////////////////////////////////////////////////////////////////////////

TChunkList::TChunkList(const TChunkListId& id)
    : TObjectWithIdBase(id)
    , Rigid_(false)
    , Version_(0)
{ }

void TChunkList::IncrementVersion()
{
    ++Version_;
}

TVersionedChunkListId TChunkList::GetVersionedId() const
{
    return TVersionedChunkListId(Id_, Version_);
}

void TChunkList::Save(const NCellMaster::TSaveContext& context) const
{
    TObjectWithIdBase::Save(context);
    
    auto* output = context.GetOutput();
    SaveObjectRefs(output, Children_);
    SaveObjectRefs(output, Parents_);
    SaveObjectRefs(output, OwningNodes_);
    ::Save(output, Statistics_);
    ::Save(output, SortedBy_);
    ::Save(output, Rigid_);
    ::Save(output, RowCountSums_);
}

void TChunkList::Load(const NCellMaster::TLoadContext& context)
{
    TObjectWithIdBase::Load(context);
    
    auto* input = context.GetInput();
    LoadObjectRefs(input, Children_, context);
    LoadObjectRefs(input, Parents_, context);
    LoadObjectRefs(input, OwningNodes_, context);
    ::Load(input, Statistics_);
    ::Load(input, SortedBy_);
    ::Load(input, Rigid_);
    ::Load(input, RowCountSums_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
