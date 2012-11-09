#include "stdafx.h"
#include "chunk_list.h"

#include <ytlib/actions/invoker.h>

#include <server/cell_master/serialization_context.h>

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
    , VisitMark_(0)
{
    Statistics_.ChunkListCount = 1;
}

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
    NChunkServer::Save(Statistics_, context);
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
    NChunkServer::Load(Statistics_, context);
    ::Load(input, SortedBy_);
    ::Load(input, Rigid_);
    ::Load(input, RowCountSums_);
}

TAtomic TChunkList::GenerateVisitMark()
{
    static TAtomic result = 0;
    return AtomicIncrement(result);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
