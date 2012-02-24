#include "stdafx.h"
#include "chunk_list.h"

#include <ytlib/cell_master/load_context.h>

namespace NYT {
namespace NChunkServer {

using namespace NObjectServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TChunkList::TChunkList(const TChunkListId& id)
    : TObjectWithIdBase(id)
{ }

void TChunkList::Save(TOutputStream* output) const
{
    TObjectWithIdBase::Save(output);
    ::Save(output, ChildrenIds_);
    ::Save(output, ParentIds_);
    ::Save(output, Statistics_);
}

void TChunkList::Load(TInputStream* input, const TLoadContext& context)
{
    UNUSED(context);
    TObjectWithIdBase::Load(input);
    ::Load(input, ChildrenIds_);
    ::Load(input, ParentIds_);
    ::Load(input, Statistics_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
