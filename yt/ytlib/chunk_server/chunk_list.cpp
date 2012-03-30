#include "stdafx.h"
#include "chunk_list.h"

#include <ytlib/actions/invoker.h>
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
    SaveObjects(output, Children_);
    SaveObjects(output, Parents_);
    ::Save(output, Statistics_);
}

void TChunkList::Load(const TLoadContext& context, TInputStream* input)
{
    UNUSED(context);
    TObjectWithIdBase::Load(input);
    LoadObjects(input, Children_, context);
    LoadObjects(input, Parents_, context);
    ::Load(input, Statistics_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
