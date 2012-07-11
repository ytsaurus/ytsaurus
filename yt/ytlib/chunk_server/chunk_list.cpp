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
    , Sorted_(false)
    , Rigid_(false)
{ }

void TChunkList::Save(TOutputStream* output) const
{
    TObjectWithIdBase::Save(output);
    SaveObjectRefs(output, Children_);
    SaveObjectRefs(output, Parents_);
    SaveObjectRefs(output, OwningNodes_);
    ::Save(output, Statistics_);
    ::Save(output, Sorted_);
    ::Save(output, KeyColumns_);
    ::Save(output, Rigid_);
    ::Save(output, RowCountSums_);
}

void TChunkList::Load(const TLoadContext& context, TInputStream* input)
{
    UNUSED(context);
    TObjectWithIdBase::Load(input);
    LoadObjectRefs(input, Children_, context);
    LoadObjectRefs(input, Parents_, context);
    LoadObjectRefs(input, OwningNodes_, context);
    ::Load(input, Statistics_);
    ::Load(input, Sorted_);
    ::Load(input, KeyColumns_);
    ::Load(input, Rigid_);
    ::Load(input, RowCountSums_);
}

void TChunkList::SetSorted(const std::vector<Stroka>& keyColumns)
{
    KeyColumns_ = keyColumns;
    Sorted_ = true;
}

void TChunkList::ResetSorted()
{
    KeyColumns_.clear();
    Sorted_ = false;
}

void TChunkList::CopySortAttributesTo(TChunkList* other)
{
    other->KeyColumns_ = KeyColumns_;
    other->Sorted_ = Sorted_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
