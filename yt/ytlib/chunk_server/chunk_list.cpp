#include "stdafx.h"
#include "chunk_list.h"

namespace NYT {
namespace NChunkServer {

using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TChunkList::TChunkList(const TChunkListId& id)
    : TObjectWithIdBase(id)
{ }

TChunkList::TChunkList(const TChunkList& other)
    : TObjectWithIdBase(other)
    , ChildrenIds_(other.ChildrenIds_)
{ }

TAutoPtr<TChunkList> TChunkList::Clone() const
{
    return new TChunkList(*this);
}

void TChunkList::Save(TOutputStream* output) const
{
    TObjectWithIdBase::Save(output);
    ::Save(output, ChildrenIds_);
}

TAutoPtr<TChunkList> TChunkList::Load(const TChunkListId& id, TInputStream* input)
{
    TAutoPtr<TChunkList> chunkList = new TChunkList(id);
    chunkList->TObjectWithIdBase::Load(input);
    ::Load(input, chunkList->ChildrenIds_);
    return chunkList;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
