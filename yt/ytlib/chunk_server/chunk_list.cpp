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
    ::Save(output, ChildrenIds_);
    ::Save(output, RefCounter);
}

TAutoPtr<TChunkList> TChunkList::Load(const TChunkListId& id, TInputStream* input)
{
    TAutoPtr<TChunkList> chunkList = new TChunkList(id);
    ::Load(input, chunkList->ChildrenIds_);
    ::Load(input, chunkList->RefCounter);
    return chunkList;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
