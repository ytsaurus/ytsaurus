#include "chunk_tree.h"
#include "chunk.h"
#include "chunk_view.h"
#include "chunk_list.h"
#include "dynamic_store.h"
#include "helpers.h"

#include <yt/ytlib/tablet_client/helpers.h>

namespace NYT::NChunkServer {

using namespace NObjectServer;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

TChunkTree::TChunkTree(TChunkTreeId id)
    : TStagedObject(id)
{ }

TChunk* TChunkTree::AsChunk()
{
    YT_ASSERT(
        GetType() == EObjectType::Chunk ||
        GetType() == EObjectType::ErasureChunk ||
        GetType() == EObjectType::JournalChunk ||
        GetType() == EObjectType::ErasureJournalChunk);
    return As<TChunk>();
}

const TChunk* TChunkTree::AsChunk() const
{
    YT_ASSERT(
        GetType() == EObjectType::Chunk ||
        GetType() == EObjectType::ErasureChunk ||
        GetType() == EObjectType::JournalChunk ||
        GetType() == EObjectType::ErasureJournalChunk);
    return As<TChunk>();
}

TChunkView* TChunkTree::AsChunkView()
{
    YT_ASSERT(GetType() == EObjectType::ChunkView);
    return As<TChunkView>();
}

const TChunkView* TChunkTree::AsChunkView() const
{
    YT_ASSERT(GetType() == EObjectType::ChunkView);
    return As<TChunkView>();
}

TDynamicStore* TChunkTree::AsDynamicStore()
{
    YT_ASSERT(IsDynamicTabletStoreType(GetType()));
    return As<TDynamicStore>();
}

const TDynamicStore* TChunkTree::AsDynamicStore() const
{
    YT_ASSERT(IsDynamicTabletStoreType(GetType()));
    return As<TDynamicStore>();
}

TChunkList* TChunkTree::AsChunkList()
{
    YT_ASSERT(GetType() == EObjectType::ChunkList);
    return As<TChunkList>();
}

const TChunkList* TChunkTree::AsChunkList() const
{
    YT_ASSERT(GetType() == EObjectType::ChunkList);
    return As<TChunkList>();
}

void TChunkTree::Save(NCellMaster::TSaveContext& context) const
{
    TStagedObject::Save(context);
}

void TChunkTree::Load(NCellMaster::TLoadContext& context)
{
    TStagedObject::Load(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
