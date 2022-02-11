#include "chunk_tree.h"
#include "chunk.h"
#include "chunk_view.h"
#include "chunk_list.h"
#include "dynamic_store.h"

#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

namespace NYT::NChunkServer {

using namespace NObjectServer;
using namespace NTabletClient;
using namespace NCellMaster;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TChunk* TChunkTree::AsChunk()
{
    YT_ASSERT(IsPhysicalChunkType(GetType()));
    return As<TChunk>();
}

const TChunk* TChunkTree::AsChunk() const
{
    YT_ASSERT(IsPhysicalChunkType(GetType()));
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

bool TChunkTree::IsSealed() const
{
    auto type = GetType();
    if (IsPhysicalChunkType(type)) {
        return AsChunk()->IsSealed();
    } else if (type == EObjectType::ChunkList) {
        return AsChunkList()->IsSealed();
    } else {
        return true;
    }
}

bool TChunkTree::GetOverlayed() const
{
    auto type = GetType();
    if (IsPhysicalChunkType(type)) {
        return AsChunk()->GetOverlayed();
    } else {
        return false;
    }
}

void TChunkTree::CheckInvariants(TBootstrap* bootstrap) const
{
    TStagedObject::CheckInvariants(bootstrap);
}

void TChunkTree::Save(TSaveContext& context) const
{
    TStagedObject::Save(context);
}

void TChunkTree::Load(TLoadContext& context)
{
    TStagedObject::Load(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
