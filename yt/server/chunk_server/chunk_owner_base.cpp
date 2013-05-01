#include "stdafx.h"

#include "chunk_list.h"

#include "chunk_owner_base.h"

#include <server/cell_master/serialization_context.h>
#include <server/security_server/cluster_resources.h>

namespace NYT {
namespace NChunkServer {

using namespace NChunkClient;
using namespace NCypressClient;

////////////////////////////////////////////////////////////////////////////////

TChunkOwnerBase::TChunkOwnerBase(const TVersionedNodeId& id)
    : TCypressNodeBase(id)
    , ChunkList_(nullptr)
    , UpdateMode_(EUpdateMode::None)
    , ReplicationFactor_(0)
{ }

int TChunkOwnerBase::GetOwningReplicationFactor() const
{
    auto* trunkNode = TrunkNode_ == this ? this : dynamic_cast<TChunkOwnerBase*>(TrunkNode_);
    YCHECK(trunkNode);
    return trunkNode->GetReplicationFactor();
}

void TChunkOwnerBase::Save(const NCellMaster::TSaveContext& context) const
{
    TCypressNodeBase::Save(context);

    auto* output = context.GetOutput();
    SaveObjectRef(context, ChunkList_);
    ::Save(output, UpdateMode_);
    ::Save(output, ReplicationFactor_);
}

void TChunkOwnerBase::Load(const NCellMaster::TLoadContext& context)
{
    TCypressNodeBase::Load(context);

    auto* input = context.GetInput();
    LoadObjectRef(context, ChunkList_);
    ::Load(input, UpdateMode_);
    ::Load(input, ReplicationFactor_);
}

NSecurityServer::TClusterResources TChunkOwnerBase::GetResourceUsage() const
{
    const auto* chunkList = GetUsageChunkList();
    i64 diskSpace = chunkList ? chunkList->Statistics().DiskSpace * GetOwningReplicationFactor() : 0;
    return NSecurityServer::TClusterResources(diskSpace, 1);
}

const TChunkList* TChunkOwnerBase::GetUsageChunkList() const
{
    switch (UpdateMode_) {
        case EUpdateMode::None:
            if (Transaction_) {
                return nullptr;;
            }
            return ChunkList_;

        case EUpdateMode::Append: {
            const auto& children = ChunkList_->Children();
            YCHECK(children.size() == 2);
            return children[1]->AsChunkList();
        }

        case EUpdateMode::Overwrite:
            return ChunkList_;

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

}
}