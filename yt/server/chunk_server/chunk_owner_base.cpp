#include "stdafx.h"

#include "chunk_list.h"

#include "chunk_owner_base.h"

#include <server/cell_master/serialize.h>

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
    , Vital_(true)
{ }

void TChunkOwnerBase::Save(NCellMaster::TSaveContext& context) const
{
    TCypressNodeBase::Save(context);

    using NYT::Save;
    Save(context, ChunkList_);
    Save(context, UpdateMode_);
    Save(context, ReplicationFactor_);
    Save(context, Vital_);
}

void TChunkOwnerBase::Load(NCellMaster::TLoadContext& context)
{
    TCypressNodeBase::Load(context);

    using NYT::Load;
    Load(context, ChunkList_);
    Load(context, UpdateMode_);
    Load(context, ReplicationFactor_);
    Load(context, Vital_);
}

NSecurityServer::TClusterResources TChunkOwnerBase::GetResourceUsage() const
{
    const auto* chunkList = GetUsageChunkList();

    i64 diskSpace = 0;
    if (chunkList) {
        diskSpace =
            chunkList->Statistics().RegularDiskSpace * GetReplicationFactor() +
            chunkList->Statistics().ErasureDiskSpace;
    }

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

} // namespace NChunkServer
} // namespace NYT
