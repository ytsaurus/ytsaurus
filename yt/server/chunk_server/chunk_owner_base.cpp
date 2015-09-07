#include "stdafx.h"
#include "chunk_list.h"
#include "chunk_owner_base.h"

#include <ytlib/chunk_client/data_statistics.h>

#include <server/cell_master/serialize.h>

#include <server/security_server/cluster_resources.h>

namespace NYT {
namespace NChunkServer {

using namespace NYTree;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NCypressClient;
using namespace NCypressClient::NProto;

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
    Save(context, SnapshotStatistics_);
    Save(context, DeltaStatistics_);
}

void TChunkOwnerBase::Load(NCellMaster::TLoadContext& context)
{
    TCypressNodeBase::Load(context);

    using NYT::Load;
    Load(context, ChunkList_);
    Load(context, UpdateMode_);
    Load(context, ReplicationFactor_);
    Load(context, Vital_);
    if (context.GetVersion() >= 200) {
        Load(context, SnapshotStatistics_);
        Load(context, DeltaStatistics_);
    }
}

const TChunkList* TChunkOwnerBase::GetSnapshotChunkList() const
{
    switch (UpdateMode_) {
        case EUpdateMode::None:
        case EUpdateMode::Overwrite:
            return ChunkList_;

        case EUpdateMode::Append:
            if (GetType() == EObjectType::Journal) {
                return ChunkList_;
            } else {
                const auto& children = ChunkList_->Children();
                YCHECK(children.size() == 2);
                return children[0]->AsChunkList();
            }

        default:
            YUNREACHABLE();
    }
}

const TChunkList* TChunkOwnerBase::GetDeltaChunkList() const
{
    switch (UpdateMode_) {
        case EUpdateMode::Append:
            if (GetType() == EObjectType::Journal) {
                return ChunkList_;
            } else {
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

void TChunkOwnerBase::BeginUpload(EUpdateMode mode)
{
    UpdateMode_ = mode;
}

void TChunkOwnerBase::EndUpload(
    const TDataStatistics* statistics,
    const std::vector<Stroka>& /*keyColumns*/)
{
    if (statistics) {
        switch (UpdateMode_) {
            case EUpdateMode::Append:
                YCHECK(IsExternal() || GetDeltaChunkList()->Statistics().ToDataStatistics() == *statistics);
                DeltaStatistics_ = *statistics;
                break;

            case EUpdateMode::Overwrite:
                YCHECK(IsExternal() || GetSnapshotChunkList()->Statistics().ToDataStatistics() == *statistics);
                SnapshotStatistics_ = *statistics;
                break;

            default:
                YUNREACHABLE();
        }
    }
}

bool TChunkOwnerBase::IsSorted() const
{
    return false;
}

ENodeType TChunkOwnerBase::GetNodeType() const
{
    return ENodeType::Entity;
}

TDataStatistics TChunkOwnerBase::ComputeTotalStatistics() const
{
    return SnapshotStatistics_ + DeltaStatistics_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
