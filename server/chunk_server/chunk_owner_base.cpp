#include "chunk_owner_base.h"
#include "chunk_list.h"
#include "helpers.h"

#include <yt/server/cell_master/serialize.h>

#include <yt/server/security_server/cluster_resources.h>

#include <yt/ytlib/chunk_client/data_statistics.h>
#include <yt/ytlib/chunk_client/helpers.h>

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
{
    Replication_.SetVital(true);
    for (auto& policy : Replication_) {
        policy.Clear();
    }
    if (IsTrunk()) {
        CompressionCodec_.Set(NCompression::ECodec::None);
        ErasureCodec_.Set(NErasure::ECodec::None);
    }
}

void TChunkOwnerBase::Save(NCellMaster::TSaveContext& context) const
{
    TCypressNodeBase::Save(context);

    using NYT::Save;
    Save(context, ChunkList_);
    Save(context, UpdateMode_);
    Save(context, Replication_);
    Save(context, PrimaryMediumIndex_);
    Save(context, SnapshotStatistics_);
    Save(context, DeltaStatistics_);
    Save(context, CompressionCodec_);
    Save(context, ErasureCodec_);
}

void TChunkOwnerBase::Load(NCellMaster::TLoadContext& context)
{
    TCypressNodeBase::Load(context);

    using NYT::Load;
    Load(context, ChunkList_);
    Load(context, UpdateMode_);
    // COMPAT(shakurov)
    if (context.GetVersion() < 400) {
        PrimaryMediumIndex_ = DefaultStoreMediumIndex;
        Replication_[DefaultStoreMediumIndex].SetReplicationFactor(Load<int>(context));
        Replication_.SetVital(Load<bool>(context));
    } else {
        Load(context, Replication_);
        PrimaryMediumIndex_ = Load<int>(context);
    }
    // COMPAT(shakurov)
    if (context.GetVersion() < 700) {
        Load<bool>(context); // drop ChunkPropertiesUpdateNeeded_
    }
    Load(context, SnapshotStatistics_);
    Load(context, DeltaStatistics_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 601) {
        Load(context, CompressionCodec_);
        Load(context, ErasureCodec_);
    } else {
        if (Attributes_) {
            auto& attributes = Attributes_->Attributes();
            {
                static const TString compressionCodecAttributeName("compression_codec");
                auto it = attributes.find(compressionCodecAttributeName);
                if (it != attributes.end()) {
                    const auto& value = it->second;
                    try {
                        CompressionCodec_.Set(NYTree::ConvertTo<NCompression::ECodec>(value));
                    } catch (...) {
                    }
                    attributes.erase(it);
                }
            }
            {
                static const TString erasureCodecAttributeName("erasure_codec");
                auto it = attributes.find(erasureCodecAttributeName);
                if (it != attributes.end()) {
                    const auto& value = it->second;
                    try {
                        ErasureCodec_.Set(NYTree::ConvertTo<NErasure::ECodec>(value));
                    } catch (...) {
                    }
                    attributes.erase(it);
                }
            }
            if (Attributes_->Attributes().empty()) {
                Attributes_.reset();
            }
        }
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
            Y_UNREACHABLE();
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
            Y_UNREACHABLE();
    }
}

void TChunkOwnerBase::BeginUpload(EUpdateMode mode)
{
    UpdateMode_ = mode;
}

void TChunkOwnerBase::EndUpload(
    const TDataStatistics* statistics,
    const NTableClient::TTableSchema& /*schema*/,
    NTableClient::ETableSchemaMode /*schemaMode*/,
    TNullable<NTableClient::EOptimizeFor> /*optimizeFor*/,
    const TNullable<TMD5Hasher>& /*md5Hasher*/)
{
    TNullable<TDataStatistics> updateStatistics;

    if (!IsExternal()) {
        updateStatistics = ComputeUpdateStatistics();
    }

    if (statistics && updateStatistics) {
        YCHECK(*statistics == *updateStatistics);
    }

    if (statistics) {
        switch (UpdateMode_) {
            case EUpdateMode::Append:
                DeltaStatistics_ = *statistics;
                break;

            case EUpdateMode::Overwrite:
                SnapshotStatistics_ = *statistics;
                break;

            default:
                Y_UNREACHABLE();
        }
    }
}

void TChunkOwnerBase::GetUploadParams(TNullable<TMD5Hasher>* /*md5Hasher*/)
{ }

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

TDataStatistics TChunkOwnerBase::ComputeUpdateStatistics() const
{
    YCHECK(!IsExternal());

    switch (UpdateMode_) {
        case EUpdateMode::Append:
            return GetDeltaChunkList()->Statistics().ToDataStatistics();

        case EUpdateMode::Overwrite:
            return GetSnapshotChunkList()->Statistics().ToDataStatistics();

        default:
            Y_UNREACHABLE();
    }
}

bool TChunkOwnerBase::HasDataWeight() const
{
    return !HasInvalidDataWeight(SnapshotStatistics_) && !HasInvalidDataWeight(DeltaStatistics_);
}

NSecurityServer::TClusterResources TChunkOwnerBase::GetTotalResourceUsage() const
{
    return TBase::GetTotalResourceUsage() + GetDiskUsage(ComputeTotalStatistics());
}

NSecurityServer::TClusterResources TChunkOwnerBase::GetDeltaResourceUsage() const
{
    TDataStatistics statistics;
    if (IsTrunk()) {
        statistics = DeltaStatistics_ + SnapshotStatistics_;
    } else {
        switch (UpdateMode_) {
            case EUpdateMode::Append:
                statistics = DeltaStatistics_;
                break;
            case EUpdateMode::Overwrite:
                statistics = SnapshotStatistics_;
                break;
            default:
                break; // Leave statistics empty - this is a newly branched node.
        }
    }
    return TBase::GetDeltaResourceUsage() + GetDiskUsage(statistics);
}

NSecurityServer::TClusterResources TChunkOwnerBase::GetDiskUsage(const TDataStatistics& statistics) const
{
    NSecurityServer::TClusterResources result;
    for (auto mediumIndex = 0; mediumIndex < MaxMediumCount; ++mediumIndex) {
        result.DiskSpace[mediumIndex] = CalculateDiskSpaceUsage(
            Replication()[mediumIndex].GetReplicationFactor(),
            statistics.regular_disk_space(),
            statistics.erasure_disk_space());
    }
    result.ChunkCount = statistics.chunk_count();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
