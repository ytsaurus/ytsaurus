#include "chunk_owner_base.h"
#include "chunk_list.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/security_server/cluster_resources.h>
#include <yt/yt/server/master/security_server/security_tags.h>

#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NCrypto;
using namespace NYTree;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NCypressClient;
using namespace NCypressClient::NProto;
using namespace NCypressServer;
using namespace NSecurityServer;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TChunkOwnerBase::TEndUploadContext::TEndUploadContext(TBootstrap* bootstrap)
    : Bootstrap(bootstrap)
{ }

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkOwnerBase::TChunkOwnerBase(TVersionedNodeId id)
    : TCypressNode(id)
{
    Replication_.SetVital(true);
    if (IsTrunk()) {
        SetCompressionCodec(NCompression::ECodec::None);
        SetErasureCodec(NErasure::ECodec::None);
        SetEnableSkynetSharing(false);
    }
}

void TChunkOwnerBase::Save(NCellMaster::TSaveContext& context) const
{
    TCypressNode::Save(context);

    using NYT::Save;
    Save(context, ChunkList_);
    Save(context, UpdateMode_);
    Save(context, Replication_);
    Save(context, PrimaryMediumIndex_);
    Save(context, SnapshotStatistics_);
    Save(context, DeltaStatistics_);
    Save(context, CompressionCodec_);
    Save(context, ErasureCodec_);
    Save(context, SnapshotSecurityTags_);
    Save(context, DeltaSecurityTags_);
    Save(context, EnableChunkMerger_);
    Save(context, EnableSkynetSharing_);
}

void TChunkOwnerBase::Load(NCellMaster::TLoadContext& context)
{
    TCypressNode::Load(context);

    using NYT::Load;
    Load(context, ChunkList_);
    Load(context, UpdateMode_);
    Load(context, Replication_);
    Load(context, PrimaryMediumIndex_);
    Load(context, SnapshotStatistics_);
    Load(context, DeltaStatistics_);
    Load(context, CompressionCodec_);
    Load(context, ErasureCodec_);
    Load(context, SnapshotSecurityTags_);
    Load(context, DeltaSecurityTags_);

    // COMPAT(aleksandra-zh)
    if (context.GetVersion() >= EMasterReign::MasterMergeJobs) {
        Load(context, EnableChunkMerger_);
    }

    // COMPAT(aleksandra-zh)
    if (context.GetVersion() < EMasterReign::BuiltinEnableSkynetSharing) {
        const auto& enableSkynetSharingAttributeName = EInternedAttributeKey::EnableSkynetSharing.Unintern();
        if (auto enableSkynetSharing = FindAttribute(enableSkynetSharingAttributeName)) {
            auto value = std::move(*enableSkynetSharing);
            YT_VERIFY(Attributes_->Remove(enableSkynetSharingAttributeName));
            try {
                SetEnableSkynetSharing(ConvertTo<bool>(value));
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Cannot parse %Qv attribute (Value: %v, NodeId: %v)",
                    enableSkynetSharingAttributeName,
                    value,
                    GetId());
            }
        }
    } else {
        Load(context, EnableSkynetSharing_);
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
                YT_VERIFY(children.size() == 2);
                return children[0]->AsChunkList();
            }

        default:
            YT_ABORT();
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
                YT_VERIFY(children.size() == 2);
                return children[1]->AsChunkList();
            }

        case EUpdateMode::Overwrite:
            return ChunkList_;

        default:
            YT_ABORT();
    }
}

TSecurityTags TChunkOwnerBase::GetSecurityTags() const
{
    return *SnapshotSecurityTags_ + *DeltaSecurityTags_;
}

void TChunkOwnerBase::BeginUpload(const TBeginUploadContext& context)
{
    UpdateMode_ = context.Mode;
}

void TChunkOwnerBase::EndUpload(const TEndUploadContext& context)
{
    if (context.CompressionCodec) {
        SetCompressionCodec(*context.CompressionCodec);
    }

    if (context.ErasureCodec) {
        SetErasureCodec(*context.ErasureCodec);
    }

    std::optional<TDataStatistics> updateStatistics;
    if (!IsExternal() && GetChunkList()->HasAppendableCumulativeStatistics()) {
        updateStatistics = ComputeUpdateStatistics();
    }

    if (context.Statistics && updateStatistics) {
        YT_VERIFY(*context.Statistics == *updateStatistics);
    }

    switch (UpdateMode_) {
        case EUpdateMode::Append:
            if (context.Statistics) {
                DeltaStatistics_ = *context.Statistics;
            }
            DeltaSecurityTags_ = context.SecurityTags;
            break;

        case EUpdateMode::Overwrite:
            if (context.Statistics) {
                SnapshotStatistics_ = *context.Statistics;
            }
            SnapshotSecurityTags_ = context.SecurityTags;
            break;

        default:
            YT_ABORT();
    }
}

void TChunkOwnerBase::GetUploadParams(std::optional<TMD5Hasher>* /*md5Hasher*/)
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
    YT_VERIFY(!IsExternal());

    switch (UpdateMode_) {
        case EUpdateMode::Append:
            return GetDeltaChunkList()->Statistics().ToDataStatistics();

        case EUpdateMode::Overwrite:
            return GetSnapshotChunkList()->Statistics().ToDataStatistics();

        default:
            YT_ABORT();
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
    for (const auto& entry : Replication()) {
        result.SetMediumDiskSpace(
            entry.GetMediumIndex(),
            CalculateDiskSpaceUsage(
                entry.Policy().GetReplicationFactor(),
                statistics.regular_disk_space(),
                statistics.erasure_disk_space()));
    }
    result.SetChunkCount(statistics.chunk_count());
    return result;
}

void TChunkOwnerBase::MaybeResetObsoleteMergeJobCounter(NObjectServer::TEpoch epoch)
{
    if (epoch != MergeJobCounterEpoch_) {
        MergeJobCounter_ = 0;
        MergeJobCounterEpoch_ = epoch;
    }
}

void TChunkOwnerBase::IncrementMergeJobCounter(NObjectServer::TEpoch epoch, int value)
{
    MaybeResetObsoleteMergeJobCounter(epoch);
    MergeJobCounter_ += value;
    YT_LOG_ALERT_IF(MergeJobCounter_ < 0, "Negative merge job counter (NodeId: %v)", GetId());
}

int TChunkOwnerBase::GetMergeJobCounter(NObjectServer::TEpoch epoch)
{
    MaybeResetObsoleteMergeJobCounter(epoch);
    return MergeJobCounter_;
}

int TChunkOwnerBase::GetCurrentEpochMergeJobCounter()
{
    return MergeJobCounter_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
