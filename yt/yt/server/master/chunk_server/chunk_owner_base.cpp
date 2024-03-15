#include "chunk_owner_base.h"
#include "chunk_list.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/object_server/object_manager.h>

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
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkOwnerBase::TCommonUploadContext::TCommonUploadContext(TBootstrap* bootstrap)
    : Bootstrap(bootstrap)
{ }

////////////////////////////////////////////////////////////////////////////////

TChunkOwnerBase::TBeginUploadContext::TBeginUploadContext(TBootstrap* bootstrap)
    : TCommonUploadContext(bootstrap)
{ }

////////////////////////////////////////////////////////////////////////////////

// COMPAT(h0pless): remove this when clients will send table schema options during begin upload.
TChunkOwnerBase::TEndUploadContext::TEndUploadContext(TBootstrap* bootstrap)
    : TCommonUploadContext(bootstrap)
{ }

////////////////////////////////////////////////////////////////////////////////

TChunkOwnerBase::TChunkOwnerBase(TVersionedNodeId id)
    : TCypressNode(id)
{
    Replication_.SetVital(true);
    HunkReplication_.SetVital(true);
    if (IsTrunk()) {
        SetCompressionCodec(NCompression::ECodec::None);
        SetErasureCodec(NErasure::ECodec::None);
        SetEnableStripedErasure(false);
        SetEnableSkynetSharing(false);
        SetChunkMergerMode(NChunkClient::EChunkMergerMode::None);
    }
}

void TChunkOwnerBase::Save(NCellMaster::TSaveContext& context) const
{
    TCypressNode::Save(context);

    using NYT::Save;
    Save(context, ChunkLists_);
    Save(context, UpdateMode_);
    Save(context, Replication_);
    Save(context, PrimaryMediumIndex_);
    Save(context, SnapshotStatistics_);
    Save(context, DeltaStatistics_);
    Save(context, CompressionCodec_);
    Save(context, ErasureCodec_);
    Save(context, EnableStripedErasure_);
    Save(context, SnapshotSecurityTags_);
    Save(context, DeltaSecurityTags_);
    Save(context, ChunkMergerMode_);
    Save(context, EnableSkynetSharing_);
    Save(context, UpdatedSinceLastMerge_);
    Save(context, ChunkMergerTraversalInfo_);
    Save(context, HunkReplication_);
    Save(context, HunkPrimaryMediumIndex_);
}

void TChunkOwnerBase::Load(NCellMaster::TLoadContext& context)
{
    TCypressNode::Load(context);

    using NYT::Load;

    Load(context, ChunkLists_);
    Load(context, UpdateMode_);
    Load(context, Replication_);
    Load(context, PrimaryMediumIndex_);
    Load(context, SnapshotStatistics_);
    Load(context, DeltaStatistics_);
    Load(context, CompressionCodec_);
    Load(context, ErasureCodec_);
    Load(context, EnableStripedErasure_);
    Load(context, SnapshotSecurityTags_);
    Load(context, DeltaSecurityTags_);

    // COMPAT(cherepashka)
    if (context.GetVersion() >= EMasterReign::ChunkMergerModeUnderTransaction) {
        Load(context, ChunkMergerMode_);
    } else {
        auto chunkMergerMode = Load<NChunkClient::EChunkMergerMode>(context);
        SetChunkMergerMode(chunkMergerMode);
    }
    Load(context, EnableSkynetSharing_);
    Load(context, UpdatedSinceLastMerge_);
    Load(context, ChunkMergerTraversalInfo_);

    // COMPAT(kivedernikov)
    if (context.GetVersion() >= EMasterReign::HunkMedia) {
        Load(context, HunkReplication_);
        Load(context, HunkPrimaryMediumIndex_);
    }
}

TChunkList* TChunkOwnerBase::GetChunkList() const
{
    return GetChunkList(EChunkListContentType::Main);
}

void TChunkOwnerBase::SetChunkList(TChunkList* chunkList)
{
    SetChunkList(EChunkListContentType::Main, chunkList);
}

TChunkList* TChunkOwnerBase::GetHunkChunkList() const
{
    return GetChunkList(EChunkListContentType::Hunk);
}

void TChunkOwnerBase::SetHunkChunkList(TChunkList* chunkList)
{
    SetChunkList(EChunkListContentType::Hunk, chunkList);
}

void TChunkOwnerBase::SetChunkList(EChunkListContentType type, TChunkList* chunkList)
{
    ChunkLists_[type].Assign(chunkList);
}

TChunkLists TChunkOwnerBase::GetChunkLists() const
{
    TChunkLists chunkLists;
    for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
        chunkLists[contentType] = GetChunkList(contentType);
    }

    return chunkLists;
}

TChunkList* TChunkOwnerBase::GetChunkList(EChunkListContentType type) const
{
    return ChunkLists_[type].Get();
}

const TChunkList* TChunkOwnerBase::GetSnapshotChunkList() const
{
    return GetSnapshotChunkList(EChunkListContentType::Main);
}

const TChunkList* TChunkOwnerBase::GetSnapshotHunkChunkList() const
{
    return GetSnapshotChunkList(EChunkListContentType::Hunk);
}

const TChunkList* TChunkOwnerBase::GetSnapshotChunkList(EChunkListContentType type) const
{
    const auto* chunkList = GetChunkList(type);
    if (!chunkList) {
        return nullptr;
    }

    switch (UpdateMode_) {
        case EUpdateMode::None:
        case EUpdateMode::Overwrite:
            return chunkList;

        case EUpdateMode::Append:
            if (GetType() == EObjectType::Journal) {
                return chunkList;
            } else {
                const auto& children = chunkList->Children();
                YT_VERIFY(children.size() == 2);
                return children[0]->AsChunkList();
            }

        default:
            YT_ABORT();
    }
}

const TChunkList* TChunkOwnerBase::GetDeltaChunkList() const
{
    const auto* chunkList = GetChunkList();
    if (!chunkList) {
        return nullptr;
    }

    switch (UpdateMode_) {
        case EUpdateMode::Append:
            if (GetType() == EObjectType::Journal) {
                return chunkList;
            } else {
                const auto& children = chunkList->Children();
                YT_VERIFY(children.size() == 2);
                return children[1]->AsChunkList();
            }

        case EUpdateMode::Overwrite:
            return chunkList;

        default:
            YT_ABORT();
    }
}

TSecurityTags TChunkOwnerBase::ComputeSecurityTags() const
{
    return *SnapshotSecurityTags_ + *DeltaSecurityTags_;
}

void TChunkOwnerBase::ParseCommonUploadContext(const TCommonUploadContext& /*context*/)
{ }

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
        YT_LOG_ALERT_IF(*context.Statistics != *updateStatistics,
            "Statistics mismatch detected while ending upload "
            "(ChunkOwnerNodeId: %v, ContextStatistics: %v, UpdateStatistics: %v)",
            GetVersionedId(),
            *context.Statistics,
            *updateStatistics);
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

    TDataStatistics updateStatistics;
    switch (UpdateMode_) {
        case EUpdateMode::Append:
            if (auto* chunkList = GetDeltaChunkList()) {
                updateStatistics = chunkList->Statistics().ToDataStatistics();
            }
            break;

        case EUpdateMode::Overwrite:
            for (auto contentType : TEnumTraits<EChunkListContentType>::GetDomainValues()) {
                if (auto* chunkList = GetSnapshotChunkList(contentType)) {
                    updateStatistics += chunkList->Statistics().ToDataStatistics();
                }
            }
            break;

        default:
            YT_ABORT();
    }

    return updateStatistics;
}

bool TChunkOwnerBase::HasDataWeight() const
{
    return !HasInvalidDataWeight(SnapshotStatistics_) && !HasInvalidDataWeight(DeltaStatistics_);
}

void TChunkOwnerBase::CheckInvariants(TBootstrap* bootstrap) const
{
    TCypressNode::CheckInvariants(bootstrap);

    YT_VERIFY(!IsTrunk() || !IsStatisticsFixNeeded());
}

bool TChunkOwnerBase::FixStatistics()
{
    if (IsStatisticsFixNeeded()) {
        DoFixStatistics();
        return true;
    }

    return false;
}

void TChunkOwnerBase::FixStatisticsAndAlert()
{
    if (!IsStatisticsFixNeeded()) {
        return;
    }

    YT_LOG_ALERT("Fixing chunk owner statistics (ChunkOwnerId: %v, SnapshotStatistics: %v, DeltaStatistics: %v)",
        GetId(),
        SnapshotStatistics_,
        DeltaStatistics_);

    DoFixStatistics();
}

bool TChunkOwnerBase::IsStatisticsFixNeeded() const
{
    YT_VERIFY(IsTrunk());

    return DeltaStatistics_ != TDataStatistics();
}

void TChunkOwnerBase::DoFixStatistics()
{
    // In this specific order.
    SnapshotStatistics_ = ComputeTotalStatistics();
    DeltaStatistics_ = TDataStatistics();
}

NSecurityServer::TClusterResources TChunkOwnerBase::GetTotalResourceUsage() const
{
    return TCypressNode::GetTotalResourceUsage() + GetDiskUsage(ComputeTotalStatistics());
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
    return TCypressNode::GetDeltaResourceUsage() + GetDiskUsage(statistics);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
