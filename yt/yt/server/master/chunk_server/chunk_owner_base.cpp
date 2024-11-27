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

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

// TODO(cherepashka): remove after corresponding compat in 25.1 will be removed.
DEFINE_ENUM(ECompatUpdateMode,
    ((None)                     (0))
    ((Append)                   (1))
    ((Overwrite)                (2))
);

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ChunkServerLogger;

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
    if (!IsTrunk()) {
        Save(context, DeltaStatistics());
    } else {
        YT_LOG_ALERT_IF(DeltaStatistics() != TChunkOwnerDataStatistics(),
            "Trunk node has non empty delta statistics, which will be lost during snapshot save "
            "(ChunkOwnerNodeId: %v, DeltaStatistics: %v)",
            GetVersionedId(),
            DeltaStatistics());
    }
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
    // COMPAT(cherepashka)
    if (context.GetVersion() >= EMasterReign::EnumsAndChunkReplicationReductionsInTTableNode) {
        Load(context, UpdateMode_);
    } else {
        UpdateMode_ = CheckedEnumCast<EUpdateMode>(Load<ECompatUpdateMode>(context));
    }
    Load(context, Replication_);
    Load(context, PrimaryMediumIndex_);

    std::optional<TChunkOwnerDataStatistics> deltaStatistics;
    // COMPAT(cherepashka)
    if (context.GetVersion() >= EMasterReign::SerializationOfDataStatistics) {
        Load(context, SnapshotStatistics_);
        // COMPAT(cherepashka)
        if (context.GetVersion() >= EMasterReign::DeltaStatisticsPointer) {
            if (!IsTrunk()) {
                deltaStatistics = Load<TChunkOwnerDataStatistics>(context);
            }
        } else {
            if (!IsTrunk()) {
                deltaStatistics = Load<TChunkOwnerDataStatistics>(context);
            } else {
                Load<TChunkOwnerDataStatistics>(context);
            }
        }
    } else {
        SnapshotStatistics_ = FromProto<TChunkOwnerDataStatistics>(Load<NChunkClient::NProto::TDataStatistics>(context));
        deltaStatistics = FromProto<TChunkOwnerDataStatistics>(Load<NChunkClient::NProto::TDataStatistics>(context));
    }
    if (deltaStatistics) {
        *MutableDeltaStatistics() = *deltaStatistics;
    }

    Load(context, CompressionCodec_);
    Load(context, ErasureCodec_);
    Load(context, EnableStripedErasure_);
    Load(context, SnapshotSecurityTags_);
    Load(context, DeltaSecurityTags_);
    // COMPAT(cherepashka)
    if (context.GetVersion() >= EMasterReign::EnumsAndChunkReplicationReductionsInTTableNode) {
        Load(context, ChunkMergerMode_);
    } else {
        auto compatChunkMergerMode = Load<TVersionedBuiltinAttribute<ECompatChunkMergerMode>>(context);
        if (compatChunkMergerMode.IsNull()) {
            ChunkMergerMode_.Reset();
        } else if (compatChunkMergerMode.IsTombstoned()) {
            ChunkMergerMode_.Remove();
        } else if (compatChunkMergerMode.IsSet()) {
            auto chunkMergerMode = compatChunkMergerMode.ToOptional();
            YT_VERIFY(chunkMergerMode);
            ChunkMergerMode_.Set(CheckedEnumCast<EChunkMergerMode>(*chunkMergerMode));
        }
    }
    Load(context, EnableSkynetSharing_);
    Load(context, UpdatedSinceLastMerge_);
    Load(context, ChunkMergerTraversalInfo_);

    // COMPAT(kivedernikov)
    if (context.GetVersion() >= EMasterReign::HunkMedia) {
        Load(context, HunkReplication_);
        Load(context, HunkPrimaryMediumIndex_);

        // COMPAT(kazachonok)
        auto optionalHunkPrimaryMediumIndex = GetHunkPrimaryMediumIndex();
        if (context.GetVersion() < EMasterReign::OptionalHunkPrimaryMedium
            && optionalHunkPrimaryMediumIndex
            && *optionalHunkPrimaryMediumIndex == PrimaryMediumIndex_)
        {
            RemoveHunkPrimaryMediumIndex();
        }
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

const TChunkOwnerDataStatistics& TChunkOwnerBase::DeltaStatistics() const
{
    static const TChunkOwnerDataStatistics Empty;
    return DeltaStatistics_ ? *DeltaStatistics_ : Empty;
}

TChunkOwnerDataStatistics* TChunkOwnerBase::MutableDeltaStatistics()
{
    if (!DeltaStatistics_) {
        DeltaStatistics_ = std::make_unique<TChunkOwnerDataStatistics>();
    }
    return DeltaStatistics_.get();
}

TSecurityTags TChunkOwnerBase::ComputeSecurityTags() const
{
    return *SnapshotSecurityTags_ + *DeltaSecurityTags_;
}

std::optional<int> TChunkOwnerBase::GetHunkPrimaryMediumIndex() const
{
    return HunkPrimaryMediumIndex_ == GenericMediumIndex
        ? std::nullopt
        : std::make_optional(HunkPrimaryMediumIndex_);
}

void TChunkOwnerBase::SetHunkPrimaryMediumIndex(std::optional<int> hunkPrimaryMediumIndex)
{
    if (hunkPrimaryMediumIndex) {
        HunkPrimaryMediumIndex_ = *hunkPrimaryMediumIndex;
    } else {
        RemoveHunkPrimaryMediumIndex();
    }
}

void TChunkOwnerBase::RemoveHunkPrimaryMediumIndex()
{
    HunkPrimaryMediumIndex_ = GenericMediumIndex;
}

int TChunkOwnerBase::GetEffectiveHunkPrimaryMediumIndex() const
{
    return HunkPrimaryMediumIndex_ == GenericMediumIndex
        ? PrimaryMediumIndex_
        : HunkPrimaryMediumIndex_;
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

    std::optional<TChunkOwnerDataStatistics> updateStatistics;
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
                *MutableDeltaStatistics() = *context.Statistics;
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

TChunkOwnerDataStatistics TChunkOwnerBase::ComputeTotalStatistics() const
{
    return SnapshotStatistics_ + DeltaStatistics();
}

TChunkOwnerDataStatistics TChunkOwnerBase::ComputeUpdateStatistics() const
{
    YT_VERIFY(!IsExternal());

    TChunkOwnerDataStatistics updateStatistics;
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
    return SnapshotStatistics_.IsDataWeightValid() && DeltaStatistics().IsDataWeightValid();
}

TClusterResources TChunkOwnerBase::GetTotalResourceUsage() const
{
    return TCypressNode::GetTotalResourceUsage() + GetDiskUsage(ComputeTotalStatistics());
}

TClusterResources TChunkOwnerBase::GetDeltaResourceUsage() const
{
    TChunkOwnerDataStatistics statistics;
    if (IsTrunk()) {
        statistics = DeltaStatistics() + SnapshotStatistics_;
    } else {
        switch (UpdateMode_) {
            case EUpdateMode::Append:
                statistics = DeltaStatistics();
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

TClusterResources TChunkOwnerBase::GetDiskUsage(const TChunkOwnerDataStatistics& statistics) const
{
    TClusterResources result;
    for (const auto& entry : Replication()) {
        result.SetMediumDiskSpace(
            entry.GetMediumIndex(),
            CalculateDiskSpaceUsage(
                entry.Policy().GetReplicationFactor(),
                statistics.RegularDiskSpace,
                statistics.ErasureDiskSpace));
    }
    result.SetChunkCount(statistics.ChunkCount);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
