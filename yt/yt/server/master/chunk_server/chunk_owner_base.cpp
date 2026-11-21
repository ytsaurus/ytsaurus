#include "chunk_owner_base.h"

#include "chunk_list.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/security_server/cluster_resources.h>
#include <yt/yt/server/master/security_server/security_tags.h>

#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/core/phoenix/type_def.h>

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

constinit const auto Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkOwnerBase::TBeginUploadContext::TBeginUploadContext(TBootstrap* bootstrap)
    : Bootstrap(bootstrap)
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

void TChunkOwnerBase::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TCypressNode>();

    PHOENIX_REGISTER_FIELD(1, ChunkLists_);
    PHOENIX_REGISTER_FIELD(2, UpdateMode_);
    // Initial migration is done in OnAfterSnapshotLoaded in CypressManager.
    PHOENIX_REGISTER_FIELD(3, SecurityTagsUpdateMode_,
        .SinceVersion(EMasterReign::FixSecurityTagsMessingWithChunkListStructure_25_4));
    PHOENIX_REGISTER_FIELD(4, Replication_);
    PHOENIX_REGISTER_FIELD(5, PrimaryMediumIndex_);
    PHOENIX_REGISTER_FIELD(6, SnapshotStatistics_);

    // NB: Delta statistics are only persisted for branched nodes.
    registrar.template VirtualField<7>("DeltaStatistics_", [] (TThis* this_, auto& context) {
        if (!this_->IsTrunk()) {
            NYT::Load(context, *this_->MutableDeltaStatistics());
        }
    }, [] (const TThis* this_, auto& context) {
        if (!this_->IsTrunk()) {
            NYT::Save(context, this_->DeltaStatistics());
        } else {
            YT_TLOG_ALERT_IF(
                this_->DeltaStatistics() != TChunkOwnerDataStatistics(),
                "Trunk node has non empty delta statistics, which will be lost during snapshot save")
                .With("ChunkOwnerNodeId", this_->GetVersionedId())
                .With("DeltaStatistics", this_->DeltaStatistics());
        }
    })();

    PHOENIX_REGISTER_FIELD(8, CompressionCodec_);
    PHOENIX_REGISTER_FIELD(9, ErasureCodec_);
    PHOENIX_REGISTER_FIELD(10, EnableStripedErasure_);
    PHOENIX_REGISTER_FIELD(11, SnapshotSecurityTags_);
    PHOENIX_REGISTER_FIELD(12, DeltaSecurityTags_);
    PHOENIX_REGISTER_FIELD(13, ChunkMergerMode_);
    PHOENIX_REGISTER_FIELD(14, EnableSkynetSharing_);

    registrar.template VirtualField<15>("ChunkMergerInfo_", [] (TThis* this_, auto& context) {
        if (context.GetVersion() >= EMasterReign::ChunkMergerInfo) {
            if (NYT::Load<bool>(context)) {
                NYT::Load(context, *this_->MutableChunkMergerInfo());
            }
        } else {
            // COMPAT(aleksandra-zh).
            auto updatedSinceLastMerge = NYT::Load<bool>(context);
            TChunkMergerTraversalInfo traversalInfo;
            NYT::Load(context, traversalInfo);
            if (updatedSinceLastMerge || traversalInfo.ChunkCount > 0) {
                auto* info = this_->MutableChunkMergerInfo();
                info->UpdatedSinceLastMerge = updatedSinceLastMerge;
                info->TraversalInfo = traversalInfo;
            }
        }
    }, [] (const TThis* this_, auto& context) {
        if (this_->ChunkMergerInfo_) {
            NYT::Save(context, true);
            NYT::Save(context, *this_->ChunkMergerInfo_);
        } else {
            NYT::Save(context, false);
        }
    })();

    PHOENIX_REGISTER_FIELD(16, HunkReplication_);
    PHOENIX_REGISTER_FIELD(17, HunkPrimaryMediumIndex_);

    registrar.AfterLoad([] (TThis* this_, auto& context) {
        // Check invariant: null hunk primary medium index <=> empty hunk replication.
        if (auto hunkPrimaryMediumIndex = this_->GetHunkPrimaryMediumIndex()) {
            if (this_->HunkReplication().GetSize() == 0) {
                YT_TLOG_ALERT("Chunk owner node with non-null hunk primary index yet empty hunk replication encountered")
                    .With("ChunkOwnerNodeId", this_->GetVersionedId())
                    .With("HunkPrimaryIndex", hunkPrimaryMediumIndex);
            } else if (!this_->HunkReplication().Get(*hunkPrimaryMediumIndex)) {
                YT_TLOG_ALERT("Chunk owner node with non-null hunk primary index yet zero hunk replication factor encountered")
                    .With("ChunkOwnerNodeId", this_->GetVersionedId())
                    .With("HunkPrimaryIndex", hunkPrimaryMediumIndex);
            }
        } else if (this_->HunkReplication().GetSize() != 0) {
            YT_TLOG_ALERT("Chunk owner node with null hunk primary index yet non-empty hunk replication encountered")
                .With("ChunkOwnerNodeId", this_->GetVersionedId())
                .With("HunkReplication", this_->HunkReplication());
        }

        // COMPAT(babenko)
        if (context.GetVersion() < EMasterReign::DropHasHunkChunkListUserAttribute && this_->GetAttributes()) {
            this_->GetMutableAttributes()->TryRemove("has_hunk_chunk_list");
        }
    });
}

PHOENIX_DEFINE_TYPE(TChunkOwnerBase);

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
            YT_VERIFY(type == EChunkListContentType::Main);
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

const TChunkList* TChunkOwnerBase::GetAppendDeltaChunkList() const
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

int TChunkOwnerBase::GetPrimaryMediumIndex() const
{
    return PrimaryMediumIndex_;
}

void TChunkOwnerBase::SetPrimaryMediumIndex(int primaryMediumIndex)
{
    PrimaryMediumIndex_ = primaryMediumIndex;
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
        ResetHunkPrimaryMediumIndex();
    }
}

void TChunkOwnerBase::ResetHunkPrimaryMediumIndex()
{
    HunkPrimaryMediumIndex_ = GenericMediumIndex;
}

int TChunkOwnerBase::GetEffectiveHunkPrimaryMediumIndex() const
{
    return HunkPrimaryMediumIndex_ == GenericMediumIndex
        ? PrimaryMediumIndex_
        : HunkPrimaryMediumIndex_;
}

const TChunkReplication& TChunkOwnerBase::EffectiveHunkReplication() const
{
    if (HunkPrimaryMediumIndex_ == GenericMediumIndex) {
        return Replication_;
    } else {
        YT_TLOG_ALERT_UNLESS(
            HunkReplication_.IsValid(),
            "Chunk owner node has invalid hunk replication despite having non-null hunk primary medium index")
            .With("ChunkOwnerNodeId", GetId())
            .With("HunkReplication", HunkReplication_)
            .With("HunkPrimaryMediumIndex", HunkPrimaryMediumIndex_);
        return HunkReplication_;
    }
}

void TChunkOwnerBase::ValidateBeginUpload(const TBeginUploadContext& /*context*/)
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
        YT_TLOG_ALERT_IF(*context.Statistics != *updateStatistics, "Statistics mismatch detected while ending upload")
            .With("ChunkOwnerNodeId", GetVersionedId())
            .With("ContextStatistics", *context.Statistics)
            .With("UpdateStatistics", *updateStatistics);
    }

    switch (UpdateMode_) {
        case EUpdateMode::Append:
            if (context.Statistics) {
                *MutableDeltaStatistics() = *context.Statistics;
            }

            if (context.SecurityTags && !context.SecurityTags->IsEmpty()) {
                DeltaSecurityTags_ = context.SecurityTags;
                SecurityTagsUpdateMode_ = ESecurityTagsUpdateMode::Append;
            }

            break;

        case EUpdateMode::Overwrite:
            if (context.Statistics) {
                SnapshotStatistics_ = *context.Statistics;
            }

            SnapshotSecurityTags_ = context.SecurityTags
                ? context.SecurityTags
                : TInternedSecurityTags{};
            DeltaSecurityTags_ = {};
            SecurityTagsUpdateMode_ = ESecurityTagsUpdateMode::Overwrite;

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
            if (auto* chunkList = GetAppendDeltaChunkList()) {
                updateStatistics = chunkList->Statistics().ToDataStatistics();
            }
            break;

        case EUpdateMode::Overwrite:
            updateStatistics += GetSnapshotChunkList()->Statistics().ToDataStatistics();
            if (auto* hunkChunkList = GetSnapshotHunkChunkList()) {
                updateStatistics += hunkChunkList->HunkStatistics().ToDataStatistics();
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

bool TChunkOwnerBase::HasChunkMergerInfo() const
{
    return ChunkMergerInfo_.operator bool();
}

const TChunkMergerInfo& TChunkOwnerBase::ChunkMergerInfo() const
{
    static const TChunkMergerInfo Empty;
    return ChunkMergerInfo_ ? *ChunkMergerInfo_ : Empty;
}

TChunkMergerInfo* TChunkOwnerBase::MutableChunkMergerInfo()
{
    if (!ChunkMergerInfo_) {
        ChunkMergerInfo_ = std::make_unique<TChunkMergerInfo>();
    }
    return ChunkMergerInfo_.get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
