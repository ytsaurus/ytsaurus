#include "chunk_owner_base.h"
#include "chunk_list.h"

#include <yt/server/cell_master/serialize.h>

#include <yt/server/security_server/cluster_resources.h>

#include <yt/ytlib/chunk_client/data_statistics.h>

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
    , ChunkPropertiesUpdateNeeded_(false)
{
    Properties_.SetVital(true);
    for (auto& mediumProperties : Properties_) {
        mediumProperties.Clear();
    }
}

void TChunkOwnerBase::Save(NCellMaster::TSaveContext& context) const
{
    TCypressNodeBase::Save(context);

    using NYT::Save;
    Save(context, ChunkList_);
    Save(context, UpdateMode_);
    Save(context, Properties_);
    Save(context, PrimaryMediumIndex_);
    Save(context, ChunkPropertiesUpdateNeeded_);
    Save(context, SnapshotStatistics_);
    Save(context, DeltaStatistics_);
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
        Properties_[DefaultStoreMediumIndex].SetReplicationFactorOrThrow(Load<int>(context));
        Properties_.SetVital(Load<bool>(context));
    } else {
        Load(context, Properties_);
        PrimaryMediumIndex_ = Load<int>(context);
    }
    Load(context, ChunkPropertiesUpdateNeeded_);
    Load(context, SnapshotStatistics_);
    Load(context, DeltaStatistics_);
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
    NTableClient::ETableSchemaMode /*schemaMode*/)
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

int TChunkOwnerBase::GetReplicationFactor(int mediumIndex) const
{
    return Properties_[mediumIndex].GetReplicationFactor();
}

void TChunkOwnerBase::SetReplicationFactorOrThrow(int mediumIndex, int replicationFactor)
{
    auto properties = Properties_;
    properties[mediumIndex].SetReplicationFactorOrThrow(replicationFactor);

    ValidateMedia(properties, PrimaryMediumIndex_);

    Properties_ = properties;
}

bool TChunkOwnerBase::GetDataPartsOnly(int mediumIndex) const
{
    return Properties_[mediumIndex].GetDataPartsOnly();
}

void TChunkOwnerBase::SetDataPartsOnlyOrThrow(int mediumIndex, bool dataPartsOnly)
{
    auto properties = Properties_;
    properties[mediumIndex].SetDataPartsOnly(dataPartsOnly);

    ValidateMedia(properties, PrimaryMediumIndex_);

    Properties_ = properties;
}

bool TChunkOwnerBase::GetVital() const
{
    return Properties_.GetVital();
}

void TChunkOwnerBase::SetVital(bool vital)
{
    Properties_.SetVital(vital);
}

void TChunkOwnerBase::SetPrimaryMediumIndexOrThrow(int mediumIndex)
{
    ValidateMedia(Properties_, mediumIndex);
    PrimaryMediumIndex_ = mediumIndex;
}

int TChunkOwnerBase::GetPrimaryMediumReplicationFactor() const
{
    return GetReplicationFactor(GetPrimaryMediumIndex());
}

void TChunkOwnerBase::SetPrimaryMediumReplicationFactorOrThrow(int replicationFactor)
{
    SetReplicationFactorOrThrow(
        GetPrimaryMediumIndex(),
        replicationFactor);
}

int TChunkOwnerBase::GetPrimaryMediumDataPartsOnly() const
{
    return GetDataPartsOnly(GetPrimaryMediumIndex());
}

void TChunkOwnerBase::SetPropertiesOrThrow(const TChunkProperties& props)
{
    ValidateMedia(props, GetPrimaryMediumIndex());
    Properties_ = props;
}

void TChunkOwnerBase::SetPrimaryMediumIndexAndPropertiesOrThrow(
    int primaryMediumIndex,
    const TChunkProperties& props)
{
    ValidateMedia(props, primaryMediumIndex);
    PrimaryMediumIndex_ = primaryMediumIndex;
    Properties_ = props;
}

/*static*/ void TChunkOwnerBase::ValidateMedia(const TChunkProperties& props, int primaryMediumIndex) {
    ValidateMediumIndex(primaryMediumIndex);

    props.ValidateOrThrow();

    auto& primaryMediumProps = props[primaryMediumIndex];

    if (primaryMediumProps.GetReplicationFactor() == 0) {
        THROW_ERROR_EXCEPTION("Medium %v stores no chunk replicas and cannot be made primary", primaryMediumIndex);
    }
    if (primaryMediumProps.GetDataPartsOnly()) {
        THROW_ERROR_EXCEPTION("Medium %v stores no parity parts and cannot be made primary", primaryMediumIndex);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
