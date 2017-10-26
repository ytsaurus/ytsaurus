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
{
    Properties_.SetVital(true);
    for (auto& mediumProperties : Properties_) {
        mediumProperties.Clear();
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
    Save(context, Properties_);
    Save(context, PrimaryMediumIndex_);
    Save(context, ChunkPropertiesUpdateNeeded_);
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
        Properties_[DefaultStoreMediumIndex].SetReplicationFactor(Load<int>(context));
        Properties_.SetVital(Load<bool>(context));
    } else {
        Load(context, Properties_);
        PrimaryMediumIndex_ = Load<int>(context);
    }
    Load(context, ChunkPropertiesUpdateNeeded_);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
