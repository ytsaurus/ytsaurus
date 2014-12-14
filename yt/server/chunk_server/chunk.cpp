#include "stdafx.h"
#include "chunk.h"
#include "chunk_tree_statistics.h"
#include "chunk_list.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/object_client/helpers.h>

#include <core/erasure/codec.h>

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NChunkServer {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NSecurityServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

bool operator== (const TChunkProperties& lhs, const TChunkProperties& rhs)
{
    return
        lhs.ReplicationFactor == rhs.ReplicationFactor &&
        lhs.Vital == rhs.Vital;
}

bool operator!= (const TChunkProperties& lhs, const TChunkProperties& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

TChunk::TChunk(const TChunkId& id)
    : TChunkTree(id)
    , ReplicationFactor_(1)
    , ReadQuorum_(0)
    , WriteQuorum_(0)
    , ErasureCodec_(NErasure::ECodec::None)
{
    Zero(Flags_);

    ChunkMeta_.set_type(EChunkType::Unknown);
    ChunkMeta_.set_version(-1);
    ChunkMeta_.mutable_extensions();
}

TChunkTreeStatistics TChunk::GetStatistics() const
{
    YASSERT(IsConfirmed());

    auto miscExt = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());

    TChunkTreeStatistics result;
    result.RowCount = miscExt.row_count();
    result.UncompressedDataSize = miscExt.uncompressed_data_size();
    result.CompressedDataSize = miscExt.compressed_data_size();
    result.DataWeight = miscExt.data_weight();

    if (IsErasure()) {
        result.ErasureDiskSpace = ChunkInfo_.disk_space();
    } else {
        result.RegularDiskSpace = ChunkInfo_.disk_space();
    }

    result.ChunkCount = 1;
    result.Rank = 0;
    result.Sealed = IsSealed();

    return result;
}

TClusterResources TChunk::GetResourceUsage() const
{
    i64 diskSpace = IsConfirmed() ? ChunkInfo_.disk_space() * GetReplicationFactor() : 0;
    return TClusterResources(diskSpace, 0, 1);
}

void TChunk::Save(NCellMaster::TSaveContext& context) const
{
    TChunkTree::Save(context);

    using NYT::Save;
    Save(context, ChunkInfo_);
    Save(context, ChunkMeta_);
    Save(context, ReplicationFactor_);
    Save(context, ReadQuorum_);
    Save(context, WriteQuorum_);
    Save(context, GetErasureCodec());
    Save(context, GetMovable());
    Save(context, GetVital());
    Save(context, Parents_);
    Save(context, StoredReplicas_);
    Save(context, CachedReplicas_);
}

void TChunk::Load(NCellMaster::TLoadContext& context)
{
    TChunkTree::Load(context);

    using NYT::Load;
    Load(context, ChunkInfo_);
    Load(context, ChunkMeta_);

    if (context.GetVersion() < 100) {
        SetReplicationFactor(Load<i16>(context));
    } else {
        SetReplicationFactor(Load<i8>(context));
        SetReadQuorum(Load<i8>(context));
        SetWriteQuorum(Load<i8>(context));
    }
    SetErasureCodec(Load<NErasure::ECodec>(context));

    SetMovable(Load<bool>(context));
    SetVital(Load<bool>(context));
    Load(context, Parents_);
    Load(context, StoredReplicas_);
    Load(context, CachedReplicas_);
}

void TChunk::AddReplica(TNodePtrWithIndex replica, bool cached)
{
    if (cached) {
        YASSERT(!IsJournal());
        if (!CachedReplicas_) {
            CachedReplicas_.reset(new yhash_set<TNodePtrWithIndex>());
        }
        YCHECK(CachedReplicas_->insert(replica).second);
    } else {
        if (IsJournal()) {
            for (auto& existingReplica : StoredReplicas_) {
                if (existingReplica.GetPtr() == replica.GetPtr()) {
                    existingReplica = replica;
                    return;
                }
            }
        }
        StoredReplicas_.push_back(replica);
    }
}

void TChunk::RemoveReplica(TNodePtrWithIndex replica, bool cached)
{
    if (cached) {
        YASSERT(CachedReplicas_);
        YCHECK(CachedReplicas_->erase(replica) == 1);
        if (CachedReplicas_->empty()) {
            CachedReplicas_.reset();
        }
    } else {
        for (auto it = StoredReplicas_.begin(); it != StoredReplicas_.end(); ++it) {
            auto& existingReplica = *it;
            if (existingReplica == replica ||
                IsJournal() && existingReplica.GetPtr() == replica.GetPtr())
            {
                std::swap(existingReplica, StoredReplicas_.back());
                StoredReplicas_.resize(StoredReplicas_.size() - 1);
                return;
            }
        }
        YUNREACHABLE();
    }
}

TNodePtrWithIndexList TChunk::GetReplicas() const
{
    TNodePtrWithIndexList result(StoredReplicas_.begin(), StoredReplicas_.end());
    if (CachedReplicas_) {
        result.insert(result.end(), CachedReplicas_->begin(), CachedReplicas_->end());
    }
    return result;
}

void TChunk::ApproveReplica(TNodePtrWithIndex replica)
{
    if (IsJournal()) {
        for (auto& existingReplica : StoredReplicas_) {
            if (existingReplica.GetPtr() == replica.GetPtr()) {
                existingReplica = replica;
                return;
            }
        }
        YUNREACHABLE();
    }
}

bool TChunk::IsConfirmed() const
{
    return ChunkMeta_.type() != EChunkType::Unknown;
}

void TChunk::ValidateConfirmed()
{
    if (!IsConfirmed()) {
        THROW_ERROR_EXCEPTION("Chunk %v is not confirmed",
            Id);
    }
}

bool TChunk::GetMovable() const
{
    return Flags_.Movable;
}

void TChunk::SetMovable(bool value)
{
    Flags_.Movable = value;
}

bool TChunk::GetVital() const
{
    return Flags_.Vital;
}

void TChunk::SetVital(bool value)
{
    Flags_.Vital = value;
}

bool TChunk::GetRefreshScheduled() const
{
    return Flags_.RefreshScheduled;
}

void TChunk::SetRefreshScheduled(bool value)
{
    Flags_.RefreshScheduled = value;
}

bool TChunk::GetPropertiesUpdateScheduled() const
{
    return Flags_.PropertiesUpdateScheduled;
}

void TChunk::SetPropertiesUpdateScheduled(bool value)
{
    Flags_.PropertiesUpdateScheduled = value;
}

bool TChunk::GetSealScheduled() const
{
    return Flags_.SealScheduled;
}

void TChunk::SetSealScheduled(bool value)
{
    Flags_.SealScheduled = value;
}

int TChunk::GetReplicationFactor() const
{
    return ReplicationFactor_;
}

void TChunk::SetReplicationFactor(int value)
{
    ReplicationFactor_ = value;
}

int TChunk::GetReadQuorum() const
{
    return ReadQuorum_;
}

void TChunk::SetReadQuorum(int value)
{
    ReadQuorum_ = value;
}

int TChunk::GetWriteQuorum() const
{
    return WriteQuorum_;
}

void TChunk::SetWriteQuorum(int value)
{
    WriteQuorum_ = value;
}

NErasure::ECodec TChunk::GetErasureCodec() const
{
    return NErasure::ECodec(ErasureCodec_);
}

void TChunk::SetErasureCodec(NErasure::ECodec value)
{
    ErasureCodec_ = static_cast<i16>(value);
}

bool TChunk::IsErasure() const
{
    return TypeFromId(Id) == EObjectType::ErasureChunk;
}

bool TChunk::IsJournal() const
{
    return TypeFromId(Id) == EObjectType::JournalChunk;
}

bool TChunk::IsRegular() const
{
    return TypeFromId(Id) == EObjectType::Chunk;
}

bool TChunk::IsAvailable() const
{
    if (IsRegular()) {
        return !StoredReplicas_.empty();
    } else if (IsErasure()) {
        auto* codec = NErasure::GetCodec(GetErasureCodec());
        int dataPartCount = codec->GetDataPartCount();
        NErasure::TPartIndexSet missingIndexSet((1 << dataPartCount) - 1);
        for (auto replica : StoredReplicas_) {
            missingIndexSet.reset(replica.GetIndex());
        }
        return !missingIndexSet.any();
    } else if (IsJournal()) {
        if (StoredReplicas_.size() >= GetReadQuorum()) {
            return true;
        }
        for (auto replica : StoredReplicas_) {
            if (replica.GetIndex() == EJournalReplicaType::Sealed) {
                return true;
            }
        }
        return false;
    } else {
        YUNREACHABLE();
    }
}

bool TChunk::IsSealed() const
{
    if (!IsConfirmed()) {
        return false;
    }

    if (!IsJournal()) {
        return true;
    }

    auto miscExt = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());
    return miscExt.sealed();
}

i64 TChunk::GetSealedRowCount() const
{
    auto miscExt = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());
    YCHECK(miscExt.sealed());
    return miscExt.row_count();
}

void TChunk::Seal(const TMiscExt& info)
{
    YASSERT(IsConfirmed());

    auto miscExt = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());

    // NB: Just a sanity check.
    YCHECK(!miscExt.sealed());
    YCHECK(miscExt.row_count() == 0);
    YCHECK(miscExt.uncompressed_data_size() == 0);
    YCHECK(miscExt.compressed_data_size() == 0);
    YCHECK(ChunkInfo_.disk_space() == 0);

    miscExt.set_sealed(true);
    miscExt.set_row_count(info.row_count());
    miscExt.set_uncompressed_data_size(info.uncompressed_data_size());
    miscExt.set_compressed_data_size(info.compressed_data_size());
    SetProtoExtension(ChunkMeta_.mutable_extensions(), miscExt);
    ChunkInfo_.set_disk_space(info.uncompressed_data_size());  // an approximation
}

TChunkProperties TChunk::GetChunkProperties() const
{
    TChunkProperties result;
    result.ReplicationFactor = GetReplicationFactor();
    result.Vital = GetVital();
    return result;
}

int TChunk::GetMaxReplicasPerRack() const
{
    switch (GetType()) {
        case EObjectType::Chunk:
            return std::max(GetReplicationFactor() - 1, 1);

        case EObjectType::ErasureChunk:
            return NErasure::GetCodec(GetErasureCodec())->GetGuaranteedRepairablePartCount();

        case EObjectType::JournalChunk:
            return 1;

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
