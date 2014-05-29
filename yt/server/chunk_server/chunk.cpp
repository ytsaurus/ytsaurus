#include "stdafx.h"
#include "chunk.h"
#include "private.h"
#include "chunk_tree_statistics.h"
#include "chunk_list.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

// XXX(babenko): fix snapshot bloat caused by remote copy
#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>
#include <core/misc/protobuf_helpers.h>

#include <core/erasure/codec.h>

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NChunkServer {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NCellMaster;
using namespace NObjectServer;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = ChunkServerLogger;

const i64 TChunk::UnknownDiskSpace = -1;

////////////////////////////////////////////////////////////////////////////////

TChunkProperties::TChunkProperties()
    : ReplicationFactor(0)
    , Vital(false)
{ }

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
    , ReplicationFactor(1)
    , ErasureCodec(NErasure::ECodec::None)
{
    Zero(Flags);
    ChunkInfo_.set_disk_space(UnknownDiskSpace);
    ChunkMeta_.set_type(EChunkType::Unknown);
    ChunkMeta_.mutable_extensions();
    ChunkMeta_.set_version(-1);
}

TChunk::~TChunk()
{ }

TChunkTreeStatistics TChunk::GetStatistics() const
{
    auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(ChunkMeta_.extensions());
    YASSERT(ChunkInfo_.disk_space() != TChunk::UnknownDiskSpace);

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

    return result;
}

TClusterResources TChunk::GetResourceUsage() const
{
    i64 diskSpace = IsConfirmed() ? ChunkInfo_.disk_space() * GetReplicationFactor() : 0;
    return TClusterResources(diskSpace, 1);
}

void TChunk::Save(NCellMaster::TSaveContext& context) const
{
    TChunkTree::Save(context);
    TStagedObject::Save(context);

    using NYT::Save;
    Save(context, ChunkInfo_);
    Save(context, ChunkMeta_);
    Save(context, ReplicationFactor);
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
    TStagedObject::Load(context);

    using NYT::Load;
    Load(context, ChunkInfo_);
    Load(context, ChunkMeta_);

    // XXX(babenko): fix snapshot bloat caused by remote copy
    RemoveProtoExtension<NChunkClient::NProto::TBlocksExt>(ChunkMeta_.mutable_extensions());
    RemoveProtoExtension<NChunkClient::NProto::TErasurePlacementExt>(ChunkMeta_.mutable_extensions());
    RemoveProtoExtension<NVersionedTableClient::NProto::TSamplesExt>(ChunkMeta_.mutable_extensions());
    RemoveProtoExtension<NTableClient::NProto::TIndexExt>(ChunkMeta_.mutable_extensions());

    SetReplicationFactor(Load<i16>(context));
    // COMPAT(psushin)
    if (context.GetVersion() >= 20) {
        SetErasureCodec(Load<NErasure::ECodec>(context));
    }
    SetMovable(Load<bool>(context));
    SetVital(Load<bool>(context));
    Load(context, Parents_);
    Load(context, StoredReplicas_);
    Load(context, CachedReplicas_);
}

void TChunk::AddReplica(TNodePtrWithIndex replica, bool cached)
{
    if (cached) {
        if (!CachedReplicas_) {
            CachedReplicas_.reset(new yhash_set<TNodePtrWithIndex>());
        }
        YCHECK(CachedReplicas_->insert(replica).second);
    } else {
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
            if (*it == replica) {
                StoredReplicas_.erase(it);
                return;
            }
        }
        YUNREACHABLE();
    }
}

SmallVector<TNodePtrWithIndex, TypicalReplicaCount> TChunk::GetReplicas() const
{
    SmallVector<TNodePtrWithIndex, TypicalReplicaCount> result(StoredReplicas_.begin(), StoredReplicas_.end());
    if (CachedReplicas_) {
        result.insert(result.end(), CachedReplicas_->begin(), CachedReplicas_->end());
    }
    return result;
}

bool TChunk::IsConfirmed() const
{
    return ChunkMeta_.type() != EChunkType::Unknown;
}

void TChunk::ValidateConfirmed()
{
    if (!IsConfirmed()) {
        THROW_ERROR_EXCEPTION("Chunk %s is not confirmed", ~ToString(Id));
    }
}

bool TChunk::ValidateChunkInfo(const NChunkClient::NProto::TChunkInfo& chunkInfo) const
{
    if (ChunkInfo_.disk_space() == UnknownDiskSpace)
        return true;

    if (chunkInfo.has_meta_checksum() && ChunkInfo_.has_meta_checksum() &&
        ChunkInfo_.meta_checksum() != chunkInfo.meta_checksum())
    {
        return false;
    }

    if (ChunkInfo_.disk_space() != chunkInfo.disk_space()) {
        return false;
    }

    return true;
}

bool TChunk::GetMovable() const
{
    return Flags.Movable;
}

void TChunk::SetMovable(bool value)
{
    Flags.Movable = value;
}

bool TChunk::GetVital() const
{
    return Flags.Vital;
}

void TChunk::SetVital(bool value)
{
    Flags.Vital = value;
}

bool TChunk::GetRefreshScheduled() const
{
    return Flags.RefreshScheduled;
}

void TChunk::SetRefreshScheduled(bool value)
{
    Flags.RefreshScheduled = value;
}

bool TChunk::GetPropertiesUpdateScheduled() const
{
    return Flags.PropertiesUpdateScheduled;
}

void TChunk::SetPropertiesUpdateScheduled(bool value)
{
    Flags.PropertiesUpdateScheduled = value;
}

int TChunk::GetReplicationFactor() const
{
    return ReplicationFactor;
}

void TChunk::SetReplicationFactor(int value)
{
    ReplicationFactor = value;
}

NErasure::ECodec TChunk::GetErasureCodec() const
{
    return NErasure::ECodec(ErasureCodec);
}

void TChunk::SetErasureCodec(NErasure::ECodec value)
{
    ErasureCodec = static_cast<i16>(value);
}

bool TChunk::IsErasure() const
{
    return GetErasureCodec() != NErasure::ECodec::None;
}

bool TChunk::IsAvailable() const
{
    auto codecId = GetErasureCodec();
    if (codecId == NErasure::ECodec::None) {
        return !StoredReplicas_.empty();
    } else {
        auto* codec = NErasure::GetCodec(codecId);
        int dataPartCount = codec->GetDataPartCount();
        NErasure::TPartIndexSet missingIndexSet((1 << dataPartCount) - 1);
        for (auto replica : StoredReplicas_) {
            missingIndexSet.reset(replica.GetIndex());
        }
        return !missingIndexSet.any();
    }
}

TChunkProperties TChunk::GetChunkProperties() const
{
    TChunkProperties result;
    result.ReplicationFactor = GetReplicationFactor();
    result.Vital = GetVital();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
