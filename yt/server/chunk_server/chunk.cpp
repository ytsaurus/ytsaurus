#include "stdafx.h"
#include "chunk.h"
#include "private.h"
#include "chunk_tree_statistics.h"
#include "chunk_list.h"

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NChunkServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;
const i64 TChunk::UnknownSize = -1;

////////////////////////////////////////////////////////////////////////////////

TChunk::TChunk(const TChunkId& id)
    : TChunkTree(id)
    , ReplicationFactor_(1)
{
    Zero(Flags);

    // Initialize required proto fields, otherwise #Save would fail.
    ChunkInfo_.set_size(UnknownSize);

    ChunkMeta_.set_type(EChunkType::Unknown);
    ChunkMeta_.mutable_extensions();
    ChunkMeta_.set_version(-1);
}

TChunk::~TChunk()
{ }

TChunkTreeStatistics TChunk::GetStatistics() const
{
    auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(ChunkMeta_.extensions());
    YASSERT(ChunkInfo_.size() != TChunk::UnknownSize);

    TChunkTreeStatistics result;
    result.RowCount = miscExt.row_count();
    result.UncompressedDataSize = miscExt.uncompressed_data_size();
    result.CompressedDataSize = miscExt.compressed_data_size();
    result.DataWeight = miscExt.data_weight();
    result.DiskSpace = ChunkInfo_.size();
    result.ChunkCount = 1;
    result.Rank = 0;

    return result;
}

TClusterResources TChunk::GetResourceUsage() const
{
    i64 diskSpace = IsConfirmed() ? ChunkInfo_.size() * ReplicationFactor_ : 0;
    return TClusterResources(diskSpace, 1);
}

void TChunk::Save(const NCellMaster::TSaveContext& context) const
{
    TChunkTree::Save(context);
    TStagedObject::Save(context);

    auto* output = context.GetOutput();
    SaveProto(output, ChunkInfo_);
    SaveProto(output, ChunkMeta_);
    ::Save(output, ReplicationFactor_);
    ::Save(output, GetMovable());
    ::Save(output, GetVital());
    SaveObjectRefs(context, Parents_);
    SaveObjectRefs(context, StoredReplicas_);
    SaveNullableObjectRefs(context, CachedReplicas_);
}

void TChunk::Load(const NCellMaster::TLoadContext& context)
{
    TChunkTree::Load(context);
    TStagedObject::Load(context);

    auto* input = context.GetInput();
    LoadProto(input, ChunkInfo_);
    LoadProto(input, ChunkMeta_);
    ::Load(input, ReplicationFactor_);
    SetMovable(NCellMaster::Load<bool>(context));
    SetVital(NCellMaster::Load<bool>(context));
    LoadObjectRefs(context, Parents_);
    LoadObjectRefs(context, StoredReplicas_);
    LoadNullableObjectRefs(context, CachedReplicas_);
}

void TChunk::AddReplica(TDataNodeWithIndex replica, bool cached)
{
    if (cached) {
        if (!CachedReplicas_) {
            CachedReplicas_.Reset(new yhash_set<TDataNodeWithIndex>());
        }
        YCHECK(CachedReplicas_->insert(replica).second);
    } else {
        StoredReplicas_.push_back(replica);
    }
}

void TChunk::RemoveReplica(TDataNodeWithIndex replica, bool cached)
{
    if (cached) {
        YASSERT(~CachedReplicas_);
        YCHECK(CachedReplicas_->erase(replica) == 1);
        if (CachedReplicas_->empty()) {
            CachedReplicas_.Destroy();
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

TSmallVector<TDataNodeWithIndex, TypicalReplicationFactor> TChunk::GetReplicas() const
{
    TSmallVector<TDataNodeWithIndex, TypicalReplicationFactor> result(StoredReplicas_.begin(), StoredReplicas_.end());
    if (~CachedReplicas_) {
        result.insert(result.end(), CachedReplicas_->begin(), CachedReplicas_->end());
    }
    return result;
}

bool TChunk::IsConfirmed() const
{
    return ChunkMeta_.type() != EChunkType::Unknown;
}

bool TChunk::ValidateChunkInfo(const NChunkClient::NProto::TChunkInfo& chunkInfo) const
{
    if (ChunkInfo_.size() == UnknownSize)
        return true;

    if (chunkInfo.has_meta_checksum() && ChunkInfo_.has_meta_checksum() &&
        ChunkInfo_.meta_checksum() != chunkInfo.meta_checksum())
    {
        return false;
    }

    if (ChunkInfo_.size() != chunkInfo.size()) {
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

bool TChunk::GetRFUpdateScheduled() const
{
    return Flags.RFUpdateScheduled;
}

void TChunk::SetRFUpdateScheduled(bool value)
{
    Flags.RFUpdateScheduled = value;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
