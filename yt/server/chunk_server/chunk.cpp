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
    : TStagedObjectBase(id)
    , ReplicationFactor_(1)
    , Movable_(true)
    , Vital_(true)
{
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
    return
        IsConfirmed()
        ? TClusterResources(ChunkInfo_.size() * ReplicationFactor_)
        : ZeroClusterResources();
}

void TChunk::Save(const NCellMaster::TSaveContext& context) const
{
    TStagedObjectBase::Save(context);
    
    auto* output = context.GetOutput();
    SaveProto(output, ChunkInfo_);
    SaveProto(output, ChunkMeta_);
    ::Save(output, ReplicationFactor_);
    ::Save(output, Movable_);
    ::Save(output, Vital_);
    SaveObjectRefs(output, Parents_);
    ::Save(output, StoredLocations_);
    SaveNullableSet(output, CachedLocations_);
}

void TChunk::Load(const NCellMaster::TLoadContext& context)
{
    TStagedObjectBase::Load(context);
    
    auto* input = context.GetInput();
    LoadProto(input, ChunkInfo_);
    LoadProto(input, ChunkMeta_);
    ::Load(input, ReplicationFactor_);
    ::Load(input, Movable_);
    ::Load(input, Vital_);
    LoadObjectRefs(input, Parents_, context);
    ::Load(input, StoredLocations_);
    LoadNullableSet(input, CachedLocations_);
}

void TChunk::AddLocation(TNodeId nodeId, bool cached)
{
    if (cached) {
        if (!CachedLocations_) {
            CachedLocations_.Reset(new yhash_set<TNodeId>());
        }
        YCHECK(CachedLocations_->insert(nodeId).second);
    } else {
        StoredLocations_.push_back(nodeId);
    }
}

void TChunk::RemoveLocation(TNodeId nodeId, bool cached)
{
    if (cached) {
        YASSERT(~CachedLocations_);
        YCHECK(CachedLocations_->erase(nodeId) == 1);
        if (CachedLocations_->empty()) {
            CachedLocations_.Destroy();
        }
    } else {
        for (auto it = StoredLocations_.begin(); it != StoredLocations_.end(); ++it) {
            if (*it == nodeId) {
                StoredLocations_.erase(it);
                return;
            }
        }
        YUNREACHABLE();
    }
}

TSmallVector<TNodeId, TypicalReplicationFactor> TChunk::GetLocations() const
{
    TSmallVector<TNodeId, TypicalReplicationFactor> result(StoredLocations_.begin(), StoredLocations_.end());
    if (~CachedLocations_) {
        result.insert(result.end(), CachedLocations_->begin(), CachedLocations_->end());
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
