#include "stdafx.h"
#include "chunk.h"

#include "common.h"
#include "chunk_tree_statistics.h"

#include <ytlib/cell_master/load_context.h>

namespace NYT {
namespace NChunkServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NChunkHolder::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

TChunk::TChunk(const TChunkId& id)
    : TObjectWithIdBase(id)
    , ReplicationFactor_(1)
{
    // We must set required proto fields to ensure successful work of #Save
    ChunkInfo_.set_size(UnknownSize);
    ChunkMeta_.set_type(EChunkType::Unknown);
}

TChunk::~TChunk()
{ }

TChunkTreeStatistics TChunk::GetStatistics() const
{
    TChunkTreeStatistics result;

    YASSERT(GetSize() != TChunk::UnknownSize);
    result.CompressedSize = GetSize();
    result.ChunkCount = 1;

    auto attributes = DeserializeAttributes();
    switch (attributes.type()) {
        case EChunkType::File: {
            const auto& fileAttributes = attributes.GetExtension(NFileClient::NProto::TFileChunkAttributes::file_attributes);
            result.UncompressedSize = fileAttributes.uncompressed_size();
            break;
        }

        case EChunkType::Table: {
            const auto& tableAttributes = attributes.GetExtension(NTableClient::NProto::TTableChunkAttributes::table_attributes);
            result.RowCount = tableAttributes.row_count();
            result.UncompressedSize = tableAttributes.uncompressed_size();
            break;
        }

        default:
            YUNREACHABLE();
    }

    return result;
}

void TChunk::Save(TOutputStream* output) const
{
    TObjectWithIdBase::Save(output);
    YVERIFY(ChunkInfo_.SerializeToStream(output));
    YVERIFY(ChunkMeta_.SerializeToStream(output));
    ::Save(output, ReplicationFactor_);
    ::Save(output, StoredLocations_);
    SaveNullableSet(output, CachedLocations_);
}

void TChunk::Load(const TLoadContext& context, TInputStream* input)
{
    UNUSED(context);
    TObjectWithIdBase::Load(input);
    YVERIFY(ChunkInfo_.ParseFromStream(input));
    YVERIFY(ChunkMeta_.ParseFromStream(input));
    ::Load(input, ReplicationFactor_);
    ::Load(input, StoredLocations_);
    LoadNullableSet(input, CachedLocations_);
}

void TChunk::AddLocation(THolderId holderId, bool cached)
{
    if (cached) {
        if (!CachedLocations_) {
            CachedLocations_.Reset(new yhash_set<THolderId>());
        }
        YVERIFY(CachedLocations_->insert(holderId).second);
    } else {
        StoredLocations_.push_back(holderId);
    }
}

void TChunk::RemoveLocation(THolderId holderId, bool cached)
{
    if (cached) {
        YASSERT(~CachedLocations_);
        YVERIFY(CachedLocations_->erase(holderId) == 1);
        if (CachedLocations_->empty()) {
            CachedLocations_.Destroy();
        }
    } else {
        for (auto it = StoredLocations_.begin(); it != StoredLocations_.end(); ++it) {
            if (*it == holderId) {
                StoredLocations_.erase(it);
                return;
            }
        }
        YUNREACHABLE();
    }
}

yvector<THolderId> TChunk::GetLocations() const
{
    yvector<THolderId> result(StoredLocations_.begin(), StoredLocations_.end());
    if (~CachedLocations_) {
        result.insert(result.end(), CachedLocations_->begin(), CachedLocations_->end());
    }
    return result;
}

bool TChunk::IsConfirmed() const
{
    return ChunkMeta_.type() != EChunkType::Unknown;
}

bool TChunk::ValidateChunkInfo(const NChunkHolder::NProto::TChunkInfo& chunkInfo) const
{
    if (ChunkInfo_.size() == UnknownSize)
        return true;

    if (chunkInfo.has_meta_checksum() && ChunkInfo_.has_meta_checksum() &&
        ChunkInfo_.meta_checksum() != chunkInfo.meta_checksum())
    {
        return false;
    }

    return ChunkInfo_.size() == chunkInfo.size();
}

const i64 TChunk::UnknownSize = -1;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
