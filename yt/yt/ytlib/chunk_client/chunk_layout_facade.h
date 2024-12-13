#pragma once

#include "public.h"
#include "block.h"

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////

// TODO(achulkov2): Do we need this?
class TReadChunkLayoutFacade
    : public TRefCounted
{
};

DEFINE_REFCOUNTED_TYPE(TReadChunkLayoutFacade)

////////////////////////////////////////////////////////////////////////////

// TODO(achulkov2): Maybe focus on the fact that this is all about reading/writing chunk meta?

// TODO(achulkov2): Think of a better name. Maybe with the word layout?
// TODO(achulkov2): Separate an interface? Depends.
// TODO(achulkov2): Thread safety.

//! The purpose of this class is to isolate all preparation logic related to the physical
//! layout of a chunk file and chunk meta contents. It acts as a guarantee that different
//! chunk writers produce binary identical chunk and chunk meta files independent of the
//! underlying storage medium (file, S3).
class TChunkLayoutFacade
    : public TRefCounted
{
public:
    TChunkLayoutFacade(NChunkClient::TChunkId chunkId);

    struct TWriteRequest
    {
        i64 StartOffset = 0;
        i64 EndOffset = 0;
        std::vector<TSharedRef> Buffers;
    };
    TWriteRequest AddBlocks(const std::vector<TBlock>& blocks);

    TSharedMutableRef Close(TDeferredChunkMetaPtr chunkMeta);

    TSharedMutableRef PrepareChunkMetaBlob();

    i64 GetDataSize() const;
    i64 GetMetaDataSize() const;
    const NChunkClient::TRefCountedChunkMetaPtr& GetChunkMeta() const;
    const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const;

private:
    const NChunkClient::TChunkId ChunkId_;
    // TODO(achulkov2): Do we need this allocation?
    const NChunkClient::TRefCountedChunkMetaPtr ChunkMeta_ = New<NChunkClient::TRefCountedChunkMeta>();
    const NLogging::TLogger Logger;

    NChunkClient::NProto::TChunkInfo ChunkInfo_;
    NChunkClient::NProto::TBlocksExt BlocksExt_;

    i64 DataSize_ = 0;
    i64 MetaDataSize_ = 0;

    // TODO(achulkov2): Add some state variable for validation that chunk is not modified after meta blob is built?
};

DEFINE_REFCOUNTED_TYPE(TChunkLayoutFacade)

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient