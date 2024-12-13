#pragma once

#include "public.h"
#include "block.h"

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////

// TODO(achulkov2): [PForReview] Figure out what to do with this. We use this code in server/lib,
// even though there is a ref-counted proto wrapper in ytlib. Why?

struct TMyBlockInfo
{
    i64 Offset = 0;
    i64 Size = 0;
    ui64 Checksum = 0;
};

DECLARE_REFCOUNTED_STRUCT(TBlocksExt)

struct TBlocksExt final
{
    TBlocksExt() = default;

    explicit TBlocksExt(const NChunkClient::NProto::TBlocksExt& message);

    std::vector<TMyBlockInfo> Blocks;
    bool SyncOnClose = false;
};

DEFINE_REFCOUNTED_TYPE(TBlocksExt)

////////////////////////////////////////////////////////////////////////////

// TODO(achulkov2): [PForReview] Start using these classes in main chunk/read/write code. Or maybe leave it for a later PR?

//! The purpose of this class is to isolate all reading logic related to the physical layout of a chunk file.
//! It acts as a guarantee that different chunk readers read the same binary chunk file in the same way.
class TChunkLayoutReader
    : public TRefCounted
{
public:
    struct TOptions
    {
        bool ValidateBlockChecksums = true;
    };

    TChunkLayoutReader(
        NChunkClient::TChunkId chunkId,
        TString chunkFileName,
        TString chunkMetaFileName,
        const TOptions& options);

    struct TBlockRange
    {
        int StartBlockIndex = 0;
        int EndBlockIndex = 0;
    };
    //! Extracts maximum contiguous block ranges from the given block indexes.
    static std::vector<TBlockRange> GetBlockRanges(const std::vector<int>& blockIndexes);
    //! Returns one range.
    static std::vector<TBlockRange> GetBlockRanges(int startBlockIndex, int blockCount);
    
    struct TReadRequest
    {
        i64 Offset;
        i64 Size;
    };
    //! Transforms block range into specific read request to be performed on the underlying storage medium.
    TReadRequest GetReadRequest(TBlockRange blockRange, const TBlocksExtPtr& blocksExt);
    

    struct TChunkMetaWithChunkId
    {
        NChunkClient::TChunkId ChunkId;
        NChunkClient::TRefCountedChunkMetaPtr ChunkMeta;
    };
    //! Deserializes chunk meta from blob with format validation.
    //! For chunk meta version 2+, the local chunk id stored in this class is validated against the one
    //! stored in the meta file. Passing NullChunkId to this class suppresses this check.
    TChunkMetaWithChunkId DeserializeMeta(TSharedRef metaFileBlob);

    //! Deserializes blocks according to blocks extension. Validates checksums if configured.
    std::vector<TBlock> DeserializeBlocks(TSharedRef blocksBlob, TBlockRange blockRange, const TBlocksExtPtr& blocksExt);

private:
    const NChunkClient::TChunkId ChunkId_;
    const TString ChunkFileName_;
    const TString ChunkMetaFileName_;
    const TOptions Options_;
    const NLogging::TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TChunkLayoutReader)

////////////////////////////////////////////////////////////////////////////

// TODO(achulkov2): [PForReview] Naming.
// TODO(achulkov2): [PNow] Thread safety?

//! The purpose of this class is to isolate all preparation logic related to the physical
//! layout of a chunk file and chunk meta contents. It acts as a guarantee that different
//! chunk writers produce binary identical chunk and chunk meta files independent of the
//! underlying storage medium (file, S3).
class TChunkLayoutFacade
    : public TRefCounted
{
public:
    TChunkLayoutFacade(NChunkClient::TChunkId chunkId);

    //! Write-related methods.

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
    const NChunkClient::TRefCountedChunkMetaPtr ChunkMeta_ = New<NChunkClient::TRefCountedChunkMeta>();
    const NLogging::TLogger Logger;

    NChunkClient::NProto::TChunkInfo ChunkInfo_;
    NChunkClient::NProto::TBlocksExt BlocksExt_;

    i64 DataSize_ = 0;
    i64 MetaDataSize_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TChunkLayoutFacade)

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient