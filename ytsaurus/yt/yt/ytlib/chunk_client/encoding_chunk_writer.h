#pragma once

#include "chunk_meta_extensions.h"

#include "memory_tracked_deferred_chunk_meta.h"

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/property.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TEncodingChunkWriter
    : public virtual TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TMemoryTrackedDeferredChunkMetaPtr, Meta, New<TMemoryTrackedDeferredChunkMeta>());
    DEFINE_BYREF_RW_PROPERTY(NProto::TMiscExt, MiscExt);

public:
    TEncodingChunkWriter(
        TEncodingWriterConfigPtr config,
        TEncodingWriterOptionsPtr options,
        IChunkWriterPtr chunkWriter,
        IBlockCachePtr blockCache,
        NLogging::TLogger logger);

    void WriteBlock(
        std::vector<TSharedRef> vectorizedBlock,
        EBlockType blockType,
        std::optional<int> groupIndex = {});
    void WriteBlock(
        TSharedRef block,
        EBlockType blockType,
        std::optional<int> groupIndex = {});

    void Close();

    TFuture<void> GetReadyEvent() const;
    bool IsReady() const;

    double GetCompressionRatio() const;

    TChunkId GetChunkId() const;

    NProto::TDataStatistics GetDataStatistics() const;
    TCodecStatistics GetCompressionStatistics() const;

    bool IsCloseDemanded() const;

private:
    const TEncodingWriterConfigPtr Config_;
    const TEncodingWriterOptionsPtr Options_;
    const IChunkWriterPtr ChunkWriter_;
    const TEncodingWriterPtr EncodingWriter_;

    int CurrentBlockIndex_ = 0;
    i64 LargestBlockSize_ = 0;

    bool Closed_ = false;

    void VerifyBlockType(EBlockType blockType) const;
};

DEFINE_REFCOUNTED_TYPE(TEncodingChunkWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
