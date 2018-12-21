#pragma once

#include "chunk_meta_extensions.h"

#include <yt/client/chunk_client/data_statistics.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/property.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TEncodingChunkWriter
    : public virtual TRefCounted
{
public:
    DEFINE_BYREF_RW_PROPERTY(NProto::TChunkMeta, Meta);
    DEFINE_BYREF_RW_PROPERTY(NProto::TMiscExt, MiscExt);

public:
    TEncodingChunkWriter(
        TEncodingWriterConfigPtr config,
        TEncodingWriterOptionsPtr options,
        IChunkWriterPtr chunkWriter,
        IBlockCachePtr blockCache,
        const NLogging::TLogger& logger);

    void WriteBlock(std::vector<TSharedRef> vectorizedBlock);
    void WriteBlock(TSharedRef block);

    void Close();

    TFuture<void> GetReadyEvent() const;
    bool IsReady() const;

    double GetCompressionRatio() const;

    TChunkId GetChunkId() const;

    NProto::TDataStatistics GetDataStatistics() const;
    TCodecStatistics GetCompressionStatistics() const;

    bool HasSickReplicas() const;

private:
    const IChunkWriterPtr ChunkWriter_;
    const TEncodingWriterPtr EncodingWriter_;

    int CurrentBlockIndex_ = 0;
    i64 LargestBlockSize_ = 0;

    bool Closed_ = false;

};

DEFINE_REFCOUNTED_TYPE(TEncodingChunkWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
