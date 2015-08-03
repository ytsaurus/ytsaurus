#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"
#include "data_statistics.h"

#include <core/misc/error.h>
#include <core/misc/property.h>

#include <core/actions/future.h>

namespace NYT {
namespace NChunkClient {

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
        IBlockCachePtr blockCache);

    void WriteBlock(std::vector<TSharedRef> vectorizedBlock);
    void WriteBlock(TSharedRef block);
    
    void Close();

    TFuture<void> GetReadyEvent() const;
    bool IsReady() const;

    double GetCompressionRatio() const;

    NProto::TDataStatistics GetDataStatistics() const;

private:
    const IChunkWriterPtr ChunkWriter_;
    const TEncodingWriterPtr EncodingWriter_;

    int CurrentBlockIndex_ = 0;
    i64 LargestBlockSize_ = 0;

};

DEFINE_REFCOUNTED_TYPE(TEncodingChunkWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
