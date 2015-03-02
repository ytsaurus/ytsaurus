#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"
#include "data_statistics.h"

#include <core/misc/error.h>
#include <core/misc/property.h>

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
        IChunkWriterPtr asyncWriter);

    void WriteBlock(std::vector<TSharedRef>&& data);
    void WriteBlock(TSharedRef&& data);
    
    void Close();

    TFuture<void> GetReadyEvent() const;
    bool IsReady() const;

    double GetCompressionRatio() const;

    NProto::TDataStatistics GetDataStatistics() const;

private:
    IChunkWriterPtr ChunkWriter_;
    TEncodingWriterPtr EncodingWriter_;

    int CurrentBlockIndex_ = 0;
    i64 LargestBlockSize_ = 0;

};

DEFINE_REFCOUNTED_TYPE(TEncodingChunkWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
