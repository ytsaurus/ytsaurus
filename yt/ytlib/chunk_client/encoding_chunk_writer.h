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
    DEFINE_BYREF_RW_PROPERTY(NProto::TChunkMeta, Meta);
    DEFINE_BYREF_RW_PROPERTY(NProto::TMiscExt, MiscExt);

public:
    TEncodingChunkWriter(
        const TEncodingWriterConfigPtr& config,
        const TEncodingWriterOptionsPtr& options,
        const IAsyncWriterPtr asyncWriter);

    void WriteBlock(std::vector<TSharedRef>&& data);
    TError Close();

    TAsyncError GetReadyEvent() const;
    bool IsReady() const;

    NProto::TDataStatistics GetDataStatistics() const;

private:
    IAsyncWriterPtr AsyncWriter_;
    TEncodingWriterPtr EncodingWriter_;

    int CurrentBlockIndex_;
    i64 LargestBlockSize_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
