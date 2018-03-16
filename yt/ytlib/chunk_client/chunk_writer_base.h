#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"
#include "data_statistics.h"
#include "writer_base.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IChunkWriterBase
    : public virtual IWriterBase
{
    virtual i64 GetMetaSize() const = 0;
    virtual i64 GetCompressedDataSize() const = 0;

    virtual i64 GetDataWeight() const = 0;

    // Exposes writer internal wish to be closed; e.g. partition chunk writer may
    // want to be closed if some partition row count is close to i32 limit.
    virtual bool IsCloseDemanded() const = 0;

    virtual NProto::TChunkMeta GetMasterMeta() const = 0;
    virtual NProto::TChunkMeta GetSchedulerMeta() const = 0;
    virtual NProto::TChunkMeta GetNodeMeta() const = 0;
    virtual TChunkId GetChunkId() const = 0;

    virtual NProto::TDataStatistics GetDataStatistics() const = 0;
    virtual TCodecStatistics GetCompressionStatistics() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkWriterBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
