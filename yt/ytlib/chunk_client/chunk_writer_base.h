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
    virtual i64 GetDataSize() const = 0;

    virtual NProto::TChunkMeta GetMasterMeta() const = 0;
    virtual NProto::TChunkMeta GetSchedulerMeta() const = 0;

    virtual NProto::TDataStatistics GetDataStatistics() const = 0;

};

DEFINE_REFCOUNTED_TYPE(IChunkWriterBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
