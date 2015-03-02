#pragma once

#include "public.h"

#include "data_statistics.h"
#include "reader_base.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IChunkReaderBase
    : public virtual IReaderBase
{
    virtual NProto::TDataStatistics GetDataStatistics() const = 0;

    virtual TFuture<void> GetFetchingCompletedEvent() = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkReaderBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
