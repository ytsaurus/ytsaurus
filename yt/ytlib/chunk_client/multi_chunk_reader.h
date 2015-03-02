#pragma once

#include "public.h"

#include "data_statistics.h"
#include "reader_base.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IMultiChunkReader
    : public virtual IReaderBase
{
    virtual bool IsFetchingCompleted() const = 0;

    virtual NProto::TDataStatistics GetDataStatistics() const = 0;

    virtual std::vector<TChunkId> GetFailedChunkIds() const = 0;

};

DEFINE_REFCOUNTED_TYPE(IMultiChunkReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
