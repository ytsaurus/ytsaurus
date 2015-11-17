#pragma once

#include "public.h"

#include "data_statistics.h"

#include <core/actions/future.h>

#include <core/misc/ref_counted.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IReaderBase
    : public virtual TRefCounted
{
    virtual TFuture<void> GetReadyEvent() = 0;

    virtual NProto::TDataStatistics GetDataStatistics() const = 0;

    virtual bool IsFetchingCompleted() const = 0;

    virtual std::vector<TChunkId> GetFailedChunkIds() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IReaderBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
