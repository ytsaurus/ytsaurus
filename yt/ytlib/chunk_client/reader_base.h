#pragma once

#include "public.h"

#include "data_statistics.h"

#include <yt/core/actions/future.h>

#include <yt/client/table_client/unversioned_row.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IReaderBase
    : public virtual TRefCounted
{
    virtual TFuture<void> GetReadyEvent() = 0;

    virtual NProto::TDataStatistics GetDataStatistics() const = 0;

    virtual TCodecStatistics GetDecompressionStatistics() const = 0;

    virtual bool IsFetchingCompleted() const = 0;

    virtual std::vector<TChunkId> GetFailedChunkIds() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IReaderBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
