#pragma once

#include "chunk_pool.h"
#include "input_stream.h"
#include "private.h"
#include "sorted_job_builder.h"

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/job_tracker_client/public.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TSortedChunkPoolOptions
{
    TSortedJobOptions SortedJobOptions;
    bool UseNewJobBuilder = false;

    // Used only in legacy pool. Refer to a commentary in legacy pool's StripeList implementation.
    bool ReturnNewDataSlices = true;

    i64 MinTeleportChunkSize = 0;
    bool SupportLocality = false;
    bool SliceForeignChunks = false;
    NControllerAgent::IJobSizeConstraintsPtr JobSizeConstraints;
    NTableClient::TRowBufferPtr RowBuffer;
    NLogging::TLogger Logger;
    NLogging::TLogger StructuredLogger;
};

////////////////////////////////////////////////////////////////////////////////

struct ISortedChunkPool
    : public IPersistentChunkPool
{
    //! Return keys (limits) that define range corresponding to cookie `cookie`.
    virtual std::pair<NTableClient::TKeyBound, NTableClient::TKeyBound>
        GetBounds(IChunkPoolOutput::TCookie cookie) const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISortedChunkPool)

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): move to ytlib.
struct IChunkSliceFetcherFactory
    : public IPersistent
    , public virtual TRefCounted
{
    virtual NTableClient::IChunkSliceFetcherPtr CreateChunkSliceFetcher() = 0;

    void Persist(const TPersistenceContext& context) override = 0;
};

IChunkSliceFetcherFactoryPtr CreateCallbackChunkSliceFetcherFactory(
    TCallback<NTableClient::IChunkSliceFetcherPtr()> factoryCallback);

DEFINE_REFCOUNTED_TYPE(IChunkSliceFetcherFactory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
