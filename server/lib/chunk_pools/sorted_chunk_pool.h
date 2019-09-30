#pragma once

#include "chunk_pool.h"
#include "input_stream.h"
#include "private.h"
#include "sorted_job_builder.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TSortedChunkPoolOptions
{
    TSortedJobOptions SortedJobOptions;
    i64 MinTeleportChunkSize = 0;
    bool SupportLocality = false;
    NControllerAgent::IJobSizeConstraintsPtr JobSizeConstraints;
    NScheduler::TOperationId OperationId;
    TString Task;
    //! External row buffer, if it is available.
    NTableClient::TRowBufferPtr RowBuffer;
};

////////////////////////////////////////////////////////////////////////////////

struct ISortedChunkPool
    : public IChunkPool
{
    //! Return keys (limits) that define range corresponding to cookie `cookie`.
    virtual std::pair<NTableClient::TUnversionedRow, NTableClient::TUnversionedRow>
        GetLimits(IChunkPoolOutput::TCookie cookie) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IChunkSliceFetcherFactory
    : public IPersistent
    , public virtual TRefCounted
{
    virtual NTableClient::IChunkSliceFetcherPtr CreateChunkSliceFetcher() = 0;

    virtual void Persist(const TPersistenceContext& context) = 0;
};

IChunkSliceFetcherFactoryPtr CreateCallbackChunkSliceFetcherFactory(
    TCallback<NTableClient::IChunkSliceFetcherPtr()> factoryCallback);

DEFINE_REFCOUNTED_TYPE(IChunkSliceFetcherFactory);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISortedChunkPool> CreateSortedChunkPool(
    const TSortedChunkPoolOptions& options,
    IChunkSliceFetcherFactoryPtr chunkSliceFetcherFactory,
    TInputStreamDirectory dataSourceDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
