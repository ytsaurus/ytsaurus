#pragma once

#include "chunk_pool.h"

#include <yt/ytlib/table_client/public.h>
#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TSortedJobOptions
{
    bool EnableKeyGuarantee;
    int PrimaryPrefixLength;
    int ForeignPrefixLength;
    bool EnablePeriodicYielder = true;

    std::vector<NTableClient::TKey> PivotKeys;

    //! An upper bound for a total number of slices that is allowed. If this value
    //! is exceeded, an exception is thrown.
    i64 MaxTotalSliceCount;

    //! Experimental workaround for YTADMINREQ-5836.
    bool UseNewEndpointKeys = false;

    void Persist(const TPersistenceContext& context);
};

struct TSortedChunkPoolOptions
{
    TSortedJobOptions SortedJobOptions;
    i64 MinTeleportChunkSize = 0;
    bool SupportLocality = false;
    IJobSizeConstraintsPtr JobSizeConstraints;
    TOperationId OperationId;

    void Persist(const TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

struct IChunkSliceFetcherFactory
    : public IPersistent
    , public virtual TRefCounted
{
    virtual NTableClient::IChunkSliceFetcherPtr CreateChunkSliceFetcher() = 0;

    virtual void Persist(const TPersistenceContext& context) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkSliceFetcherFactory);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IChunkPool> CreateSortedChunkPool(
    const TSortedChunkPoolOptions& options,
    IChunkSliceFetcherFactoryPtr chunkSliceFetcherFactory,
    TInputStreamDirectory dataSourceDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
