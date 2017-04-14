#pragma once

#include "chunk_pool.h"

#include <yt/ytlib/table_client/public.h>
#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

struct TSortedChunkPoolOptions
{
    bool EnableKeyGuarantee;
    int PrimaryPrefixLength = 0;
    int ForeignPrefixLength = 0;
    i64 MinTeleportChunkSize = 0;
    i64 MaxTotalSliceCount = 0;
    bool SupportLocality = false;
    bool EnablePeriodicYielder = true;
    IJobSizeConstraintsPtr JobSizeConstraints;

    TOperationId OperationId;

    void Persist(const TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

struct IChunkSliceFetcherFactory
    : public IPersistent
    , public TRefCounted
{
    virtual NTableClient::IChunkSliceFetcherPtr CreateChunkSliceFetcher() = 0;

    virtual void Persist(const TPersistenceContext& context) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkSliceFetcherFactory);

////////////////////////////////////////////////////////////////////

std::unique_ptr<IChunkPool> CreateSortedChunkPool(
    const TSortedChunkPoolOptions& options,
    IChunkSliceFetcherFactoryPtr chunkSliceFetcherFactory,
    TInputStreamDirectory dataSourceDirectory);

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
