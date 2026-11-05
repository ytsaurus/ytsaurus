#pragma once

#include "job_size_tracker.h"
#include "job_manager.h"
#include "private.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/table_client/comparator.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TSortedChunkPoolStatistics final
{
    i64 ForeignSlicesCheckCountInDecideRowSliceability = 0;

    PHOENIX_DECLARE_TYPE(TSortedChunkPoolStatistics, 0x71181524);
};

using TSortedChunkPoolStatisticsPtr = TIntrusivePtr<TSortedChunkPoolStatistics>;

void FormatValue(TStringBuilderBase* builder, const TSortedChunkPoolStatisticsPtr& statistics, TStringBuf spec);

struct TSortedJobOptions
{
    //! Guarantee that each key goes to the single job.
    bool EnableKeyGuarantee = false;

    //! Comparator corresponding to the primary merge or reduce key.
    NTableClient::TComparator PrimaryComparator;

    //! Comparator corresponding to the foreign reduce key.
    NTableClient::TComparator ForeignComparator;

    int PrimaryPrefixLength = 0;
    int ForeignPrefixLength = 0;
    bool EnablePeriodicYielder = true;
    bool ValidateOrder = true;

    bool ConsiderOnlyPrimarySize = false;

    std::vector<NTableClient::TLegacyKey> PivotKeys;

    //! An upper bound for a total number of slices that is allowed.
    i64 MaxTotalSliceCount;

    // Not persisted.
    TJobSizeTrackerOptions JobSizeTrackerOptions;

    PHOENIX_DECLARE_TYPE(TSortedJobOptions, 0x54c67649);
};

////////////////////////////////////////////////////////////////////////////////

//! An interface for the class that encapsulates the whole logic of building sorted* jobs.
//! This class defines a transient object (it is never persisted).
struct ISortedJobBuilder
    : public TRefCounted
{
    virtual void AddDataSlice(const NChunkClient::TDataSlicePtr& originalDataSlice) = 0;
    virtual std::vector<TJobStub> Build() = 0;
    virtual i64 GetTotalDataSliceCount() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISortedJobBuilder)

////////////////////////////////////////////////////////////////////////////////

ISortedJobBuilderPtr CreateSortedJobBuilder(
    const TSortedJobOptions& options,
    NControllerAgent::IJobSizeConstraintsPtr jobSizeConstraints,
    NTableClient::TRowBufferPtr rowBuffer,
    const std::vector<NChunkClient::TInputChunkPtr>& teleportChunks,
    int retryIndex,
    const TInputStreamDirectory& inputStreamDirectory,
    TSortedChunkPoolStatisticsPtr chunkPoolStatistics,
    NLogging::TLogger logger,
    NLogging::TLogger structuredLogger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
