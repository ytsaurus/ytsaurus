#pragma once

#include "private.h"

#include "new_job_manager.h"
#include "sorted_job_builder.h"

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

//! An interface for the class that encapsulates the whole logic of building sorted* jobs.
//! This class defines a transient object (it is never persisted).
struct INewSortedJobBuilder
    : public TRefCounted
{
    virtual void AddDataSlice(const NChunkClient::TLegacyDataSlicePtr& dataSlice) = 0;
    virtual std::vector<TNewJobStub> Build() = 0;
    virtual i64 GetTotalDataSliceCount() const = 0;
};

DEFINE_REFCOUNTED_TYPE(INewSortedJobBuilder)

////////////////////////////////////////////////////////////////////////////////

INewSortedJobBuilderPtr CreateNewSortedJobBuilder(
    const TSortedJobOptions& options,
    NControllerAgent::IJobSizeConstraintsPtr jobSizeConstraints,
    const NTableClient::TRowBufferPtr& rowBuffer,
    const std::vector<NChunkClient::TInputChunkPtr>& teleportChunks,
    int retryIndex,
    const TInputStreamDirectory& inputStreamDirectory,
    const NLogging::TLogger& logger,
    const NLogging::TLogger& structuredLogger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
