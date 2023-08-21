#pragma once

#include "private.h"
#include "resource.h"

#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/client/table_client/key_bound.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESliceType,
    (Buffer)
    (Solid)
    (Foreign)
);

//! This interface defines a helper object for means of job creation. Think of it as of a staging
//! area for data slices; it reacts on events like "promote current job upper bound to the
//! next interesting endpoint" of "flush".
struct ISortedStagingArea
    : public TRefCounted
{
    //! Promote upper bound for currently built job.
    virtual void PromoteUpperBound(NTableClient::TKeyBound upperBound) = 0;

    //! Put data slices of particular kind into staging area:
    //! - Buffer: regular primary data slice which must be sliced by current upper bound before
    //!   getting into the job.
    //! - Solid: primary data slice which was already row sliced before putting into staging area;
    //!   in this case data slice will not be sliced by upper bound and will be taken as a whole
    //!   into ne next job.
    //! - Foreign: foreign data slice. Despite to previous two kinds, its lower bound may not be
    //!   equal to current upper bound inverse.
    virtual void Put(NChunkClient::TLegacyDataSlicePtr dataSlice, ESliceType sliceType) = 0;

    //! Barriers are used to indicate positions which should not be overlapped by jobs
    //! (in particular, pivot keys and teleport chunks define barriers).
    virtual void PutBarrier() = 0;

    //! Flush data slices into a new single job.
    virtual void Flush() = 0;

    //! Called at the end of processing to finalize some stuff and flush the remaining job (if any).
    virtual void Finish() = 0;

    //! Returns reference to all prepared jobs.
    virtual std::vector<TNewJobStub>& PreparedJobs() = 0;

    //! Total number of data slices in all created jobs.
    //! Used for internal bookkeeping by the outer code.
    virtual i64 GetTotalDataSliceCount() const = 0;

    //! Returns largest of all primary data slice upper bounds. Used to
    //! determine how far current staging area spans to the right.
    virtual NTableClient::TKeyBound GetPrimaryUpperBound() const = 0;

    //! Returns total resource vector of all staged foreign data slices.
    virtual TResourceVector GetForeignResourceVector() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISortedStagingArea)

////////////////////////////////////////////////////////////////////////////////

ISortedStagingAreaPtr CreateSortedStagingArea(
    bool enableKeyGuarantee,
    NTableClient::TComparator primaryComparator,
    NTableClient::TComparator foreignComparator,
    const NTableClient::TRowBufferPtr& rowBuffer,
    i64 initialTotalDataSliceCount,
    i64 maxTotalDataSliceCount,
    const TInputStreamDirectory& inputStreamDirectory,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
