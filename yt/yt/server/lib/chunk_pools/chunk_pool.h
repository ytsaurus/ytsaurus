#pragma once

#include "private.h"

#include <yt/yt/server/lib/controller_agent/progress_counter.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/chunk_pools/chunk_pool.h>
#include <yt/yt/ytlib/chunk_pools/chunk_stripe.h>
#include <yt/yt/ytlib/chunk_pools/chunk_stripe_key.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/logging/logger_owner.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct IPersistentChunkPoolInput
    : public virtual IChunkPoolInput
    , public virtual IPersistent
{ };

DEFINE_REFCOUNTED_TYPE(IPersistentChunkPoolInput)

////////////////////////////////////////////////////////////////////////////////

struct IMultiChunkPoolInput
    : public virtual IPersistentChunkPoolInput
{
    //! Finishes underlying pool with given index.
    //! NB: One should not finish underlying pools directlty.
    //! For now, this method is used for testing purposes only.
    virtual void FinishPool(int poolIndex) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMultiChunkPoolInput)

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolInputBase
    : public virtual IPersistentChunkPoolInput
{
public:
    // IPersistentChunkPoolInput implementation.
    void Finish() override;

    bool IsFinished() const override;

    //! This implementation checks that key is not set (that is true for all standard
    //! chunk pools) and that `stripe` contains data slices, after that it
    //! forwards the call to the internal `Add` method.
    TCookie AddWithKey(TChunkStripePtr stripe, TChunkStripeKey key) override;

    //! This implementation is not ready to go that far.
    void Reset(TCookie cookie, TChunkStripePtr stripe, TInputChunkMappingPtr mapping) override;

    // IPersistent implementation.
    void Persist(const TPersistenceContext& context) override;

protected:
    bool Finished = false;
};

////////////////////////////////////////////////////////////////////////////////

//! This interface is a chunk pool counterpart for a job splitter.
struct IPersistentChunkPoolJobSplittingHost
    : public virtual IChunkPoolJobSplittingHost
    , public virtual IPersistent
{ };

DEFINE_REFCOUNTED_TYPE(IPersistentChunkPoolJobSplittingHost)

////////////////////////////////////////////////////////////////////////////////

struct IPersistentChunkPoolOutput
    : public virtual IChunkPoolOutput
    , public virtual IPersistent
    , public virtual IPersistentChunkPoolJobSplittingHost
{ };

DEFINE_REFCOUNTED_TYPE(IPersistentChunkPoolOutput)

////////////////////////////////////////////////////////////////////////////////

struct IMultiChunkPoolOutput
    : public virtual IPersistentChunkPoolOutput
{
    //! Should be called when all underlying pools are added.
    virtual void Finalize() = 0;

    //! Adds new underlying chunk pool output to multi chunk pool.
    virtual void AddPoolOutput(IPersistentChunkPoolOutputPtr pool, int poolIndex) = 0;

    //! Extracts cookie from underlying pool `underlyingPoolIndexHint' if possible.
    virtual TCookie ExtractFromPool(
        int underlyingPoolIndexHint,
        NNodeTrackerClient::TNodeId nodeId = NNodeTrackerClient::InvalidNodeId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMultiChunkPoolOutput)

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolOutputBase
    : public virtual IPersistentChunkPoolOutput
{
public:
    TOutputOrderPtr GetOutputOrder() const override;

    i64 GetLocality(NNodeTrackerClient::TNodeId nodeId) const override;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolOutputWithCountersBase
    : public TChunkPoolOutputBase
{
public:
    TChunkPoolOutputWithCountersBase();

    void Persist(const TPersistenceContext& context) override;

    const NControllerAgent::TProgressCounterPtr& GetJobCounter() const override;
    const NControllerAgent::TProgressCounterPtr& GetDataWeightCounter() const override;
    const NControllerAgent::TProgressCounterPtr& GetRowCounter() const override;
    const NControllerAgent::TProgressCounterPtr& GetDataSliceCounter() const override;

protected:
    NControllerAgent::TProgressCounterPtr DataWeightCounter;
    NControllerAgent::TProgressCounterPtr RowCounter;
    NControllerAgent::TProgressCounterPtr JobCounter;
    NControllerAgent::TProgressCounterPtr DataSliceCounter;
};

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): maybe make job manager implement IPersistentChunkPoolOutput itself?
template <class TJobManager>
class TChunkPoolOutputWithJobManagerBase
    : public TChunkPoolOutputBase
{
public:
    //! Used only for persistence.
    TChunkPoolOutputWithJobManagerBase() = default;

    TChunkPoolOutputWithJobManagerBase(const NLogging::TLogger& logger);

    NTableClient::TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override;
    IChunkPoolOutput::TCookie Extract(NNodeTrackerClient::TNodeId nodeId) override;
    TChunkStripeListPtr GetStripeList(IChunkPoolOutput::TCookie cookie) override;
    int GetStripeListSliceCount(IChunkPoolOutput::TCookie cookie) const override;
    void Completed(IChunkPoolOutput::TCookie cookie, const NControllerAgent::TCompletedJobSummary& jobSummary) override;
    void Failed(IChunkPoolOutput::TCookie cookie) override;
    void Aborted(IChunkPoolOutput::TCookie cookie, NScheduler::EAbortReason reason) override;
    void Lost(IChunkPoolOutput::TCookie cookie) override;
    const NControllerAgent::TProgressCounterPtr& GetJobCounter() const override;
    const NControllerAgent::TProgressCounterPtr& GetDataWeightCounter() const override;
    const NControllerAgent::TProgressCounterPtr& GetRowCounter() const override;
    const NControllerAgent::TProgressCounterPtr& GetDataSliceCounter() const override;

    void Persist(const TPersistenceContext& context) override;

public:
    DEFINE_SIGNAL_OVERRIDE(void(NChunkClient::TInputChunkPtr, std::any tag), ChunkTeleported);
    DEFINE_SIGNAL_OVERRIDE(void(), Completed);
    DEFINE_SIGNAL_OVERRIDE(void(), Uncompleted);

protected:
    TIntrusivePtr<TJobManager> JobManager_;
};

using TChunkPoolOutputWithLegacyJobManagerBase = TChunkPoolOutputWithJobManagerBase<TLegacyJobManager>;
using TChunkPoolOutputWithNewJobManagerBase = TChunkPoolOutputWithJobManagerBase<TNewJobManager>;

////////////////////////////////////////////////////////////////////////////////

//! A base implementing IsSplittable.
//!
//! Prerequisite (*): chunk pool implementation must call #Completed for each job before
//!   its own completion routine and #RegisterChildCookies for any job splitting event.
//!
//! As a consequence, IsSplittable returns false for jobs that should not be split. The logic
//! is following: if (a) job's parent was asked to split into more than one job and
//! (b) all but one siblings of a job are completed without interruption after reading zero rows,
//! then we mark all descendants of a job unsplittable.
//!
//! (*) If the prerequisite is not met, IsSplittable is guaranteed to always return true
//! and state of this base is trivial, i.e. the derived class may skip persisting the base class.
class TJobSplittingBase
    : public virtual IPersistentChunkPoolJobSplittingHost
    , public virtual NLogging::TLoggerOwner
{
public:
    using TCookie = TOutputCookie;
    static constexpr TCookie NullCookie = -1;

    //! For a non-completed job, returns false if job was a single job split result or if we later
    //! found out that all its siblings were actually empty (in this case we also mark all descendants
    //! of the job empty).
    //! For a completed job return value is undefined (yet safe to call).
    bool IsSplittable(TCookie cookie) const override;

protected:
    //! Registers the children of the job in the job splitting tree. If there is only one child,
    //! it is marked as unsplittable.
    void RegisterChildCookies(TCookie cookie, std::vector<TCookie> childCookies);

    // The method below is a part of an internal interface between a derived class and this base.
    // It does not participate in IChunkPoolOutput::Completed overriding, thus should always
    // be explicitly qualified with "TJobSplittingBase::".

    //! Should be called before to possibly indicate that the job was actually empty.
    void Completed(TCookie cookie, const NControllerAgent::TCompletedJobSummary& jobSummary);

    //! Used in tests to ensure that we do not resize vectors more than needed.
    size_t GetMaxVectorSize() const;

    void Persist(const TPersistenceContext& context) override;

private:
    //! List of children output cookies for split jobs. Empty list corresponds to a job that was not split.
    std::vector<std::vector<IChunkPoolOutput::TCookie>> CookieToChildCookies_;
    //! The number of already known empty children.
    std::vector<int> CookieToEmptyChildCount_;
    //! List of parent output cookies for split jobs. Null cookie corresponds to a job that was originally built.
    std::vector<IChunkPoolOutput::TCookie> CookieToParentCookie_;
    //! Marker indicating of output cookie is splittable. We may mark a completed job as unsplittable
    //! as we do not save information about job being completed, but this is not an issue.
    std::vector<bool> CookieIsSplittable_;
    //! True if split job count is greater than 1, i.e. proper splitting is expected.
    std::vector<bool> CookieShouldBeSplitProperly_;

    //! Mark all descendants of the cookie as unsplittable.
    void MarkDescendantsUnsplittable(TCookie cookie);
};

////////////////////////////////////////////////////////////////////////////////

struct IPersistentChunkPool
    : public virtual IChunkPool
    , public virtual IPersistentChunkPoolInput
    , public virtual IPersistentChunkPoolOutput
{ };

DEFINE_REFCOUNTED_TYPE(IPersistentChunkPool)

////////////////////////////////////////////////////////////////////////////////

struct IMultiChunkPool
    : public virtual IMultiChunkPoolInput
    , public virtual IMultiChunkPoolOutput
    , public virtual IPersistentChunkPool
{
    //! Adds new underlying chunk pool to multi chunk pool.
    virtual void AddPool(IPersistentChunkPoolPtr pool, int poolIndex) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMultiChunkPool)

////////////////////////////////////////////////////////////////////////////////

struct IShuffleChunkPool
    : public virtual TRefCounted
    , public virtual IPersistent
{
    virtual IPersistentChunkPoolInputPtr GetInput() = 0;
    virtual IPersistentChunkPoolOutputPtr GetOutput(int partitionIndex) = 0;
    virtual i64 GetTotalDataSliceCount() const = 0;
    virtual i64 GetTotalJobCount() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IShuffleChunkPool)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

