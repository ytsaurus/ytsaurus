#pragma once

#include "chunk_pool.h"
#include "private.h"

#include <yt/server/controller_agent/progress_counter.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/client/table_client/unversioned_row.h>

namespace NYT {
namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

class TJobStub
{
public:
    DEFINE_BYREF_RO_PROPERTY(NTableClient::TKey, LowerPrimaryKey, NTableClient::MaxKey().Get());
    DEFINE_BYREF_RO_PROPERTY(NTableClient::TKey, UpperPrimaryKey, NTableClient::MinKey().Get());

    DEFINE_BYVAL_RO_PROPERTY(int, PrimarySliceCount, 0);
    DEFINE_BYVAL_RO_PROPERTY(int, ForeignSliceCount, 0);
    DEFINE_BYVAL_RO_PROPERTY(int, PreliminaryForeignSliceCount, 0);

    DEFINE_BYVAL_RO_PROPERTY(i64, PrimaryDataWeight, 0);
    DEFINE_BYVAL_RO_PROPERTY(i64, ForeignDataWeight, 0);
    DEFINE_BYVAL_RO_PROPERTY(i64, PreliminaryForeignDataWeight, 0);

    DEFINE_BYVAL_RO_PROPERTY(i64, PrimaryRowCount, 0);
    DEFINE_BYVAL_RO_PROPERTY(i64, ForeignRowCount, 0);
    DEFINE_BYVAL_RO_PROPERTY(i64, PreliminaryForeignRowCount, 0);

    friend class TJobManager;

public:
    TJobStub() = default;

    void AddDataSlice(const NChunkClient::TInputDataSlicePtr& dataSlice, IChunkPoolInput::TCookie cookie, bool isPrimary);
    void AddPreliminaryForeignDataSlice(const NChunkClient::TInputDataSlicePtr& dataSlice);

    //! Removes all empty stripes, sets `Foreign` = true for all foreign stripes,
    //! calculates the statistics for the stripe list and maybe additionally sorts slices
    //! in all stripes according to their original table position in order to satisfy
    //! silly^W tricky sorted operation guarantees.
    void Finalize(bool sortByPosition);

    i64 GetDataWeight() const;
    i64 GetRowCount() const;
    int GetSliceCount() const;

    i64 GetPreliminaryDataWeight() const;
    i64 GetPreliminaryRowCount() const;
    int GetPreliminarySliceCount() const;

    void SetUnsplittable();

private:
    TChunkStripeListPtr StripeList_ = New<TChunkStripeList>();

    //! All the input cookies that provided data that forms this job.
    std::vector<IChunkPoolInput::TCookie> InputCookies_;

    const TChunkStripePtr& GetStripe(int streamIndex, bool isStripePrimary);
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJobState,
    (Pending)
    (Running)
    (Completed)
);

//! A helper class that is used in TSortedChunkPool and TOrderedChunkPool to store all the jobs with their cookies
//! and deal with their suspends, resumes etc.
class TJobManager
    : public TRefCounted
{
public:
    // TODO(max42): Remove data size counter and row counter the hell outta here when YT-6673 is done.
    DEFINE_BYREF_RO_PROPERTY(NControllerAgent::TProgressCounterPtr, DataWeightCounter, New<NControllerAgent::TProgressCounter>());
    DEFINE_BYREF_RO_PROPERTY(NControllerAgent::TProgressCounterPtr, RowCounter, New<NControllerAgent::TProgressCounter>());
    DEFINE_BYREF_RO_PROPERTY(NControllerAgent::TProgressCounterPtr, JobCounter, New<NControllerAgent::TProgressCounter>());
    DEFINE_BYVAL_RO_PROPERTY(int, SuspendedJobCount);

public:
    TJobManager();

    void AddJobs(std::vector<std::unique_ptr<TJobStub>> jobStubs);

    //! Add a job that is built from the given stub.
    IChunkPoolOutput::TCookie AddJob(std::unique_ptr<TJobStub> jobStub);

    void Completed(IChunkPoolOutput::TCookie cookie, NScheduler::EInterruptReason reason);
    void Failed(IChunkPoolOutput::TCookie cookie);
    void Aborted(IChunkPoolOutput::TCookie cookie, NScheduler::EAbortReason reason);
    void Lost(IChunkPoolOutput::TCookie /* cookie */);

    void Suspend(IChunkPoolInput::TCookie inputCookie);
    void Resume(IChunkPoolInput::TCookie inputCookie);

    IChunkPoolOutput::TCookie ExtractCookie();

    void Invalidate(IChunkPoolInput::TCookie inputCookie);

    std::vector<NChunkClient::TInputDataSlicePtr> ReleaseForeignSlices(IChunkPoolInput::TCookie inputCookie);

    void Persist(const TPersistenceContext& context);

    TChunkStripeStatisticsVector GetApproximateStripeStatistics() const;

    int GetPendingJobCount() const;

    const TChunkStripeListPtr& GetStripeList(IChunkPoolOutput::TCookie cookie);

    void InvalidateAllJobs();

    void SetLogger(NLogging::TLogger logger);

private:
    class TStripeListComparator
    {
    public:
        TStripeListComparator(TJobManager* owner);

        bool operator ()(IChunkPoolOutput::TCookie lhs, IChunkPoolOutput::TCookie rhs) const;
    private:
        TJobManager* Owner_;
    };

    //! бассейн с печеньками^W^W^W
    typedef std::multiset<IChunkPoolOutput::TCookie, TStripeListComparator> TCookiePool;

    //! The list of all job cookies that are in state `Pending` (i.e. do not depend on suspended data).
    std::unique_ptr<TCookiePool> CookiePool_;

    //! A mapping between input cookie and all jobs that are affected by its suspension.
    std::vector<std::vector<IChunkPoolOutput::TCookie>> InputCookieToAffectedOutputCookies_;

    //! All jobs before this job were invalidated.
    int FirstValidJobIndex_ = 0;

    //! All input cookies that are currently suspended.
    THashSet<IChunkPoolInput::TCookie> SuspendedInputCookies_;

    //! An internal representation of a finalized job.
    class TJob
    {
    public:
        DEFINE_BYVAL_RO_PROPERTY(EJobState, State, EJobState::Pending);
        DEFINE_BYVAL_RO_PROPERTY(i64, DataWeight);
        DEFINE_BYVAL_RO_PROPERTY(i64, RowCount);
        DEFINE_BYREF_RO_PROPERTY(TChunkStripeListPtr, StripeList);

    public:
        //! Used only for persistence.
        TJob();
        TJob(TJobManager* owner, std::unique_ptr<TJobStub> jobBuilder, IChunkPoolOutput::TCookie cookie);

        void SetState(EJobState state);

        void ChangeSuspendedStripeCountBy(int delta);

        void Invalidate();

        bool IsInvalidated() const;

        void Persist(const TPersistenceContext& context);

        //! A helper for accounting this job in all three progress counters of the owner simultaneously.
        template <class... TArgs>
        void UpdateCounters(void (NControllerAgent::TProgressCounter::*Method)(i64, TArgs...), TArgs... args);

    private:
        TJobManager* Owner_ = nullptr;
        int SuspendedStripeCount_ = 0;
        TCookiePool::iterator CookiePoolIterator_;
        IChunkPoolOutput::TCookie Cookie_;

        //! Is true for a job if it is present in owner's CookiePool_.
        //! Changes of this flag are accompanied with AddSelf()/RemoveSelf().
        bool InPool_ = false;
        //! Is true for a job if it is in the pending state and has suspended stripes.
        //! Changes of this flag are accompanied with SuspendSelf()/ResumeSelf().
        bool Suspended_ = false;
        //! Is true for a job that was invalidated (when pool was rebuilt from scratch).
        bool Invalidated_ = false;

        //! Adds or removes self from the job pool according to the job state and suspended stripe count.
        void UpdateSelf();

        void RemoveSelf();
        void AddSelf();

        void SuspendSelf();
        void ResumeSelf();
    };

    std::vector<TJob> Jobs_;

    NLogging::TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TJobManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
