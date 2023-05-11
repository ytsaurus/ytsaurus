#pragma once

#include "chunk_pool.h"
#include "private.h"
#include "job_manager.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/table_client/key_bound.h>
#include <yt/yt/client/table_client/comparator.h>

#include <yt/yt/core/logging/serializable_logger.h>

#include <random>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

class TNewJobStub
{
public:
    //! Barriers are special entries in job manager internal job list that designate the fact
    //! that adjacent jobs may not be joined together.
    DEFINE_BYVAL_RW_PROPERTY(bool, IsBarrier, false);

    DEFINE_BYVAL_RW_PROPERTY(NTableClient::TKeyBound, PrimaryLowerBound);
    DEFINE_BYVAL_RW_PROPERTY(NTableClient::TKeyBound, PrimaryUpperBound);

    DEFINE_BYVAL_RO_PROPERTY(int, PrimarySliceCount, 0);
    DEFINE_BYVAL_RO_PROPERTY(int, ForeignSliceCount, 0);
    DEFINE_BYVAL_RO_PROPERTY(int, PreliminaryForeignSliceCount, 0);

    DEFINE_BYVAL_RO_PROPERTY(i64, PrimaryDataWeight, 0);
    DEFINE_BYVAL_RO_PROPERTY(i64, ForeignDataWeight, 0);
    DEFINE_BYVAL_RO_PROPERTY(i64, PreliminaryForeignDataWeight, 0);

    DEFINE_BYVAL_RO_PROPERTY(i64, PrimaryRowCount, 0);
    DEFINE_BYVAL_RO_PROPERTY(i64, ForeignRowCount, 0);
    DEFINE_BYVAL_RO_PROPERTY(i64, PreliminaryForeignRowCount, 0);

    DEFINE_BYVAL_RO_PROPERTY(TChunkStripeListPtr, StripeList, New<TChunkStripeList>());

    friend class TNewJobManager;

public:
    TNewJobStub() = default;

    void AddDataSlice(const NChunkClient::TLegacyDataSlicePtr& dataSlice, IChunkPoolInput::TCookie cookie, bool isPrimary);
    // TODO(max42): this seems to be unused, clean up.
    void AddPreliminaryForeignDataSlice(const NChunkClient::TLegacyDataSlicePtr& dataSlice);

    //! Calculates the statistics for the stripe list and moves stripes from stripe map
    //! into stripe list.
    void Finalize();

    i64 GetDataWeight() const;
    i64 GetRowCount() const;
    int GetSliceCount() const;

    i64 GetPreliminaryDataWeight() const;
    i64 GetPreliminaryRowCount() const;
    int GetPreliminarySliceCount() const;

    TString GetDebugString() const;

private:
    //! All the input cookies that provided data that forms this job.
    std::vector<IChunkPoolInput::TCookie> InputCookies_;

    //! Maps pair of <stream_index, range_index> into corresponding stripe.
    THashMap<std::pair<int, int>, TChunkStripePtr> StripeMap_;

    const TChunkStripePtr& GetStripe(int streamIndex, int rangeIndex, bool isStripePrimary);
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TNewJobStub& jobStub, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

//! A helper class that is used in TSortedChunkPool and TOrderedChunkPool to store all the jobs with their cookies
//! and deal with their suspends, resumes etc.
class TNewJobManager
    : public TRefCounted
{
public:
    DEFINE_BYREF_RO_PROPERTY(NControllerAgent::TProgressCounterPtr, DataWeightCounter, New<NControllerAgent::TProgressCounter>());
    DEFINE_BYREF_RO_PROPERTY(NControllerAgent::TProgressCounterPtr, RowCounter, New<NControllerAgent::TProgressCounter>());
    DEFINE_BYREF_RO_PROPERTY(NControllerAgent::TProgressCounterPtr, JobCounter, New<NControllerAgent::TProgressCounter>());
    DEFINE_BYREF_RO_PROPERTY(NControllerAgent::TProgressCounterPtr, DataSliceCounter, New<NControllerAgent::TProgressCounter>());

public:
    //! Used only for persistence.
    TNewJobManager();

    TNewJobManager(const NLogging::TLogger& logger);

    std::vector<IChunkPoolOutput::TCookie> AddJobs(std::vector<std::unique_ptr<TNewJobStub>> jobStubs);

    //! Add a job that is built from the given stub.
    IChunkPoolOutput::TCookie AddJob(std::unique_ptr<TNewJobStub> jobStub);

    void Completed(IChunkPoolOutput::TCookie cookie, NScheduler::EInterruptReason reason);
    void Failed(IChunkPoolOutput::TCookie cookie);
    void Aborted(IChunkPoolOutput::TCookie cookie, NScheduler::EAbortReason reason);
    void Lost(IChunkPoolOutput::TCookie cookie);

    void Suspend(IChunkPoolInput::TCookie inputCookie);
    void Resume(IChunkPoolInput::TCookie inputCookie);

    IChunkPoolOutput::TCookie ExtractCookie();

    void Invalidate(IChunkPoolInput::TCookie inputCookie);

    std::vector<NChunkClient::TLegacyDataSlicePtr> ReleaseForeignSlices(IChunkPoolInput::TCookie inputCookie);

    void Persist(const TPersistenceContext& context);

    NTableClient::TChunkStripeStatisticsVector GetApproximateStripeStatistics() const;

    const TChunkStripeListPtr& GetStripeList(IChunkPoolOutput::TCookie cookie);

    void InvalidateAllJobs();

    //! Perform a pass over all jobs in their order and join some groups of
    //! adjacent jobs that are still smaller than `dataWeightPerJob` in total.
    void Enlarge(
        i64 dataWeightPerJob,
        i64 primaryDataWeightPerJob);

    std::pair<NTableClient::TKeyBound, NTableClient::TKeyBound>
        GetBounds(IChunkPoolOutput::TCookie cookie) const;

private:
    class TStripeListComparator
    {
    public:
        explicit TStripeListComparator(TNewJobManager* owner);

        bool operator ()(IChunkPoolOutput::TCookie lhs, IChunkPoolOutput::TCookie rhs) const;
    private:
        TNewJobManager* Owner_;
    };

    //! бассейн с печеньками^W^W^W
    using TCookiePool = std::multiset<IChunkPoolOutput::TCookie, TStripeListComparator>;

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
        DEFINE_BYVAL_RO_PROPERTY(bool, IsBarrier);
        DEFINE_BYVAL_RO_PROPERTY(i64, DataWeight);
        DEFINE_BYVAL_RO_PROPERTY(i64, RowCount);
        DEFINE_BYVAL_RO_PROPERTY(NTableClient::TKeyBound, LowerBound);
        DEFINE_BYVAL_RO_PROPERTY(NTableClient::TKeyBound, UpperBound);
        DEFINE_BYREF_RO_PROPERTY(TChunkStripeListPtr, StripeList);

        //! All the input cookies that provided data that forms this job.
        DEFINE_BYREF_RW_PROPERTY(std::vector<IChunkPoolInput::TCookie>, InputCookies);

    public:
        //! Used only for persistence.
        TJob() = default;

        TJob(TNewJobManager* owner, std::unique_ptr<TNewJobStub> jobBuilder, IChunkPoolOutput::TCookie cookie);

        void SetState(EJobState state);

        void SetInterruptReason(NScheduler::EInterruptReason reason);

        void ChangeSuspendedStripeCountBy(int delta);

        void Invalidate();

        bool IsInvalidated() const;

        void Remove();

        void Persist(const TPersistenceContext& context);

        template <class... TArgs>
        void CallProgressCounterGuards(void (NControllerAgent::TProgressCounterGuard::*Method)(TArgs...), TArgs... args);

        void UpdateSelf();

    private:
        TNewJobManager* Owner_ = nullptr;
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
        //! If true, this job does not exists for job manager anymore.
        bool Removed_ = false;

        NControllerAgent::TProgressCounterGuard DataWeightProgressCounterGuard_;
        NControllerAgent::TProgressCounterGuard RowProgressCounterGuard_;
        NControllerAgent::TProgressCounterGuard JobProgressCounterGuard_;

        NScheduler::EInterruptReason InterruptReason_ = NScheduler::EInterruptReason::None;

        void RemoveSelf();
        void AddSelf();

        void SuspendSelf();
        void ResumeSelf();
    };

    std::vector<TJob> Jobs_;

    NLogging::TSerializableLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TNewJobManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
