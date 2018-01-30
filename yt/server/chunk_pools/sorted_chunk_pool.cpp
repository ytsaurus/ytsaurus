#include "sorted_chunk_pool.h"

#include "job_manager.h"
#include "helpers.h"

#include <yt/server/controller_agent/helpers.h>
#include <yt/server/controller_agent/controller_agent.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/table_client/chunk_slice_fetcher.h>
#include <yt/ytlib/table_client/row_buffer.h>

#include <yt/core/concurrency/periodic_yielder.h>

#include <yt/core/misc/numeric_helpers.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYT {
namespace NChunkPools {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NLogging;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

void TSortedJobOptions::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, EnableKeyGuarantee);
    Persist(context, PrimaryPrefixLength);
    Persist(context, ForeignPrefixLength);
    Persist(context, MaxTotalSliceCount);
    Persist(context, EnablePeriodicYielder);
    Persist(context, PivotKeys);
    Persist(context, UseNewEndpointKeys);
    Persist(context, MaxDataWeightPerJob);
}

void TSortedChunkPoolOptions::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, SortedJobOptions);
    Persist(context, MinTeleportChunkSize);
    Persist(context, JobSizeConstraints);
    Persist(context, SupportLocality);
    Persist(context, OperationId);
    Persist(context, Task);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EEndpointType,
    (PivotKey)
    (ForeignLeft)
    (Left)
    (Right)
    (ForeignRight)
);

//! A class that incapsulates the whole logic of building sorted* jobs.
//! This class defines a transient object (it is never persisted).
class TSortedJobBuilder
    : public TRefCounted
{
public:
    TSortedJobBuilder(
        const TSortedJobOptions& options,
        IJobSizeConstraintsPtr jobSizeConstraints,
        const TRowBufferPtr& rowBuffer,
        const std::vector<TInputChunkPtr>& teleportChunks,
        bool inSplit,
        const TLogger& logger)
        : Options_(options)
        , JobSizeConstraints_(std::move(jobSizeConstraints))
        , RowBuffer_(rowBuffer)
        , TeleportChunks_(teleportChunks)
        , InSplit_(inSplit)
        , Logger(logger)
    { }

    void AddForeignDataSlice(const TInputDataSlicePtr& dataSlice, IChunkPoolInput::TCookie cookie)
    {
        DataSliceToInputCookie_[dataSlice] = cookie;

        if (dataSlice->InputStreamIndex >= ForeignDataSlices_.size()) {
            ForeignDataSlices_.resize(dataSlice->InputStreamIndex + 1);
        }
        ForeignDataSlices_[dataSlice->InputStreamIndex].emplace_back(dataSlice);

        // NB: We do not need to shorten keys here. Endpoints of type "Foreign" only make
        // us to stop, to add all foreign slices up to the current moment and to check
        // if we already have to end the job due to the large data size or slice count.
        TEndpoint leftEndpoint = {
            EEndpointType::ForeignLeft,
            dataSlice,
            GetStrictKey(dataSlice->LowerLimit().Key, Options_.PrimaryPrefixLength, RowBuffer_),
            dataSlice->LowerLimit().RowIndex.Get(0)
        };
        TEndpoint rightEndpoint = {
            EEndpointType::ForeignRight,
            dataSlice,
            GetStrictKeySuccessor(dataSlice->UpperLimit().Key, Options_.PrimaryPrefixLength + 1, RowBuffer_),
            dataSlice->UpperLimit().RowIndex.Get(0)
        };

        try {
            ValidateClientKey(leftEndpoint.Key);
            ValidateClientKey(rightEndpoint.Key);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                "Error validating sample key in input stream %v",
                dataSlice->InputStreamIndex)
                    << ex;
        }

        Endpoints_.emplace_back(leftEndpoint);
        Endpoints_.emplace_back(rightEndpoint);
    }

    void AddPrimaryDataSlice(const TInputDataSlicePtr& dataSlice, IChunkPoolInput::TCookie cookie)
    {
        if (dataSlice->LowerLimit().Key >= dataSlice->UpperLimit().Key) {
            // This can happen if ranges were specified.
            // Chunk slice fetcher can produce empty slices.
            return;
        }

        DataSliceToInputCookie_[dataSlice] = cookie;

        TEndpoint leftEndpoint;
        TEndpoint rightEndpoint;

        if (Options_.EnableKeyGuarantee) {
            leftEndpoint = {
                EEndpointType::Left,
                dataSlice,
                GetKeyPrefix(dataSlice->LowerLimit().Key, Options_.PrimaryPrefixLength, RowBuffer_),
                0LL /* RowIndex */
            };

            rightEndpoint = {
                EEndpointType::Right,
                dataSlice,
                GetKeySuccessor(GetKeyPrefix(dataSlice->UpperLimit().Key, Options_.PrimaryPrefixLength, RowBuffer_), RowBuffer_),
                0LL /* RowIndex */
            };
        } else if (Options_.UseNewEndpointKeys) {
            int leftRowIndex = dataSlice->LowerLimit().RowIndex.Get(0);
            leftEndpoint = {
                EEndpointType::Left,
                dataSlice,
                GetStrictKey(dataSlice->LowerLimit().Key, Options_.PrimaryPrefixLength, RowBuffer_, EValueType::Min),
                leftRowIndex
            };

            int rightRowIndex = dataSlice->UpperLimit().RowIndex.Get(
                dataSlice->Type == EDataSourceType::UnversionedTable
                ? dataSlice->GetSingleUnversionedChunkOrThrow()->GetRowCount()
                : 0);

            rightEndpoint = {
                EEndpointType::Right,
                dataSlice,
                GetStrictKey(dataSlice->UpperLimit().Key, Options_.PrimaryPrefixLength, RowBuffer_, EValueType::Max),
                rightRowIndex
            };
        } else {
            // COMPAT(psushin): old behaviour for join reduce and sorted merge (faulty around YT-8156).
            int leftRowIndex = dataSlice->LowerLimit().RowIndex.Get(0);
            leftEndpoint = {
                EEndpointType::Left,
                dataSlice,
                GetStrictKey(dataSlice->LowerLimit().Key, Options_.PrimaryPrefixLength, RowBuffer_, EValueType::Max),
                leftRowIndex
            };

            int rightRowIndex = dataSlice->UpperLimit().RowIndex.Get(
                    dataSlice->Type == EDataSourceType::UnversionedTable
                    ? dataSlice->GetSingleUnversionedChunkOrThrow()->GetRowCount()
                    : 0);

            rightEndpoint = {
                EEndpointType::Right,
                dataSlice,
                GetStrictKey(dataSlice->UpperLimit().Key, Options_.PrimaryPrefixLength, RowBuffer_, EValueType::Max),
                rightRowIndex
            };
        }

        try {
            ValidateClientKey(leftEndpoint.Key);
            ValidateClientKey(rightEndpoint.Key);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                "Error validating sample key in input stream %v",
                dataSlice->InputStreamIndex)
                    << ex;
        }

        Endpoints_.push_back(leftEndpoint);
        Endpoints_.push_back(rightEndpoint);
    }

    std::vector<std::unique_ptr<TJobStub>> Build()
    {
        AddPivotKeysEndpoints();
        SortEndpoints();
        BuildJobs();
        AttachForeignSlices();
        for (auto& job : Jobs_) {
            job->Finalize(true /* sortByPosition */);

            if (job->GetDataWeight() > Options_.MaxDataWeightPerJob) {
                THROW_ERROR_EXCEPTION(
                    "Maximum allowed data weight violated for a sorted job: %v > %v",
                    job->GetDataWeight(),
                    Options_.MaxDataWeightPerJob)
                    << TErrorAttribute("lower_key", job->LowerPrimaryKey())
                    << TErrorAttribute("upper_key", job->UpperPrimaryKey());
            }
        }
        return std::move(Jobs_);
    }

    i64 GetTotalDataSliceCount() const
    {
        return TotalSliceCount_;
    }

private:
    void AddPivotKeysEndpoints()
    {
        for (const auto& pivotKey : Options_.PivotKeys) {
            TEndpoint endpoint = {
                EEndpointType::PivotKey,
                nullptr,
                pivotKey,
                0
            };
            Endpoints_.emplace_back(endpoint);
        }
    }

    void SortEndpoints()
    {
        LOG_DEBUG("Sorting endpoints (Count: %v)", Endpoints_.size());
        std::sort(
            Endpoints_.begin(),
            Endpoints_.end(),
            [=] (const TEndpoint& lhs, const TEndpoint& rhs) -> bool {
                {
                    auto cmpResult = CompareRows(lhs.Key, rhs.Key);
                    if (cmpResult != 0) {
                        return cmpResult < 0;
                    }
                }

                if (lhs.DataSlice && lhs.DataSlice->Type == EDataSourceType::UnversionedTable &&
                    rhs.DataSlice && rhs.DataSlice->Type == EDataSourceType::UnversionedTable)
                {
                    // If keys are equal, we put slices in the same order they are in the original input stream.
                    const auto& lhsChunk = lhs.DataSlice->GetSingleUnversionedChunkOrThrow();
                    const auto& rhsChunk = rhs.DataSlice->GetSingleUnversionedChunkOrThrow();

                    auto cmpResult = (lhsChunk->GetTableRowIndex() + lhs.RowIndex) - (rhsChunk->GetTableRowIndex() + rhs.RowIndex);
                    if (cmpResult != 0) {
                        return cmpResult < 0;
                    }
                }

                {
                    auto cmpResult = static_cast<int>(lhs.Type) - static_cast<int>(rhs.Type);
                    if (cmpResult != 0) {
                        return cmpResult > 0;
                    }
                }

                return false;
            });
    }

    void BuildJobs()
    {
        Jobs_.emplace_back(std::make_unique<TJobStub>());

        yhash<TInputDataSlicePtr, TKey> openedSlicesLowerLimits;

        auto yielder = CreatePeriodicYielder();

        // An index of a closest teleport chunk to the right of current endpoint.
        int nextTeleportChunk = 0;

        auto endJob = [&] (TKey lastKey, bool inclusive) {
            TKey upperLimit = (inclusive) ? GetKeyPrefixSuccessor(lastKey, Options_.PrimaryPrefixLength, RowBuffer_) : lastKey;
            for (auto iterator = openedSlicesLowerLimits.begin(); iterator != openedSlicesLowerLimits.end(); ) {
                // Save the iterator to the next element because we may possibly erase current iterator.
                auto nextIterator = std::next(iterator);
                const auto& dataSlice = iterator->first;
                auto& lowerLimit = iterator->second;
                auto exactDataSlice = CreateInputDataSlice(dataSlice, lowerLimit, upperLimit);
                auto inputCookie = DataSliceToInputCookie_.at(dataSlice);
                exactDataSlice->Tag = inputCookie;
                Jobs_.back()->AddDataSlice(
                    exactDataSlice,
                    inputCookie,
                    true /* isPrimary */);
                lowerLimit = upperLimit;
                if (lowerLimit >= dataSlice->UpperLimit().Key) {
                    openedSlicesLowerLimits.erase(iterator);
                }
                iterator = nextIterator;
            }
            // If current job does not contain primary data slices then we can re-use it,
            // otherwise we should create a new job.
            if (Jobs_.back()->GetSliceCount() > 0) {
                LOG_DEBUG("Sorted job created (Index: %v, PrimaryDataSize: %v, PrimaryRowCount: %v, "
                    "PrimarySliceCount: %v, PreliminaryForeignDataSize: %v, PreliminaryForeignRowCount: %v, "
                    "PreliminaryForeignSliceCount: %v, LowerPrimaryKey: %v, UpperPrimaryKey: %v)",
                    static_cast<int>(Jobs_.size()) - 1,
                    Jobs_.back()->GetPrimaryDataWeight(),
                    Jobs_.back()->GetPrimaryRowCount(),
                    Jobs_.back()->GetPrimarySliceCount(),
                    Jobs_.back()->GetPreliminaryForeignDataWeight(),
                    Jobs_.back()->GetPreliminaryForeignRowCount(),
                    Jobs_.back()->GetPreliminaryForeignSliceCount(),
                    Jobs_.back()->LowerPrimaryKey(),
                    Jobs_.back()->UpperPrimaryKey());

                TotalSliceCount_ += Jobs_.back()->GetSliceCount();
                CheckTotalSliceCountLimit();
                Jobs_.emplace_back(std::make_unique<TJobStub>());
            }
        };

        for (int index = 0, nextKeyIndex = 0; index < Endpoints_.size(); ++index) {
            yielder.TryYield();
            auto key = Endpoints_[index].Key;
            while (nextKeyIndex != Endpoints_.size() && Endpoints_[nextKeyIndex].Key == key) {
                ++nextKeyIndex;
            }

            auto nextKey = (nextKeyIndex == Endpoints_.size()) ? TKey() : Endpoints_[nextKeyIndex].Key;
            bool nextKeyIsLeft = (nextKeyIndex == Endpoints_.size()) ? false : Endpoints_[nextKeyIndex].Type == EEndpointType::Left;

            while (nextTeleportChunk < TeleportChunks_.size() &&
                   CompareRows(TeleportChunks_[nextTeleportChunk]->BoundaryKeys()->MinKey, key, Options_.PrimaryPrefixLength) < 0)
            {
                ++nextTeleportChunk;
            }

            if (Endpoints_[index].Type == EEndpointType::Left) {
                openedSlicesLowerLimits[Endpoints_[index].DataSlice] = TKey();
            } else if (Endpoints_[index].Type == EEndpointType::Right) {
                const auto& dataSlice = Endpoints_[index].DataSlice;
                auto it = openedSlicesLowerLimits.find(dataSlice);
                // It might have happened that we already removed this slice from the
                // `openedSlicesLowerLimits` during one of the previous `endJob` calls.
                if (it != openedSlicesLowerLimits.end()) {
                    auto exactDataSlice = CreateInputDataSlice(dataSlice, it->second);
                    auto inputCookie = DataSliceToInputCookie_.at(dataSlice);
                    exactDataSlice->Tag = inputCookie;
                    Jobs_.back()->AddDataSlice(exactDataSlice, inputCookie, true /* isPrimary */);
                    openedSlicesLowerLimits.erase(it);
                }
            } else if (Endpoints_[index].Type == EEndpointType::ForeignRight) {
                Jobs_.back()->AddPreliminaryForeignDataSlice(Endpoints_[index].DataSlice);
            }

            // Is set to true if we decide to end here. The decision logic may be
            // different depending on if we have user-provided pivot keys.
            bool endHere = false;

            if (Options_.PivotKeys.empty()) {
                bool jobIsLargeEnough =
                    Jobs_.back()->GetPreliminarySliceCount() + openedSlicesLowerLimits.size() > JobSizeConstraints_->GetMaxDataSlicesPerJob() ||
                    Jobs_.back()->GetPreliminaryDataWeight() >= JobSizeConstraints_->GetDataWeightPerJob() ||
                    Jobs_.back()->GetPrimaryDataWeight() >= JobSizeConstraints_->GetPrimaryDataWeightPerJob();

                // If next teleport chunk is closer than next data slice then we are obligated to close the job here.
                bool beforeTeleportChunk = nextKeyIndex == index + 1 &&
                    nextKeyIsLeft &&
                    (nextTeleportChunk != TeleportChunks_.size() &&
                    CompareRows(TeleportChunks_[nextTeleportChunk]->BoundaryKeys()->MinKey, nextKey, Options_.PrimaryPrefixLength) <= 0);

                // If key guarantee is enabled, we can not end here if next data slice may contain the same reduce key.
                bool canEndHere = !Options_.EnableKeyGuarantee || index + 1 == nextKeyIndex;

                // The contrary would mean that teleport chunk was chosen incorrectly, because teleport chunks
                // should not normally intersect the other data slices.
                YCHECK(!(beforeTeleportChunk && !canEndHere));

                endHere = canEndHere && (beforeTeleportChunk || jobIsLargeEnough);
            } else {
                // We may end jobs only at the pivot keys.
                endHere = Endpoints_[index].Type == EEndpointType::PivotKey;
            }

            if (endHere) {
                bool inclusive = Endpoints_[index].Type != EEndpointType::PivotKey;
                endJob(key, inclusive);
            }
        }
        endJob(MaxKey(), true /* inclusive */);
        if (!Jobs_.empty() && Jobs_.back()->GetSliceCount() == 0) {
            Jobs_.pop_back();
        }
        LOG_DEBUG("Jobs created (Count: %v)", Jobs_.size());
        if (InSplit_ && Jobs_.size() == 1 && JobSizeConstraints_->GetJobCount() > 1) {
            LOG_WARNING("Pool was not able to split job properly (SplitJobCount: %v, JobCount: %v)",
                JobSizeConstraints_->GetJobCount(),
                Jobs_.size());

            Jobs_.front()->SetUnsplittable();
            for (int index = 0; index < Endpoints_.size(); ++index) {
                const auto& endpoint = Endpoints_[index];
                LOG_DEBUG("Endpoint (Index: %v, Key: %v, RowIndex: %v, GlobalRowIndex: %v, Type: %v, DataSlice: %p)",
                    index,
                    endpoint.Key,
                    endpoint.RowIndex,
                    (endpoint.DataSlice && endpoint.DataSlice->Type == EDataSourceType::UnversionedTable)
                    ? MakeNullable(endpoint.DataSlice->GetSingleUnversionedChunkOrThrow()->GetTableRowIndex() + endpoint.RowIndex)
                    : Null,
                    endpoint.Type,
                    endpoint.DataSlice.Get());
            }
            for (const auto& pair : DataSliceToInputCookie_) {
                const auto& dataSlice = pair.first;
                LOG_DEBUG("Data slice %v (DataWeight: %v, InputStreamIndex: %v)",
                    dataSlice.Get(),
                    dataSlice->GetDataWeight(),
                    dataSlice->InputStreamIndex);
            }
        }
    }

    void AttachForeignSlices()
    {
        auto yielder = CreatePeriodicYielder();
        for (int streamIndex = 0; streamIndex < ForeignDataSlices_.size(); ++streamIndex) {
            yielder.TryYield();

            int startJobIndex = 0;

            for (const auto& foreignDataSlice : ForeignDataSlices_[streamIndex]) {

                while (
                    startJobIndex != Jobs_.size() &&
                    CompareRows(Jobs_[startJobIndex]->UpperPrimaryKey(), foreignDataSlice->LowerLimit().Key, Options_.ForeignPrefixLength) < 0)
                {
                    ++startJobIndex;
                }
                if (startJobIndex == Jobs_.size()) {
                    break;
                }
                for (
                    int jobIndex = startJobIndex;
                    jobIndex < Jobs_.size() &&
                    CompareRows(Jobs_[jobIndex]->LowerPrimaryKey(), foreignDataSlice->UpperLimit().Key, Options_.ForeignPrefixLength) <= 0;
                    ++jobIndex)
                {
                    yielder.TryYield();

                    auto exactForeignDataSlice = CreateInputDataSlice(
                        foreignDataSlice,
                        GetKeyPrefix(Jobs_[jobIndex]->LowerPrimaryKey(), Options_.ForeignPrefixLength, RowBuffer_),
                        GetKeyPrefixSuccessor(Jobs_[jobIndex]->UpperPrimaryKey(), Options_.ForeignPrefixLength, RowBuffer_));
                    auto inputCookie = DataSliceToInputCookie_.at(foreignDataSlice);
                    exactForeignDataSlice->Tag = inputCookie;
                    ++TotalSliceCount_;
                    Jobs_[jobIndex]->AddDataSlice(
                        exactForeignDataSlice,
                        inputCookie,
                        false /* isPrimary */);
                }
            }
            CheckTotalSliceCountLimit();
        }
    }

    TPeriodicYielder CreatePeriodicYielder()
    {
        if (Options_.EnablePeriodicYielder) {
            return TPeriodicYielder(PrepareYieldPeriod);
        } else {
            return TPeriodicYielder();
        }
    }

    void CheckTotalSliceCountLimit() const
    {
        if (TotalSliceCount_ > Options_.MaxTotalSliceCount) {
            THROW_ERROR_EXCEPTION("Total number of data slices in sorted pool is too large.")
                << TErrorAttribute("actual_total_slice_count", TotalSliceCount_)
                << TErrorAttribute("max_total_slice_count", Options_.MaxTotalSliceCount)
                << TErrorAttribute("current_job_count", Jobs_.size());
        }
    }

    TSortedJobOptions Options_;

    IJobSizeConstraintsPtr JobSizeConstraints_;

    TRowBufferPtr RowBuffer_;

    struct TEndpoint
    {
        EEndpointType Type;
        TInputDataSlicePtr DataSlice;
        TKey Key;
        i64 RowIndex;
    };

    //! Endpoints of primary table slices in SortedReduce and SortedMerge.
    std::vector<TEndpoint> Endpoints_;

    //! Vector keeping the pool-side state of all jobs that depend on the data from this pool.
    //! These items are merely stubs of a future jobs that are filled during the BuildJobsBy{Key/TableIndices}()
    //! call, and when current job is finished it is passed to the `JobManager_` that becomes responsible
    //! for its future.
    std::vector<std::unique_ptr<TJobStub>> Jobs_;

    //! Stores correspondence between primary data slices added via `AddPrimaryDataSlice`
    //! (both unversioned and versioned) and their input cookies.
    yhash<TInputDataSlicePtr, IChunkPoolInput::TCookie> DataSliceToInputCookie_;

    std::vector<std::vector<TInputDataSlicePtr>> ForeignDataSlices_;

    //! Stores the number of slices in all jobs up to current moment.
    i64 TotalSliceCount_ = 0;

    const std::vector<TInputChunkPtr>& TeleportChunks_;

    bool InSplit_ = false;

    const TLogger& Logger;
};

DEFINE_REFCOUNTED_TYPE(TSortedJobBuilder);
DECLARE_REFCOUNTED_TYPE(TSortedJobBuilder);

////////////////////////////////////////////////////////////////////////////////

class TSortedChunkPool
    : public TChunkPoolInputBase
    , public TChunkPoolOutputWithJobManagerBase
    , public IChunkPool
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
    , public TRefTracked<TSortedChunkPool>
{
public:
    //! Used only for persistence.
    TSortedChunkPool()
    { }

    TSortedChunkPool(
        const TSortedChunkPoolOptions& options,
        IChunkSliceFetcherFactoryPtr chunkSliceFetcherFactory,
        TInputStreamDirectory inputStreamDirectory)
        : SortedJobOptions_(options.SortedJobOptions)
        , ChunkSliceFetcherFactory_(std::move(chunkSliceFetcherFactory))
        , EnableKeyGuarantee_(options.SortedJobOptions.EnableKeyGuarantee)
        , InputStreamDirectory_(std::move(inputStreamDirectory))
        , PrimaryPrefixLength_(options.SortedJobOptions.PrimaryPrefixLength)
        , ForeignPrefixLength_(options.SortedJobOptions.ForeignPrefixLength)
        , MinTeleportChunkSize_(options.MinTeleportChunkSize)
        , JobSizeConstraints_(options.JobSizeConstraints)
        , SupportLocality_(options.SupportLocality)
        , OperationId_(options.OperationId)
        , Task_(options.Task)
    {
        ForeignStripeCookiesByStreamIndex_.resize(InputStreamDirectory_.GetDescriptorCount());
        Logger.AddTag("ChunkPoolId: %v", ChunkPoolId_);
        Logger.AddTag("OperationId: %v", OperationId_);
        Logger.AddTag("Task: %v", Task_);
        JobManager_->SetLogger(Logger);

        LOG_DEBUG("Sorted chunk pool created (EnableKeyGuarantee: %v, PrimaryPrefixLength: %v, "
            "ForeignPrefixLenght: %v, UseNewEndpointKeys: %v, DataWeightPerJob: %v, "
            "PrimaryDataWeightPerJob: %v, MaxDataSlicesPerJob: %v)",
            SortedJobOptions_.EnableKeyGuarantee,
            SortedJobOptions_.PrimaryPrefixLength,
            SortedJobOptions_.ForeignPrefixLength,
            SortedJobOptions_.UseNewEndpointKeys,
            JobSizeConstraints_->GetDataWeightPerJob(),
            JobSizeConstraints_->GetPrimaryDataWeightPerJob(),
            JobSizeConstraints_->GetMaxDataSlicesPerJob());
    }

    virtual IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        YCHECK(!Finished);

        if (stripe->DataSlices.empty()) {
            return IChunkPoolInput::NullCookie;
        }

        auto cookie = static_cast<int>(Stripes_.size());
        Stripes_.emplace_back(stripe);

        int streamIndex = stripe->GetInputStreamIndex();

        if (InputStreamDirectory_.GetDescriptor(streamIndex).IsForeign()) {
            ForeignStripeCookiesByStreamIndex_[streamIndex].push_back(cookie);
        }

        return cookie;
    }

    virtual void Finish() override
    {
        YCHECK(!Finished);
        TChunkPoolInputBase::Finish();

        // NB: this method accounts all the stripes that were suspended before
        // the chunk pool was finished. It should be called only once.
        SetupSuspendedStripes();

        DoFinish();
    }

    virtual void Suspend(IChunkPoolInput::TCookie cookie) override
    {
        auto& suspendableStripe = Stripes_[cookie];
        suspendableStripe.Suspend();
        if (Finished) {
            JobManager_->Suspend(cookie);
        }
    }

    virtual void Resume(IChunkPoolInput::TCookie cookie, TChunkStripePtr stripe) override
    {
        auto& suspendableStripe = Stripes_[cookie];
        if (!Finished) {
            suspendableStripe.Resume(stripe);
            suspendableStripe.ReplaceOriginalStripe();
        } else {
            JobManager_->Resume(cookie);
            yhash<TInputChunkPtr, TInputChunkPtr> newChunkMapping;
            try {
                newChunkMapping = suspendableStripe.ResumeAndBuildChunkMapping(stripe);
            } catch (std::exception& ex) {
                suspendableStripe.Resume(stripe);
                auto error = TError("Chunk stripe resumption failed")
                    << ex
                    << TErrorAttribute("input_cookie", cookie);
                LOG_WARNING(error, "Rebuilding all jobs because of error during resumption");
                InvalidateCurrentJobs();
                DoFinish();
                PoolOutputInvalidated_.Fire(error);
                return;
            }
            for (const auto& pair : newChunkMapping) {
                InputChunkMapping_[pair.first] = pair.second;
            }
        }
    }

    virtual bool IsCompleted() const override
    {
        return
            Finished &&
            GetPendingJobCount() == 0 &&
            JobManager_->JobCounter()->GetRunning() == 0 &&
            JobManager_->GetSuspendedJobCount() == 0;
    }

    virtual TChunkStripeListPtr GetStripeList(IChunkPoolOutput::TCookie cookie) override
    {
        return ApplyChunkMappingToStripe(JobManager_->GetStripeList(cookie), InputChunkMapping_);
    }

    virtual void Completed(IChunkPoolOutput::TCookie cookie, const TCompletedJobSummary& jobSummary) override
    {
        if (jobSummary.InterruptReason != EInterruptReason::None) {
            LOG_DEBUG("Splitting job (OutputCookie: %v, InterruptReason: %v, SplitJobCount: %v)",
                cookie,
                jobSummary.InterruptReason,
                jobSummary.SplitJobCount);
            auto foreignSlices = JobManager_->ReleaseForeignSlices(cookie);
            JobManager_->Invalidate(cookie);
            SplitJob(std::move(jobSummary.UnreadInputDataSlices), std::move(foreignSlices), jobSummary.SplitJobCount);
        }
        JobManager_->Completed(cookie, jobSummary.InterruptReason);
    }

    virtual i64 GetDataSliceCount() const override
    {
        return TotalDataSliceCount_;
    }

    virtual void Persist(const TPersistenceContext& context) final override
    {
        TChunkPoolInputBase::Persist(context);
        TChunkPoolOutputWithJobManagerBase::Persist(context);

        using NYT::Persist;
        Persist(context, ForeignStripeCookiesByStreamIndex_);
        Persist<TMapSerializer<TDefaultSerializer, TDefaultSerializer, TUnsortedTag>>(context, InputChunkMapping_);
        Persist(context, Stripes_);
        Persist(context, EnableKeyGuarantee_);
        Persist(context, InputStreamDirectory_);
        Persist(context, PrimaryPrefixLength_);
        Persist(context, ForeignPrefixLength_);
        Persist(context, MinTeleportChunkSize_);
        Persist(context, JobSizeConstraints_);
        Persist(context, ChunkSliceFetcherFactory_);
        Persist(context, SupportLocality_);
        Persist(context, OperationId_);
        Persist(context, Task_);
        Persist(context, ChunkPoolId_);
        Persist(context, SortedJobOptions_);
        Persist(context, ForeignStripeCookiesByStreamIndex_);
        Persist(context, TotalDataSliceCount_);

        if (context.IsLoad()) {
            Logger.AddTag("ChunkPoolId: %v", ChunkPoolId_);
            Logger.AddTag("OperationId: %v", OperationId_);
            Logger.AddTag("Task: %v", Task_);
            JobManager_->SetLogger(Logger);
        }
    }

public:
    DEFINE_SIGNAL(void(const TError& error), PoolOutputInvalidated)

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortedChunkPool, 0x91bca805);

    //! All options necessary for sorted job builder.
    TSortedJobOptions SortedJobOptions_;

    //! A factory that is used to spawn chunk slice fetcher.
    IChunkSliceFetcherFactoryPtr ChunkSliceFetcherFactory_;

    //! During the pool lifetime some input chunks may be suspended and replaced with
    //! another chunks on resumption. We keep track of all such substitutions in this
    //! map and apply it whenever the `GetStripeList` is called.
    yhash<TInputChunkPtr, TInputChunkPtr> InputChunkMapping_;

    //! Guarantee that each key goes to the single job.
    bool EnableKeyGuarantee_;

    //! Information about input sources (e.g. input tables for sorted reduce operation).
    TInputStreamDirectory InputStreamDirectory_;

    //! Length of the key according to which primary tables should be sorted during
    //! sorted reduce / sorted merge.
    int PrimaryPrefixLength_;

    //! Length of the key that defines a range in foreign tables that should be joined
    //! to the job.
    int ForeignPrefixLength_;

    //! An option to control chunk teleportation logic. Only large complete
    //! chunks of at least that size will be teleported.
    i64 MinTeleportChunkSize_;

    //! All stripes that were added to this pool.
    std::vector<TSuspendableStripe> Stripes_;

    //! Stores input cookies of all foreign stripes grouped by input stream index.
    std::vector<std::vector<int>> ForeignStripeCookiesByStreamIndex_;

    IJobSizeConstraintsPtr JobSizeConstraints_;

    bool SupportLocality_ = false;

    TLogger Logger = ChunkPoolLogger;

    TOperationId OperationId_;
    TString Task_;

    TGuid ChunkPoolId_ = TGuid::Create();

    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    i64 TotalDataSliceCount_ = 0;

    void InitInputChunkMapping()
    {
        for (const auto& suspendableStripe : Stripes_) {
            for (const auto& dataSlice : suspendableStripe.GetStripe()->DataSlices) {
                for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                    InputChunkMapping_[chunkSlice->GetInputChunk()] = chunkSlice->GetInputChunk();
                }
            }
        }
    }

    //! This method processes all input stripes that do not correspond to teleported chunks
    //! and either slices them using ChunkSliceFetcher (for unversioned stripes) or leaves them as is
    //! (for versioned stripes).
    void FetchNonTeleportPrimaryDataSlices(const TSortedJobBuilderPtr& builder)
    {
        auto chunkSliceFetcher = ChunkSliceFetcherFactory_ ? ChunkSliceFetcherFactory_->CreateChunkSliceFetcher() : nullptr;

        // If chunkSliceFetcher == nullptr, we form chunk slices manually by putting them
        // into this vector.
        std::vector<TInputChunkSlicePtr> unversionedChunkSlices;

        yhash<TInputChunkPtr, IChunkPoolInput::TCookie> unversionedInputChunkToInputCookie;
        yhash<TInputChunkPtr, int> unversionedInputChunkToInputStreamIndex;

        std::vector<std::pair<TInputDataSlicePtr, IChunkPoolInput::TCookie>> nonTeleportPrimaryDataSlices;

        for (int inputCookie = 0; inputCookie < Stripes_.size(); ++inputCookie) {
            const auto& suspendableStripe = Stripes_[inputCookie];
            const auto& stripe = suspendableStripe.GetStripe();

            if (suspendableStripe.GetTeleport() || !InputStreamDirectory_.GetDescriptor(stripe->GetInputStreamIndex()).IsPrimary()) {
                continue;
            }

            for (const auto& dataSlice : stripe->DataSlices) {
                // Unversioned data slices should be additionally sliced using chunkSliceFetcher,
                // while versioned slices are taken as is.
                if (dataSlice->Type == EDataSourceType::UnversionedTable) {
                    auto inputChunk = dataSlice->GetSingleUnversionedChunkOrThrow();
                    if (chunkSliceFetcher) {
                        chunkSliceFetcher->AddChunk(inputChunk);
                    } else {
                        auto chunkSlice = CreateInputChunkSlice(inputChunk);
                        InferLimitsFromBoundaryKeys(chunkSlice, RowBuffer_, PrimaryPrefixLength_);
                        unversionedChunkSlices.emplace_back(std::move(chunkSlice));
                    }

                    unversionedInputChunkToInputCookie[inputChunk] = inputCookie;
                    unversionedInputChunkToInputStreamIndex[inputChunk] = stripe->GetInputStreamIndex();
                } else {
                    builder->AddPrimaryDataSlice(dataSlice, inputCookie);
                }
            }
        }

        if (chunkSliceFetcher) {
            WaitFor(chunkSliceFetcher->Fetch())
                .ThrowOnError();
            unversionedChunkSlices = chunkSliceFetcher->GetChunkSlices();
        }

        for (const auto& chunkSlice : unversionedChunkSlices) {
            int inputCookie = unversionedInputChunkToInputCookie.at(chunkSlice->GetInputChunk());
            int inputStreamIndex = unversionedInputChunkToInputStreamIndex.at(chunkSlice->GetInputChunk());

            // We additionally slice maniac slices by evenly by row indices.
            auto chunk = chunkSlice->GetInputChunk();
            if (!EnableKeyGuarantee_ &&
                chunk->IsCompleteChunk() &&
                CompareRows(chunk->BoundaryKeys()->MinKey, chunk->BoundaryKeys()->MaxKey, PrimaryPrefixLength_) == 0 &&
                chunkSlice->GetDataWeight() > JobSizeConstraints_->GetInputSliceDataWeight())
            {
                auto smallerSlices = chunkSlice->SliceEvenly(
                    JobSizeConstraints_->GetInputSliceDataWeight(),
                    JobSizeConstraints_->GetInputSliceRowCount());
                for (const auto& smallerSlice : smallerSlices) {
                    auto dataSlice = CreateUnversionedInputDataSlice(smallerSlice);
                    dataSlice->InputStreamIndex = inputStreamIndex;
                    builder->AddPrimaryDataSlice(dataSlice, inputCookie);
                }
            } else {
                auto dataSlice = CreateUnversionedInputDataSlice(chunkSlice);
                dataSlice->InputStreamIndex = inputStreamIndex;
                builder->AddPrimaryDataSlice(dataSlice, inputCookie);
            }
        }
        unversionedInputChunkToInputCookie.clear();
    }

    //! In this function all data slices that correspond to teleportable unversioned input chunks
    //! are added to `TeleportChunks_`.
    void FindTeleportChunks()
    {
        // Consider each chunk as a segment [minKey, maxKey]. Chunk may be teleported if:
        // 1) it is unversioned;
        // 2) it is complete (i. e. does not contain non-trivial read limits);
        // 3a) in case of SortedMerge: no other key (belonging to the different input chunk) lies in the interval (minKey, maxKey);
        // 3b) in case of SortedReduce: no other key lies in the s [minKey', maxKey'] (NB: if some other chunk shares endpoint
        //     with our chunk, our chunk can not be teleported since all instances of each key must be either teleported or
        //     be processed in the same job).
        //
        // We use the following logic to determine how different kinds of intervals are located on a line:
        //
        // * with respect to the s [a; b] data slice [l; r) is located:
        // *** to the left iff r <= a;
        // *** to the right iff l > b;
        // * with respect to the interval (a; b) interval [l; r) is located:
        // *** to the left iff r <= succ(a);
        // *** to the right iff l >= b.
        //
        // Using it, we find how many upper/lower keys are located to the left/right of candidate s/interval using the binary search
        // over the vectors of all lower and upper limits (including or excluding the chunk endpoints if `includeChunkBoundaryKeys`)
        // and check that in total there are exactly |ss| - 1 segments, this would exactly mean that all data slices (except the one
        // that corresponds to the chosen chunk) are either to the left or to the right of the chosen chunk.
        //
        // Unfortunately, there is a tricky corner case that should be treated differently: when chunk pool type is SortedMerge
        // (i. e. `includeChunkBoundaryKeys` is true) and the interval (a, b) is empty. For example, consider the following data slices:
        // (1) ["a"; "n",max), (2) ["n"; "n",max), (3) ["n"; "z",max) and suppose that we test the chunk (*) ["n"; "n"] to be teleportable.
        // In fact it is teleportable: (*) can be located between slices (1) and (2) or between slices (2) and (3). But if we try
        // to use the approach described above and calculate number of slices to the left of ("n"; "n") and slices to the right of ("n"; "n"),
        // we will account slice (2) twice: it is located to the left of ("n"; "n") because "n",max <= succ("n") and it is also located
        // to the right of ("n"; "n") because "n" >= "n". In the other words, when chunk pool type is SortedMerge teleporting of single-key chunks
        // is a hard work :(
        //
        // To overcome this difficulty, we additionally subtract the number of single-key slices that define the same key as our chunk.
        auto yielder = CreatePeriodicYielder();

        if (!SortedJobOptions_.PivotKeys.empty()) {
            return;
        }

        std::vector<TKey> lowerLimits, upperLimits;
        yhash<TKey, int> singleKeySliceNumber;
        std::vector<std::pair<TInputChunkPtr, IChunkPoolInput::TCookie>> teleportCandidates;

        for (int inputCookie = 0; inputCookie < Stripes_.size(); ++inputCookie) {
            const auto& stripe = Stripes_[inputCookie].GetStripe();
            if (InputStreamDirectory_.GetDescriptor(stripe->GetInputStreamIndex()).IsPrimary()) {
                for (const auto& dataSlice : stripe->DataSlices) {
                    yielder.TryYield();

                    if (InputStreamDirectory_.GetDescriptor(stripe->GetInputStreamIndex()).IsTeleportable() &&
                        dataSlice->GetSingleUnversionedChunkOrThrow()->IsLargeCompleteChunk(MinTeleportChunkSize_))
                    {
                        teleportCandidates.emplace_back(dataSlice->GetSingleUnversionedChunkOrThrow(), inputCookie);
                    }

                    lowerLimits.emplace_back(GetKeyPrefix(dataSlice->LowerLimit().Key, PrimaryPrefixLength_, RowBuffer_));
                    if (dataSlice->UpperLimit().Key.GetCount() > PrimaryPrefixLength_) {
                        upperLimits.emplace_back(GetKeySuccessor(GetKeyPrefix(dataSlice->UpperLimit().Key, PrimaryPrefixLength_, RowBuffer_), RowBuffer_));
                    } else {
                        upperLimits.emplace_back(dataSlice->UpperLimit().Key);
                    }

                    if (CompareRows(dataSlice->LowerLimit().Key, dataSlice->UpperLimit().Key, PrimaryPrefixLength_) == 0) {
                        ++singleKeySliceNumber[lowerLimits.back()];
                    }
                }
            }
        }

        if (teleportCandidates.empty()) {
            return;
        }

        std::sort(lowerLimits.begin(), lowerLimits.end());
        std::sort(upperLimits.begin(), upperLimits.end());
        yielder.TryYield();

        int dataSlicesCount = lowerLimits.size();

        for (const auto& pair : teleportCandidates) {
            yielder.TryYield();

            const auto& teleportCandidate = pair.first;
            auto cookie = pair.second;

            // NB: minKey and maxKey are inclusive, in contrast to the lower/upper limits.
            auto minKey = GetKeyPrefix(teleportCandidate->BoundaryKeys()->MinKey, PrimaryPrefixLength_, RowBuffer_);
            auto maxKey = GetKeyPrefix(teleportCandidate->BoundaryKeys()->MaxKey, PrimaryPrefixLength_, RowBuffer_);

            int slicesToTheLeft = (EnableKeyGuarantee_
                ? std::upper_bound(upperLimits.begin(), upperLimits.end(), minKey)
                : std::upper_bound(upperLimits.begin(), upperLimits.end(), GetKeySuccessor(minKey, RowBuffer_))) - upperLimits.begin();
            int slicesToTheRight = lowerLimits.end() - (EnableKeyGuarantee_
                ? std::upper_bound(lowerLimits.begin(), lowerLimits.end(), maxKey)
                : std::lower_bound(lowerLimits.begin(), lowerLimits.end(), maxKey));
            int extraCoincidingSingleKeySlices = 0;
            if (minKey == maxKey && !EnableKeyGuarantee_) {
                auto it = singleKeySliceNumber.find(minKey);
                YCHECK(it != singleKeySliceNumber.end());
                // +1 because we accounted data slice for the current chunk twice (in slicesToTheLeft and slicesToTheRight),
                // but we actually want to account it zero time since we condier only data slices different from current.
                extraCoincidingSingleKeySlices = it->second + 1;
            }
            int nonIntersectingSlices = slicesToTheLeft + slicesToTheRight - extraCoincidingSingleKeySlices;
            YCHECK(nonIntersectingSlices <= dataSlicesCount - 1);
            if (nonIntersectingSlices == dataSlicesCount - 1) {
                Stripes_[cookie].SetTeleport(true);
                TeleportChunks_.emplace_back(teleportCandidate);
            }
        }

        // The last step is to sort the resulting teleport chunks in order to be able to use
        // them while we build the jobs (they provide us with mandatory places where we have to
        // break the jobs).
        std::sort(
            TeleportChunks_.begin(),
            TeleportChunks_.end(),
            [] (const TInputChunkPtr& lhs, const TInputChunkPtr& rhs) {
                int cmpMin = CompareRows(lhs->BoundaryKeys()->MinKey, rhs->BoundaryKeys()->MinKey);
                if (cmpMin != 0) {
                    return cmpMin < 0;
                }
                int cmpMax = CompareRows(lhs->BoundaryKeys()->MaxKey, rhs->BoundaryKeys()->MaxKey);
                if (cmpMax != 0) {
                    return cmpMax < 0;
                }
                // This is possible only when both chunks contain the same only key.
                YCHECK(lhs->BoundaryKeys()->MinKey == lhs->BoundaryKeys()->MaxKey);
                return false;
            });

        i64 totalTeleportChunkSize = 0;
        for (const auto& teleportChunk : TeleportChunks_) {
            totalTeleportChunkSize += teleportChunk->GetUncompressedDataSize();
        }

        LOG_DEBUG("Chunks teleported (ChunkCount: %v, TotalSize: %v)",
            TeleportChunks_.size(),
            totalTeleportChunkSize);
    }

    void PrepareForeignDataSlices(const TSortedJobBuilderPtr& builder)
    {
        auto yielder = CreatePeriodicYielder();

        std::vector<std::pair<TInputDataSlicePtr, IChunkPoolInput::TCookie>> foreignDataSlices;

        for (int streamIndex = 0; streamIndex < ForeignStripeCookiesByStreamIndex_.size(); ++streamIndex) {
            if (!InputStreamDirectory_.GetDescriptor(streamIndex).IsForeign()) {
                continue;
            }

            yielder.TryYield();

            auto& stripeCookies = ForeignStripeCookiesByStreamIndex_[streamIndex];

            // In most cases the foreign table stripes follow in sorted order, but still let's ensure that.
            auto cmpStripesByKey = [&] (int lhs, int rhs) {
                const auto& lhsLowerLimit = Stripes_[lhs].GetStripe()->DataSlices.front()->LowerLimit().Key;
                const auto& lhsUpperLimit = Stripes_[lhs].GetStripe()->DataSlices.back()->UpperLimit().Key;
                const auto& rhsLowerLimit = Stripes_[rhs].GetStripe()->DataSlices.front()->LowerLimit().Key;
                const auto& rhsUpperLimit = Stripes_[rhs].GetStripe()->DataSlices.back()->UpperLimit().Key;
                if (lhsLowerLimit != rhsLowerLimit) {
                    return lhsLowerLimit < rhsLowerLimit;
                } else if (lhsUpperLimit != rhsUpperLimit) {
                    return lhsUpperLimit < rhsUpperLimit;
                } else {
                    // If lower limits coincide and upper limits coincide too, these stripes
                    // must either be the same stripe or they both are maniac stripes with the same key.
                    // In both cases they may follow in any order.
                    return false;
                }
            };
            if (!std::is_sorted(stripeCookies.begin(), stripeCookies.end(), cmpStripesByKey)) {
                std::stable_sort(stripeCookies.begin(), stripeCookies.end(), cmpStripesByKey);
            }
            for (const auto& inputCookie : stripeCookies) {
                for (const auto& dataSlice : Stripes_[inputCookie].GetStripe()->DataSlices) {
                    builder->AddForeignDataSlice(dataSlice, inputCookie);
                }
            }
        }
    }

    void SetupSuspendedStripes()
    {
        for (int inputCookie = 0; inputCookie < Stripes_.size(); ++inputCookie) {
            const auto& stripe = Stripes_[inputCookie];
            if (stripe.IsSuspended()) {
                JobManager_->Suspend(inputCookie);
            }
        }
    }

    bool CanScheduleJob() const
    {
        return Finished && JobManager_->GetPendingJobCount() != 0;
    }

    TPeriodicYielder CreatePeriodicYielder()
    {
        if (SortedJobOptions_.EnablePeriodicYielder) {
            return TPeriodicYielder(PrepareYieldPeriod);
        } else {
            return TPeriodicYielder();
        }
    }

    TOutputOrderPtr GetOutputOrder() const
    {
        return nullptr;
    }

    void DoFinish()
    {
        // NB(max42): this method may be run several times (in particular, when
        // the resumed input is not consistent with the original input).

        InitInputChunkMapping();
        FindTeleportChunks();

        auto builder = New<TSortedJobBuilder>(
            SortedJobOptions_,
            JobSizeConstraints_,
            RowBuffer_,
            TeleportChunks_,
            false /* inSplit */,
            Logger);

        FetchNonTeleportPrimaryDataSlices(builder);
        PrepareForeignDataSlices(builder);
        auto jobStubs = builder->Build();
        JobManager_->AddJobs(std::move(jobStubs));

        TotalDataSliceCount_ += builder->GetTotalDataSliceCount();
    }

    void SplitJob(
        std::vector<TInputDataSlicePtr> unreadInputDataSlices,
        std::vector<TInputDataSlicePtr> foreignInputDataSlices,
        int splitJobCount)
    {
        i64 dataWeight = 0;
        for (const auto& dataSlice : unreadInputDataSlices) {
            dataWeight += dataSlice->GetDataWeight();
        }
        for (const auto& dataSlice : foreignInputDataSlices) {
            dataWeight += dataSlice->GetDataWeight();
        }
        i64 dataWeightPerJob = splitJobCount == 1
            ? std::numeric_limits<i64>::max()
            : DivCeil(dataWeight, static_cast<i64>(splitJobCount));

        // We create new job size constraints by incorporating the new desired data size per job
        // into the old job size constraints.
        auto jobSizeConstraints = CreateExplicitJobSizeConstraints(
            false /* canAdjustDataSizePerJob */,
            false /* isExplicitJobCount */,
            splitJobCount /* jobCount */,
            dataWeightPerJob,
            std::numeric_limits<i64>::max(),
            JobSizeConstraints_->GetMaxDataSlicesPerJob(),
            JobSizeConstraints_->GetMaxDataWeightPerJob(),
            JobSizeConstraints_->GetInputSliceDataWeight(),
            JobSizeConstraints_->GetInputSliceRowCount());

        // Teleport chunks do not affect the job split process since each original
        // job is already located between the teleport chunks.
        std::vector<TInputChunkPtr> teleportChunks;
        auto splitSortedJobOptions = SortedJobOptions_;
        // We do not want to yield during job splitting because it may potentially lead
        // to snapshot creation that will catch pool in inconsistent state.
        splitSortedJobOptions.EnablePeriodicYielder = false;
        auto builder = New<TSortedJobBuilder>(
            splitSortedJobOptions,
            std::move(jobSizeConstraints),
            RowBuffer_,
            teleportChunks,
            true /* inSplit */,
            Logger);
        for (const auto& dataSlice : unreadInputDataSlices) {
            int inputCookie = *dataSlice->Tag;
            YCHECK(InputStreamDirectory_.GetDescriptor(dataSlice->InputStreamIndex).IsPrimary());
            builder->AddPrimaryDataSlice(dataSlice, inputCookie);
        }
        for (const auto& dataSlice : foreignInputDataSlices) {
            int inputCookie = *dataSlice->Tag;
            YCHECK(InputStreamDirectory_.GetDescriptor(dataSlice->InputStreamIndex).IsForeign());
            builder->AddForeignDataSlice(dataSlice, inputCookie);
        }

        auto jobs = builder->Build();
        JobManager_->AddJobs(std::move(jobs));
    }

    void InvalidateCurrentJobs()
    {
        TeleportChunks_.clear();
        InputChunkMapping_.clear();
        for (auto& stripe : Stripes_) {
            stripe.ReplaceOriginalStripe();
            stripe.SetTeleport(false);
        }
        JobManager_->InvalidateAllJobs();
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TSortedChunkPool);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IChunkPool> CreateSortedChunkPool(
    const TSortedChunkPoolOptions& options,
    IChunkSliceFetcherFactoryPtr chunkSliceFetcherFactory,
    TInputStreamDirectory inputStreamDirectory)
{
    return std::make_unique<TSortedChunkPool>(options, std::move(chunkSliceFetcherFactory), std::move(inputStreamDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
