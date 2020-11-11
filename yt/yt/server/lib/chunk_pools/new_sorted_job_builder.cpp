#include "new_sorted_job_builder.h"

#include "helpers.h"

#include <yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/client/table_client/row_buffer.h>

#include <yt/library/random/bernoulli_sampler.h>

#include <yt/core/concurrency/periodic_yielder.h>

#include <yt/core/misc/collection_helpers.h>

namespace NYT::NChunkPools {

using namespace NTableClient;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

// XXX(max42): do not divide into left and right?
DEFINE_ENUM(EEndpointType,
    (PivotKey)
    (ForeignLeft)
    (Left)
    (Right)
    (ForeignRight)
);

class TNewSortedJobBuilder
    : public ISortedJobBuilder
{
public:
    TNewSortedJobBuilder(
        const TSortedJobOptions& options,
        IJobSizeConstraintsPtr jobSizeConstraints,
        const TRowBufferPtr& rowBuffer,
        const std::vector<TInputChunkPtr>& teleportChunks,
        bool inSplit,
        int retryIndex,
        const TLogger& logger)
        : Options_(options)
        , Comparator_(std::vector<ESortOrder>(options.PrimaryPrefixLength, ESortOrder::Ascending))
        , JobSizeConstraints_(std::move(jobSizeConstraints))
        , JobSampler_(JobSizeConstraints_->GetSamplingRate())
        , RowBuffer_(rowBuffer)
        , InSplit_(inSplit)
        , RetryIndex_(retryIndex)
        , Logger(logger)
    {
        for (const auto& inputChunk : teleportChunks) {
            auto minKeyRow = RowBuffer_->Capture(inputChunk->BoundaryKeys()->MinKey.Begin(), Comparator_.GetLength());
            auto maxKeyRow = RowBuffer_->Capture(inputChunk->BoundaryKeys()->MaxKey.Begin(), Comparator_.GetLength());
            TeleportChunks_.emplace_back(TTeleportChunk{
                .InputChunk = inputChunk,
                .MinKey = TKey::FromRow(minKeyRow),
                .ExclusiveLowerKeyBound = TKeyBound::FromRow(minKeyRow, /* isInclusive */ false, /* isUpper */ false),
                .MaxKey = TKey::FromRow(maxKeyRow),
                .ExclusiveUpperKeyBound = TKeyBound::FromRow(maxKeyRow, /* isInclusive */ false, /* isUpper */ true),
            });
        }

        for (size_t index = 0; index + 1 < TeleportChunks_.size(); ++index) {
            YT_VERIFY(Comparator_.CompareKeys(TeleportChunks_[index].MaxKey, TeleportChunks_[index + 1].MinKey) <= 0);
        }
    }

    virtual void AddForeignDataSlice(const TInputDataSlicePtr& dataSlice, IChunkPoolInput::TCookie cookie) override
    {
        YT_VERIFY(!dataSlice->IsLegacy);
        DataSliceToInputCookie_[dataSlice] = cookie;

        if (dataSlice->InputStreamIndex >= InputStreamIndexToForeignDataSlices_.size()) {
            InputStreamIndexToForeignDataSlices_.resize(dataSlice->InputStreamIndex + 1);
        }
        InputStreamIndexToForeignDataSlices_[dataSlice->InputStreamIndex].emplace_back(dataSlice);

        // NB: We do not need to shorten keys here. Endpoints of type "Foreign" only make
        // us to stop, to add all foreign slices up to the current moment and to check
        // if we already have to end the job due to the large data size or slice count.
        TEndpoint leftEndpoint = {
            EEndpointType::ForeignLeft,
            dataSlice,
            dataSlice->LowerLimit().KeyBound,
            dataSlice->LowerLimit().RowIndex.value_or(0)
        };
        TEndpoint rightEndpoint = {
            EEndpointType::ForeignRight,
            dataSlice,
            dataSlice->UpperLimit().KeyBound,
            dataSlice->UpperLimit().RowIndex.value_or(0)
        };

        Endpoints_.emplace_back(leftEndpoint);
        Endpoints_.emplace_back(rightEndpoint);
    }

    virtual void AddPrimaryDataSlice(const TInputDataSlicePtr& dataSlice, IChunkPoolInput::TCookie cookie) override
    {
        YT_VERIFY(!dataSlice->IsLegacy);

        TEndpoint leftEndpoint;
        TEndpoint rightEndpoint;

        if (Options_.EnableKeyGuarantee) {
            leftEndpoint = {
                EEndpointType::Left,
                dataSlice,
                dataSlice->LowerLimit().KeyBound,
                0LL /* RowIndex */
            };

            rightEndpoint = {
                EEndpointType::Right,
                dataSlice,
                dataSlice->UpperLimit().KeyBound,
                0LL /* RowIndex */
            };
        } else {
            int leftRowIndex = dataSlice->LowerLimit().RowIndex.value_or(0);
            leftEndpoint = {
                EEndpointType::Left,
                dataSlice,
                dataSlice->LowerLimit().KeyBound,
                leftRowIndex
            };

            int rightRowIndex = dataSlice->UpperLimit().RowIndex.value_or(
                dataSlice->Type == EDataSourceType::UnversionedTable
                ? dataSlice->GetSingleUnversionedChunkOrThrow()->GetRowCount()
                : 0);

            rightEndpoint = {
                EEndpointType::Right,
                dataSlice,
                dataSlice->UpperLimit().KeyBound,
                rightRowIndex
            };
        }

        if (Comparator_.IsRangeEmpty(leftEndpoint.KeyBound, rightEndpoint.KeyBound)) {
            // This can happen if ranges were specified.
            // Chunk slice fetcher can produce empty slices.
            return;
        }

        DataSliceToInputCookie_[dataSlice] = cookie;

        Endpoints_.push_back(leftEndpoint);
        Endpoints_.push_back(rightEndpoint);
    }

    virtual std::vector<std::unique_ptr<TJobStub>> Build() override
    {
        AddPivotKeysEndpoints();
        SortEndpoints();
        if (Options_.LogDetails) {
            LogDetails();
        }
        BuildJobs();
        AttachForeignSlices();
        for (auto& job : Jobs_) {
            job->Finalize(true /* sortByPosition */);
            ValidateJob(job.get());
        }
        return std::move(Jobs_);
    }

    void ValidateJob(const TJobStub* job)
    {
        if (job->GetDataWeight() > JobSizeConstraints_->GetMaxDataWeightPerJob()) {
            YT_LOG_DEBUG("Maximum allowed data weight per sorted job exceeds the limit (DataWeight: %v, MaxDataWeightPerJob: %v, "
                "LowerKey: %v, UpperKey: %v, JobDebugString: %v)",
                job->GetDataWeight(),
                JobSizeConstraints_->GetMaxDataWeightPerJob(),
                job->LowerPrimaryKey(),
                job->UpperPrimaryKey(),
                job->GetDebugString());

            THROW_ERROR_EXCEPTION(
                EErrorCode::MaxDataWeightPerJobExceeded, "Maximum allowed data weight per sorted job exceeds the limit: %v > %v",
                job->GetDataWeight(),
                JobSizeConstraints_->GetMaxDataWeightPerJob())
                << TErrorAttribute("lower_key", job->LowerPrimaryKey())
                << TErrorAttribute("upper_key", job->UpperPrimaryKey());
        }

        if (job->GetPrimaryDataWeight() > JobSizeConstraints_->GetMaxPrimaryDataWeightPerJob()) {
            YT_LOG_DEBUG("Maximum allowed primary data weight per sorted job exceeds the limit (PrimaryDataWeight: %v, MaxPrimaryDataWeightPerJob: %v, "
                "LowerKey: %v, UpperKey: %v, JobDebugString: %v)",
                job->GetPrimaryDataWeight(),
                JobSizeConstraints_->GetMaxPrimaryDataWeightPerJob(),
                job->LowerPrimaryKey(),
                job->UpperPrimaryKey(),
                job->GetDebugString());

            THROW_ERROR_EXCEPTION(
                EErrorCode::MaxPrimaryDataWeightPerJobExceeded, "Maximum allowed primary data weight per sorted job exceeds the limit: %v > %v",
                job->GetPrimaryDataWeight(),
                JobSizeConstraints_->GetMaxPrimaryDataWeightPerJob())
                << TErrorAttribute("lower_key", job->LowerPrimaryKey())
                << TErrorAttribute("upper_key", job->UpperPrimaryKey());
        }
    }

    virtual i64 GetTotalDataSliceCount() const override
    {
        return TotalSliceCount_;
    }

private:
    TSortedJobOptions Options_;

    TComparator Comparator_;

    IJobSizeConstraintsPtr JobSizeConstraints_;
    TBernoulliSampler JobSampler_;

    TRowBufferPtr RowBuffer_;

    struct TEndpoint
    {
        EEndpointType Type;
        TInputDataSlicePtr DataSlice;
        TKeyBound KeyBound;
        i64 RowIndex;

        i64 GetGlobalRowIndex() const
        {
            return RowIndex + DataSlice->GetSingleUnversionedChunkOrThrow()->GetTableRowIndex();
        }
    };

    //! Endpoints of primary table slices in SortedReduce and SortedMerge.
    std::vector<TEndpoint> Endpoints_;

    //! Vector keeping the pool-side state of all jobs that depend on the data from this pool.
    //! These items are merely stubs of a future jobs that are filled during the BuildJobsBy{Key/TableIndices}()
    //! call, and when current job is finished it is passed to the `JobManager_` that becomes responsible
    //! for its future.
    std::vector<std::unique_ptr<TJobStub>> Jobs_;

    //! Stores correspondence between data slices and their input cookies.
    THashMap<TInputDataSlicePtr, IChunkPoolInput::TCookie> DataSliceToInputCookie_;

    std::vector<std::vector<TInputDataSlicePtr>> InputStreamIndexToForeignDataSlices_;

    //! Stores the number of slices in all jobs up to current moment.
    i64 TotalSliceCount_ = 0;

    struct TTeleportChunk
    {
        TInputChunkPtr InputChunk;
        //! Minimum key in chunk.
        TKey MinKey;
        //! Key bound "> MinKey".
        TKeyBound ExclusiveLowerKeyBound;
        //! Maximum key in chunk.
        TKey MaxKey;
        //! Key bound "< MaxKey".
        TKeyBound ExclusiveUpperKeyBound;
    };

    std::vector<TTeleportChunk> TeleportChunks_;

    //! Indicates if this sorted job builder is used during job splitting.
    bool InSplit_ = false;

    int RetryIndex_;

    const TLogger& Logger;

    void AddPivotKeysEndpoints()
    {
        for (const auto& pivotKey : Options_.PivotKeys) {
            // Pivot keys act as key bounds of type >=.
            TEndpoint endpoint = {
                EEndpointType::PivotKey,
                nullptr,
                TKeyBound::FromRow(pivotKey, /* isInclusive */ true, /* isUpper */ false),
                0
            };
            Endpoints_.emplace_back(endpoint);
        }
    }

    void SortEndpoints()
    {
        YT_LOG_DEBUG("Sorting endpoints (Count: %v)", Endpoints_.size());
        std::sort(
            Endpoints_.begin(),
            Endpoints_.end(),
            [=] (const TEndpoint& lhs, const TEndpoint& rhs) -> bool {
                {
                    // XXX(max42): upperFirst?
                    auto cmpResult = Comparator_.CompareKeyBounds(lhs.KeyBound, rhs.KeyBound);
                    if (cmpResult != 0) {
                        return cmpResult < 0;
                    }
                }

                if (lhs.DataSlice && lhs.DataSlice->Type == EDataSourceType::UnversionedTable &&
                    rhs.DataSlice && rhs.DataSlice->Type == EDataSourceType::UnversionedTable)
                {
                    // If keys are equal, we put slices in the same order they are in the original input stream.
                    auto cmpResult = lhs.GetGlobalRowIndex() - rhs.GetGlobalRowIndex();
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

    void LogDetails()
    {
        for (int index = 0; index < Endpoints_.size(); ++index) {
            const auto& endpoint = Endpoints_[index];
            YT_LOG_DEBUG("Endpoint (Index: %v, KeyBound: %v, RowIndex: %v, GlobalRowIndex: %v, Type: %v, DataSlice: %v)",
                index,
                endpoint.KeyBound,
                endpoint.RowIndex,
                (endpoint.DataSlice && endpoint.DataSlice->Type == EDataSourceType::UnversionedTable)
                ? std::make_optional(endpoint.DataSlice->GetSingleUnversionedChunkOrThrow()->GetTableRowIndex() + endpoint.RowIndex)
                : std::nullopt,
                endpoint.Type,
                endpoint.DataSlice.Get());
        }
        for (const auto& dataSlice : GetKeys(DataSliceToInputCookie_)) {
            std::vector<TChunkId> chunkIds;
            chunkIds.reserve(dataSlice->ChunkSlices.size());
            for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                chunkIds.emplace_back(chunkSlice->GetInputChunk()->ChunkId());
            }
            YT_LOG_DEBUG("Data slice (Address: %v, DataWeight: %v, InputStreamIndex: %v, ChunkIds: %v)",
                dataSlice.Get(),
                dataSlice->GetDataWeight(),
                dataSlice->InputStreamIndex,
                chunkIds);
        }
        for (const auto& teleportChunk : TeleportChunks_) {
            YT_LOG_DEBUG("Teleport chunk (Address: %v, MinKey: %v, MaxKey: %v)",
                teleportChunk.InputChunk.Get(),
                teleportChunk.MinKey,
                teleportChunk.MaxKey);
        }
    }

    i64 GetDataWeightPerJob() const
    {
        return
            JobSizeConstraints_->GetSamplingRate()
            ? JobSizeConstraints_->GetSamplingDataWeightPerJob()
            : JobSizeConstraints_->GetDataWeightPerJob();
    }

    i64 GetPrimaryDataWeightPerJob() const
    {
        return
            JobSizeConstraints_->GetSamplingRate()
            ? JobSizeConstraints_->GetSamplingPrimaryDataWeightPerJob()
            : JobSizeConstraints_->GetPrimaryDataWeightPerJob();
    }

    void AddDataSlice(
        TJobStub& jobStub,
        const TInputDataSlicePtr& dataSlice,
        IChunkPoolInput::TCookie inputCookie,
        bool isPrimary)
    {
        dataSlice->TransformToLegacy(RowBuffer_);

        jobStub.AddDataSlice(dataSlice, inputCookie, isPrimary);
    }

    // TODO(max42): split this method into several smaller ones.
    void BuildJobs()
    {
        if (auto samplingRate = JobSizeConstraints_->GetSamplingRate()) {
            YT_LOG_DEBUG(
                "Building jobs with sampling "
                "(SamplingRate: %v, SamplingDataWeightPerJob: %v, SamplingPrimaryDataWeightPerJob: %v)",
                *JobSizeConstraints_->GetSamplingRate(),
                JobSizeConstraints_->GetSamplingDataWeightPerJob(),
                JobSizeConstraints_->GetSamplingPrimaryDataWeightPerJob());
        }

        Jobs_.emplace_back(std::make_unique<TJobStub>());

        // Key bound defining the part of the input data slice that is not consumed yet.
        THashMap<TInputDataSlicePtr, TKeyBound> openedPrimarySliceToLowerBound;

        auto yielder = CreatePeriodicYielder();

        // An index of a closest teleport chunk to the right of current endpoint.
        int nextTeleportChunkIndex = 0;

        int jobIndex = 0;

        i64 totalDataWeight = 0;

        auto endJob = [&] (TKeyBound upperBound) {
            for (auto it = openedPrimarySliceToLowerBound.begin(); it != openedPrimarySliceToLowerBound.end(); ) {
                // Save the it to the next element because we may possibly erase current it.
                auto nextIterator = std::next(it);

                const auto& dataSlice = it->first;

                // Restrict data slice using lower bound from it and upper bound from endJob argument.
                auto& lowerBound = it->second;
                auto exactDataSlice = CreateInputDataSlice(dataSlice, Comparator_, lowerBound, upperBound);
                exactDataSlice->CopyPayloadFrom(*dataSlice);

                // TODO(max42): this seems useless now as CopyPayloadFrom already does that.
                auto inputCookie = DataSliceToInputCookie_.at(dataSlice);
                exactDataSlice->Tag = inputCookie;

                AddDataSlice(
                    *Jobs_.back(),
                    exactDataSlice,
                    inputCookie,
                    true /* isPrimary */);

                // Recall that lowerBound is a reference to it->second.
                // Adjust it to represent the remaining part of a data slice.
                lowerBound = upperBound.Invert();
                if (Comparator_.IsRangeEmpty(lowerBound, dataSlice->UpperLimit().KeyBound)) {
                    openedPrimarySliceToLowerBound.erase(it);
                }

                it = nextIterator;
            }

            // If current job does not contain primary data slices then we can re-use it,
            // otherwise we should create a new job.
            if (Jobs_.back()->GetSliceCount() > 0) {
                if (JobSampler_.Sample()) {
                    YT_LOG_DEBUG("Sorted job created (JobIndex: %v, BuiltJobCount: %v, PrimaryDataSize: %v, PrimaryRowCount: %v, "
                        "PrimarySliceCount: %v, PreliminaryForeignDataSize: %v, PreliminaryForeignRowCount: %v, "
                        "PreliminaryForeignSliceCount: %v, LowerPrimaryKey: %v, UpperPrimaryKey: %v)",
                        jobIndex,
                        static_cast<int>(Jobs_.size()) - 1,
                        Jobs_.back()->GetPrimaryDataWeight(),
                        Jobs_.back()->GetPrimaryRowCount(),
                        Jobs_.back()->GetPrimarySliceCount(),
                        Jobs_.back()->GetPreliminaryForeignDataWeight(),
                        Jobs_.back()->GetPreliminaryForeignRowCount(),
                        Jobs_.back()->GetPreliminaryForeignSliceCount(),
                        Jobs_.back()->LowerPrimaryKey(),
                        Jobs_.back()->UpperPrimaryKey());

                    totalDataWeight += Jobs_.back()->GetDataWeight();
                    TotalSliceCount_ += Jobs_.back()->GetSliceCount();
                    ValidateTotalSliceCountLimit();
                    Jobs_.emplace_back(std::make_unique<TJobStub>());

                    if (Options_.LogDetails) {
                        YT_LOG_DEBUG("Sorted job details (JobIndex: %v, BuiltJobCount: %v, Details: %v)",
                            jobIndex,
                            static_cast<int>(Jobs_.size()) - 1,
                            Jobs_.back()->GetDebugString());
                    }
                } else {
                    YT_LOG_DEBUG("Sorted job skipped (JobIndex: %v, BuiltJobCount: %v, PrimaryDataSize: %v, "
                        "PreliminaryForeignDataSize: %v, LowerPrimaryKey: %v, UpperPrimaryKey: %v)",
                        jobIndex,
                        static_cast<int>(Jobs_.size()) - 1,
                        Jobs_.back()->GetPrimaryDataWeight(),
                        Jobs_.back()->GetPreliminaryForeignDataWeight(),
                        Jobs_.back()->LowerPrimaryKey(),
                        Jobs_.back()->UpperPrimaryKey());
                    Jobs_.back() = std::make_unique<TJobStub>();
                }
                ++jobIndex;
            }
        };

        auto addBarrier = [&] {
            Jobs_.back()->SetIsBarrier(true);
            Jobs_.emplace_back(std::make_unique<TJobStub>());
        };

        for (int index = 0, nextEndpointIndex = 0; index < Endpoints_.size(); ++index) {
            yielder.TryYield();

            // Find first endpoint located (strictly) to the right from us.
            // In particular, all key bounds in range [index, nextEndpointIndex)
            // will be either of types {<=, >} or {<, >=}.
            auto keyBound = Endpoints_[index].KeyBound;
            while (nextEndpointIndex != Endpoints_.size() && Comparator_.CompareKeyBounds(Endpoints_[nextEndpointIndex].KeyBound, keyBound) == 0) {
                ++nextEndpointIndex;
            }

            auto nextKeyBound = (nextEndpointIndex == Endpoints_.size()) ? TKeyBound() : Endpoints_[nextEndpointIndex].KeyBound;
            // XXX(max42): coincides with nextKeyBound.IsUpper?
            bool nextKeyIsLeft = (nextEndpointIndex == Endpoints_.size()) ? false : Endpoints_[nextEndpointIndex].Type == EEndpointType::Left;

            // Find closest teleport chunk to the right from us in order to do that, take current key bound,
            // transform it to type > or >= and use it as a predicate for checking if teleport chunk is to the right.
            auto keyBoundUpperCounterpart = keyBound.UpperCounterpart();
            // NB: when key guarantee is disabled, it may happen that chunk range intersects keyBoundUpperCounterpart
            // by exactly one key. Thus we use ExclusiveLowerKeyBound instead of MinKey.
            while (
                nextTeleportChunkIndex < TeleportChunks_.size() &&
                !Comparator_.IsRangeEmpty(TeleportChunks_[nextTeleportChunkIndex].ExclusiveLowerKeyBound, keyBoundUpperCounterpart))
            {
                ++nextTeleportChunkIndex;
            }

            // Process data current endpoint.

            if (Endpoints_[index].Type == EEndpointType::Left) {
                // Put newly opened data slice.
                openedPrimarySliceToLowerBound[Endpoints_[index].DataSlice] = TKeyBound::MakeUniversal(/* isUpper */ false);
            } else if (Endpoints_[index].Type == EEndpointType::Right) {
                // Remove data slice that closes at this point.
                const auto& dataSlice = Endpoints_[index].DataSlice;
                auto it = openedPrimarySliceToLowerBound.find(dataSlice);
                // It might have happened that we already removed this slice from the
                // `openedSlicesLowerLimits` during one of the previous `endJob` calls.
                if (it != openedPrimarySliceToLowerBound.end()) {
                    auto exactDataSlice = CreateInputDataSlice(dataSlice, Comparator_, it->second);
                    exactDataSlice->CopyPayloadFrom(*dataSlice);
                    // TODO(max42): this seems useless now as CopyPayloadFrom already does that.
                    auto inputCookie = DataSliceToInputCookie_.at(dataSlice);
                    exactDataSlice->Tag = inputCookie;
                    AddDataSlice(*Jobs_.back(), exactDataSlice, inputCookie, true /* isPrimary */);
                    openedPrimarySliceToLowerBound.erase(it);
                }
            } else if (Endpoints_[index].Type == EEndpointType::ForeignRight) {
                Jobs_.back()->AddPreliminaryForeignDataSlice(Endpoints_[index].DataSlice);
            }

            // Is set to true if we decide to end a job here. The decision logic may be
            // different depending on if we have user-provided pivot keys or not.
            bool endHere = false;
            bool addBarrierHere = false;

            if (Options_.PivotKeys.empty()) {
                double retryFactor = std::pow(JobSizeConstraints_->GetDataWeightPerJobRetryFactor(), RetryIndex_);
                bool jobIsLargeEnough =
                    Jobs_.back()->GetPreliminarySliceCount() + openedPrimarySliceToLowerBound.size() > JobSizeConstraints_->GetMaxDataSlicesPerJob() ||
                    Jobs_.back()->GetPreliminaryDataWeight() >= GetDataWeightPerJob() * retryFactor ||
                    Jobs_.back()->GetPrimaryDataWeight() >= GetPrimaryDataWeightPerJob() * retryFactor;

                // If next teleport chunk is closer than next data slice then we are obligated to close the job here.
                bool beforeTeleportChunk;
                {
                    auto nextKeyBoundLowerCounterpart = nextKeyBound.LowerCounterpart();
                    beforeTeleportChunk = nextEndpointIndex == index + 1 &&
                        nextKeyIsLeft &&
                        (nextTeleportChunkIndex != TeleportChunks_.size() &&
                         Comparator_.IsRangeEmpty(nextKeyBoundLowerCounterpart, TeleportChunks_[nextTeleportChunkIndex].ExclusiveUpperKeyBound));
                }

                // If key guarantee is enabled, we cannot end here if next data slice covers the same reduce key.
                bool canEndHere = !Options_.EnableKeyGuarantee || index + 1 == nextEndpointIndex;

                // Sanity check. Contrary would mean that teleport chunk was chosen incorrectly,
                // because teleport chunks should not normally intersect the other data slices.
                YT_VERIFY(!(beforeTeleportChunk && !canEndHere));

                endHere = canEndHere && (beforeTeleportChunk || jobIsLargeEnough);
                addBarrierHere = beforeTeleportChunk;
            } else {
                // We may end jobs only at the pivot keys.
                endHere = Endpoints_[index].Type == EEndpointType::PivotKey;
                addBarrierHere = true;
            }

            if (endHere) {
                endJob(keyBound.UpperCounterpart());
                if (addBarrierHere) {
                    addBarrier();
                }
            }
        }
        endJob(TKeyBound::MakeUniversal(/* isUpper */ true));

        JobSizeConstraints_->UpdateInputDataWeight(totalDataWeight);

        if (!Jobs_.empty() && Jobs_.back()->GetSliceCount() == 0) {
            Jobs_.pop_back();
        }
        YT_LOG_DEBUG("Jobs created (Count: %v)", Jobs_.size());
        if (InSplit_ && Jobs_.size() == 1 && JobSizeConstraints_->GetJobCount() > 1) {
            YT_LOG_DEBUG("Pool was not able to split job properly (SplitJobCount: %v, JobCount: %v)",
                JobSizeConstraints_->GetJobCount(),
                Jobs_.size());

            Jobs_.front()->SetUnsplittable();
        }
    }

    void AttachForeignSlices()
    {
        auto yielder = CreatePeriodicYielder();

        // An optimization of memory consumption. Precalculate
        // shortened lower and upper bounds for jobs instead of allocating
        // them on each pair of foreign data slice and matching job.
        std::vector<TKeyBound> jobUpperBounds;
        std::vector<TKeyBound> jobLowerBounds;

        jobUpperBounds.reserve(Jobs_.size());
        jobLowerBounds.reserve(Jobs_.size());

        for (const auto& job : Jobs_) {
            // COMPAT(max42): job manager currently deals with legacy keys, so this code is a bit more ugly than expected.
            auto jobLowerKeyBound = KeyBoundFromLegacyRow(
                job->LowerPrimaryKey(),
                /* isUpper */ false,
                Comparator_.GetLength(),
                RowBuffer_);
            auto jobUpperKeyBound = KeyBoundFromLegacyRow(
                job->UpperPrimaryKey(),
                /* isUpper */ true,
                Comparator_.GetLength(),
                RowBuffer_);
            jobLowerBounds.emplace_back(ShortenKeyBound(jobLowerKeyBound, Options_.ForeignPrefixLength, RowBuffer_));
            jobUpperBounds.emplace_back(ShortenKeyBound(jobUpperKeyBound, Options_.ForeignPrefixLength, RowBuffer_));
        }

        // Traverse sequence of foreign data slices from each foreign input stream.
        for (auto& foreignDataSlices : InputStreamIndexToForeignDataSlices_) {
            yielder.TryYield();

            // Index of the first job that matches current foreign data slice.
            int startJobIndex = 0;

            for (const auto& foreignDataSlice : foreignDataSlices) {
                // Skip jobs that are entirely to the left of foreign data slice.
                while (startJobIndex < Jobs_.size()) {
                    const auto& job = Jobs_[startJobIndex];
                    if (job->GetIsBarrier()) {
                        // Job is a barrier, ignore it.
                        ++startJobIndex;
                    } else if (Comparator_.IsRangeEmpty(foreignDataSlice->LowerLimit().KeyBound, jobUpperBounds[startJobIndex])) {
                        // Job's rightmost key is to the left of the slice's leftmost key.
                        ++startJobIndex;
                    } else {
                        break;
                    }
                }

                // There are no more jobs that may possibly overlap with our foreign data slice.
                if (startJobIndex == Jobs_.size()) {
                    break;
                }

                int jobIndex = startJobIndex;
                while (jobIndex < Jobs_.size()) {
                    yielder.TryYield();

                    const auto& job = Jobs_[jobIndex];
                    if (job->GetIsBarrier()) {
                        // Job is a barrier, ignore it.
                        ++jobIndex;
                        continue;
                    }

                    // Keep in mind: we already sure that for all jobs starting from #startJobIndex their
                    // upper limits are to the right of current data slice lower limit.
                    if (!Comparator_.IsRangeEmpty(jobLowerBounds[jobIndex], foreignDataSlice->UpperLimit().KeyBound)) {
                        // Job key range intersects with foreign slice key range, add foreign data slice into the job.
                        auto exactForeignDataSlice = CreateInputDataSlice(
                            foreignDataSlice,
                            Comparator_,
                            jobLowerBounds[jobIndex],
                            jobUpperBounds[jobIndex]);
                        exactForeignDataSlice->CopyPayloadFrom(*foreignDataSlice);

                        // TODO(max42): this seems useless now as CopyPayloadFrom already does that.
                        auto inputCookie = DataSliceToInputCookie_.at(foreignDataSlice);
                        exactForeignDataSlice->Tag = inputCookie;

                        ++TotalSliceCount_;
                        AddDataSlice(
                            *job,
                            exactForeignDataSlice,
                            inputCookie,
                            /* isPrimary */ false);
                        ++jobIndex;
                    } else {
                        // This job and all the subsequent jobs are entirely to the right of foreign data slice.
                        break;
                    }
                }
            }
            ValidateTotalSliceCountLimit();
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

    void ValidateTotalSliceCountLimit() const
    {
        if (TotalSliceCount_ > Options_.MaxTotalSliceCount) {
            THROW_ERROR_EXCEPTION(EErrorCode::DataSliceLimitExceeded, "Total number of data slices in sorted pool is too large.")
                << TErrorAttribute("actual_total_slice_count", TotalSliceCount_)
                << TErrorAttribute("max_total_slice_count", Options_.MaxTotalSliceCount)
                << TErrorAttribute("current_job_count", Jobs_.size());
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TNewSortedJobBuilder);

////////////////////////////////////////////////////////////////////////////////

ISortedJobBuilderPtr CreateNewSortedJobBuilder(
    const TSortedJobOptions& options,
    IJobSizeConstraintsPtr jobSizeConstraints,
    const TRowBufferPtr& rowBuffer,
    const std::vector<TInputChunkPtr>& teleportChunks,
    bool inSplit,
    int retryIndex,
    const TLogger& logger)
{
    return New<TNewSortedJobBuilder>(options, std::move(jobSizeConstraints), rowBuffer, teleportChunks, inSplit, retryIndex, logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
