#include "legacy_sorted_job_builder.h"

#include "helpers.h"

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/library/random/bernoulli_sampler.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NChunkPools {

using namespace NTableClient;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EEndpointType,
    (PivotKey)
    (ForeignLeft)
    (Left)
    (Right)
    (ForeignRight)
);

class TLegacySortedJobBuilder
    : public ILegacySortedJobBuilder
{
public:
    TLegacySortedJobBuilder(
        const TSortedJobOptions& options,
        IJobSizeConstraintsPtr jobSizeConstraints,
        const TRowBufferPtr& rowBuffer,
        const std::vector<TInputChunkPtr>& teleportChunks,
        int retryIndex,
        const TLogger& logger)
        : Options_(options)
        , JobSizeConstraints_(std::move(jobSizeConstraints))
        , JobSampler_(JobSizeConstraints_->GetSamplingRate())
        , RowBuffer_(rowBuffer)
        , TeleportChunks_(teleportChunks)
        , RetryIndex_(retryIndex)
        , Logger(logger)
    { }

    void AddForeignDataSlice(const TLegacyDataSlicePtr& dataSlice, IChunkPoolInput::TCookie cookie) override
    {
        YT_VERIFY(dataSlice->IsLegacy);

        DataSliceToInputCookie_[dataSlice] = cookie;

        if (dataSlice->GetInputStreamIndex() >= std::ssize(ForeignDataSlices_)) {
            ForeignDataSlices_.resize(dataSlice->GetInputStreamIndex() + 1);
        }
        ForeignDataSlices_[dataSlice->GetInputStreamIndex()].emplace_back(dataSlice);

        // NB: We do not need to shorten keys here. Endpoints of type "Foreign" only make
        // us to stop, to add all foreign slices up to the current moment and to check
        // if we already have to end the job due to the large data size or slice count.
        TEndpoint leftEndpoint = {
            EEndpointType::ForeignLeft,
            dataSlice,
            GetStrictKey(dataSlice->LegacyLowerLimit().Key, Options_.PrimaryPrefixLength, RowBuffer_),
            dataSlice->LegacyLowerLimit().RowIndex.value_or(0)
        };
        TEndpoint rightEndpoint = {
            EEndpointType::ForeignRight,
            dataSlice,
            GetStrictKeySuccessor(dataSlice->LegacyUpperLimit().Key, Options_.PrimaryPrefixLength + 1, RowBuffer_),
            dataSlice->LegacyUpperLimit().RowIndex.value_or(0)
        };

        try {
            ValidateClientKey(leftEndpoint.Key);
            ValidateClientKey(rightEndpoint.Key);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                "Error validating sample key in input stream %v",
                dataSlice->GetInputStreamIndex())
                    << ex;
        }

        Endpoints_.emplace_back(leftEndpoint);
        Endpoints_.emplace_back(rightEndpoint);
    }

    void AddPrimaryDataSlice(const TLegacyDataSlicePtr& dataSlice, IChunkPoolInput::TCookie cookie) override
    {
        YT_VERIFY(dataSlice->IsLegacy);

        if (dataSlice->LegacyLowerLimit().Key >= dataSlice->LegacyUpperLimit().Key) {
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
                GetKeyPrefix(dataSlice->LegacyLowerLimit().Key, Options_.PrimaryPrefixLength, RowBuffer_),
                0LL /*RowIndex*/
            };

            rightEndpoint = {
                EEndpointType::Right,
                dataSlice,
                GetKeySuccessor(GetKeyPrefix(dataSlice->LegacyUpperLimit().Key, Options_.PrimaryPrefixLength, RowBuffer_), RowBuffer_),
                0LL /*RowIndex*/
            };
        } else {
            int leftRowIndex = dataSlice->LegacyLowerLimit().RowIndex.value_or(0);
            leftEndpoint = {
                EEndpointType::Left,
                dataSlice,
                GetStrictKey(dataSlice->LegacyLowerLimit().Key, Options_.PrimaryPrefixLength, RowBuffer_, EValueType::Min),
                leftRowIndex
            };

            int rightRowIndex = dataSlice->LegacyUpperLimit().RowIndex.value_or(
                dataSlice->Type == EDataSourceType::UnversionedTable
                ? dataSlice->GetSingleUnversionedChunk()->GetRowCount()
                : 0);

            rightEndpoint = {
                EEndpointType::Right,
                dataSlice,
                GetStrictKey(dataSlice->LegacyUpperLimit().Key, Options_.PrimaryPrefixLength, RowBuffer_, EValueType::Min),
                rightRowIndex
            };
        }

        try {
            ValidateClientKey(leftEndpoint.Key);
            ValidateClientKey(rightEndpoint.Key);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                "Error validating sample key in input stream %v",
                dataSlice->GetInputStreamIndex())
                    << ex;
        }

        Endpoints_.push_back(leftEndpoint);
        Endpoints_.push_back(rightEndpoint);
    }

    std::vector<std::unique_ptr<TLegacyJobStub>> Build() override
    {
        AddPivotKeysEndpoints();
        SortEndpoints();
        LogDetails();
        BuildJobs();
        AttachForeignSlices();
        for (auto& job : Jobs_) {
            job->Finalize(true /*sortByPosition*/);

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
        return std::move(Jobs_);
    }

    i64 GetTotalDataSliceCount() const override
    {
        return TotalSliceCount_;
    }

private:
    TSortedJobOptions Options_;

    IJobSizeConstraintsPtr JobSizeConstraints_;
    TBernoulliSampler JobSampler_;

    TRowBufferPtr RowBuffer_;

    struct TEndpoint
    {
        EEndpointType Type;
        TLegacyDataSlicePtr DataSlice;
        TLegacyKey Key;
        i64 RowIndex;
    };

    //! Endpoints of primary table slices in SortedReduce and SortedMerge.
    std::vector<TEndpoint> Endpoints_;

    //! Vector keeping the pool-side state of all jobs that depend on the data from this pool.
    //! These items are merely stubs of a future jobs that are filled during the BuildJobsBy{Key/TableIndices}()
    //! call, and when current job is finished it is passed to the `JobManager_` that becomes responsible
    //! for its future.
    std::vector<std::unique_ptr<TLegacyJobStub>> Jobs_;

    //! Stores correspondence between primary data slices added via `AddPrimaryDataSlice`
    //! (both unversioned and versioned) and their input cookies.
    THashMap<TLegacyDataSlicePtr, IChunkPoolInput::TCookie> DataSliceToInputCookie_;

    std::vector<std::vector<TLegacyDataSlicePtr>> ForeignDataSlices_;

    //! Stores the number of slices in all jobs up to current moment.
    i64 TotalSliceCount_ = 0;

    const std::vector<TInputChunkPtr>& TeleportChunks_;

    int RetryIndex_;

    const TLogger& Logger;

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
        YT_LOG_DEBUG("Sorting endpoints (Count: %v)", Endpoints_.size());
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
                    const auto& lhsChunk = lhs.DataSlice->GetSingleUnversionedChunk();
                    const auto& rhsChunk = rhs.DataSlice->GetSingleUnversionedChunk();

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

    void LogDetails()
    {
        if (!Logger.IsLevelEnabled(ELogLevel::Trace)) {
            return;
        }

        for (int index = 0; index < std::ssize(Endpoints_); ++index) {
            const auto& endpoint = Endpoints_[index];
            YT_LOG_TRACE("Endpoint (Index: %v, Key: %v, RowIndex: %v, GlobalRowIndex: %v, Type: %v, DataSlice: %v)",
                index,
                endpoint.Key,
                endpoint.RowIndex,
                (endpoint.DataSlice && endpoint.DataSlice->Type == EDataSourceType::UnversionedTable)
                ? std::make_optional(endpoint.DataSlice->GetSingleUnversionedChunk()->GetTableRowIndex() + endpoint.RowIndex)
                : std::nullopt,
                endpoint.Type,
                endpoint.DataSlice.Get());
        }
        for (const auto& dataSlice : GetKeys(DataSliceToInputCookie_)) {
            std::vector<TChunkId> chunkIds;
            for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                chunkIds.push_back(chunkSlice->GetInputChunk()->GetChunkId());
            }
            YT_LOG_TRACE("Data slice (Address: %v, DataWeight: %v, InputStreamIndex: %v, ChunkIds: %v)",
                dataSlice.Get(),
                dataSlice->GetDataWeight(),
                dataSlice->GetInputStreamIndex(),
                chunkIds);
        }
        for (const auto& teleportChunk : TeleportChunks_) {
            YT_LOG_TRACE("Teleport chunk (Address: %v, MinKey: %v, MaxKey: %v)",
                teleportChunk.Get(),
                teleportChunk->BoundaryKeys()->MinKey,
                teleportChunk->BoundaryKeys()->MaxKey);
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

        Jobs_.emplace_back(std::make_unique<TLegacyJobStub>());

        THashMap<TLegacyDataSlicePtr, TLegacyKey> openedSlicesLowerLimits;

        auto yielder = CreatePeriodicYielder();

        // An index of a closest teleport chunk to the right of current endpoint.
        int nextTeleportChunk = 0;

        int jobIndex = 0;

        i64 totalDataWeight = 0;

        auto endJob = [&] (TLegacyKey lastKey, bool inclusive) {
            TLegacyKey upperLimit = (inclusive) ? GetKeyPrefixSuccessor(lastKey, Options_.PrimaryPrefixLength, RowBuffer_) : lastKey;
            for (auto iterator = openedSlicesLowerLimits.begin(); iterator != openedSlicesLowerLimits.end(); ) {
                // Save the iterator to the next element because we may possibly erase current iterator.
                auto nextIterator = std::next(iterator);
                const auto& dataSlice = iterator->first;
                auto& lowerLimit = iterator->second;
                auto exactDataSlice = CreateInputDataSlice(dataSlice, lowerLimit, upperLimit);
                exactDataSlice->CopyPayloadFrom(*dataSlice);
                auto inputCookie = GetOrCrash(DataSliceToInputCookie_, dataSlice);
                exactDataSlice->Tag = inputCookie;
                Jobs_.back()->AddDataSlice(
                    exactDataSlice,
                    inputCookie,
                    true /*isPrimary*/);
                lowerLimit = upperLimit;
                if (lowerLimit >= dataSlice->LegacyUpperLimit().Key) {
                    openedSlicesLowerLimits.erase(iterator);
                }
                iterator = nextIterator;
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
                    Jobs_.emplace_back(std::make_unique<TLegacyJobStub>());

                    YT_LOG_TRACE("Sorted job details (JobIndex: %v, BuiltJobCount: %v, Details: %v)",
                        jobIndex,
                        static_cast<int>(Jobs_.size()) - 1,
                        Jobs_.back()->GetDebugString());
                } else {
                    YT_LOG_DEBUG("Sorted job skipped (JobIndex: %v, BuiltJobCount: %v, PrimaryDataSize: %v, "
                        "PreliminaryForeignDataSize: %v, LowerPrimaryKey: %v, UpperPrimaryKey: %v)",
                        jobIndex,
                        static_cast<int>(Jobs_.size()) - 1,
                        Jobs_.back()->GetPrimaryDataWeight(),
                        Jobs_.back()->GetPreliminaryForeignDataWeight(),
                        Jobs_.back()->LowerPrimaryKey(),
                        Jobs_.back()->UpperPrimaryKey());
                    Jobs_.back() = std::make_unique<TLegacyJobStub>();
                }
                ++jobIndex;
            }
        };

        auto addBarrier = [&] () {
            Jobs_.back()->SetIsBarrier(true);
            Jobs_.emplace_back(std::make_unique<TLegacyJobStub>());
        };

        for (int index = 0, nextKeyIndex = 0; index < std::ssize(Endpoints_); ++index) {
            yielder.TryYield();
            auto key = Endpoints_[index].Key;
            while (nextKeyIndex != std::ssize(Endpoints_) && Endpoints_[nextKeyIndex].Key == key) {
                ++nextKeyIndex;
            }

            auto nextKey = (nextKeyIndex == std::ssize(Endpoints_)) ? TLegacyKey() : Endpoints_[nextKeyIndex].Key;
            bool nextKeyIsLeft = (nextKeyIndex == std::ssize(Endpoints_)) ? false : Endpoints_[nextKeyIndex].Type == EEndpointType::Left;

            while (nextTeleportChunk < std::ssize(TeleportChunks_) &&
                CompareRows(TeleportChunks_[nextTeleportChunk]->BoundaryKeys()->MinKey, key, Options_.PrimaryPrefixLength) < 0)
            {
                ++nextTeleportChunk;
            }

            if (Endpoints_[index].Type == EEndpointType::Left) {
                openedSlicesLowerLimits[Endpoints_[index].DataSlice] = TLegacyKey();
            } else if (Endpoints_[index].Type == EEndpointType::Right) {
                const auto& dataSlice = Endpoints_[index].DataSlice;
                auto it = openedSlicesLowerLimits.find(dataSlice);
                // It might have happened that we already removed this slice from the
                // `openedSlicesLowerLimits` during one of the previous `endJob` calls.
                if (it != openedSlicesLowerLimits.end()) {
                    auto exactDataSlice = CreateInputDataSlice(dataSlice, it->second);
                    exactDataSlice->CopyPayloadFrom(*dataSlice);
                    auto inputCookie = GetOrCrash(DataSliceToInputCookie_, dataSlice);
                    exactDataSlice->Tag = inputCookie;
                    Jobs_.back()->AddDataSlice(exactDataSlice, inputCookie, true /*isPrimary*/);
                    openedSlicesLowerLimits.erase(it);
                }
            } else if (Endpoints_[index].Type == EEndpointType::ForeignRight) {
                Jobs_.back()->AddPreliminaryForeignDataSlice(Endpoints_[index].DataSlice);
            }

            // Is set to true if we decide to end here. The decision logic may be
            // different depending on if we have user-provided pivot keys.
            bool endHere = false;
            bool addBarrierHere = false;

            if (Options_.PivotKeys.empty()) {
                double retryFactor = std::pow(JobSizeConstraints_->GetDataWeightPerJobRetryFactor(), RetryIndex_);
                bool jobIsLargeEnough =
                    Jobs_.back()->GetPreliminarySliceCount() + std::ssize(openedSlicesLowerLimits) > JobSizeConstraints_->GetMaxDataSlicesPerJob() ||
                    Jobs_.back()->GetPreliminaryDataWeight() >= GetDataWeightPerJob() * retryFactor ||
                    Jobs_.back()->GetPrimaryDataWeight() >= GetPrimaryDataWeightPerJob() * retryFactor;

                // If next teleport chunk is closer than next data slice then we are obligated to close the job here.
                bool beforeTeleportChunk = nextKeyIndex == index + 1 &&
                    nextKeyIsLeft &&
                    (nextTeleportChunk != std::ssize(TeleportChunks_) &&
                    CompareRows(TeleportChunks_[nextTeleportChunk]->BoundaryKeys()->MinKey, nextKey, Options_.PrimaryPrefixLength) <= 0);

                // If key guarantee is enabled, we can not end here if next data slice may contain the same reduce key.
                bool canEndHere = !Options_.EnableKeyGuarantee || index + 1 == nextKeyIndex;

                // The contrary would mean that teleport chunk was chosen incorrectly, because teleport chunks
                // should not normally intersect the other data slices.
                YT_VERIFY(!(beforeTeleportChunk && !canEndHere));

                endHere = canEndHere && (beforeTeleportChunk || jobIsLargeEnough);
                addBarrierHere = beforeTeleportChunk;
            } else {
                // We may end jobs only at the pivot keys.
                endHere = Endpoints_[index].Type == EEndpointType::PivotKey;
                addBarrierHere = true;
            }

            if (endHere) {
                bool inclusive = Endpoints_[index].Type != EEndpointType::PivotKey;
                endJob(key, inclusive);
                if (addBarrierHere) {
                    addBarrier();
                }
            }
        }
        endJob(MaxKey(), true /*inclusive*/);

        JobSizeConstraints_->UpdateInputDataWeight(totalDataWeight);

        if (!Jobs_.empty() && Jobs_.back()->GetSliceCount() == 0) {
            Jobs_.pop_back();
        }
        YT_LOG_DEBUG("Jobs created (Count: %v)", Jobs_.size());
    }

    void AttachForeignSlices()
    {
        auto yielder = CreatePeriodicYielder();
        for (auto& ForeignDataSlice : ForeignDataSlices_) {
            yielder.TryYield();

            int startJobIndex = 0;

            for (const auto& foreignDataSlice : ForeignDataSlice) {
                // Skip jobs that are entirely to the left of foreign data slice.
                while (startJobIndex < std::ssize(Jobs_)) {
                    const auto& job = Jobs_[startJobIndex];
                    if (job->GetIsBarrier()) {
                        // Job is a barrier, ignore it.
                        ++startJobIndex;
                    } else if (CompareRows(job->UpperPrimaryKey(), foreignDataSlice->LegacyLowerLimit().Key, Options_.ForeignPrefixLength) < 0) {
                        // Job's rightmost key is to the left of the slice's leftmost key.
                        ++startJobIndex;
                    } else {
                        break;
                    }
                }

                if (startJobIndex == std::ssize(Jobs_)) {
                    break;
                }

                int jobIndex = startJobIndex;
                while (jobIndex < std::ssize(Jobs_)) {
                    yielder.TryYield();

                    const auto& job = Jobs_[jobIndex];
                    if (job->GetIsBarrier()) {
                        // Job is a barrier, ignore it.
                        ++jobIndex;
                        continue;
                    }

                    if (CompareRows(job->LowerPrimaryKey(), foreignDataSlice->LegacyUpperLimit().Key, Options_.ForeignPrefixLength) <= 0) {
                        // Job's key range intersects with foreign slice's key range, add foreign data slice into the job.
                        auto exactForeignDataSlice = CreateInputDataSlice(
                            foreignDataSlice,
                            GetKeyPrefix(job->LowerPrimaryKey(), Options_.ForeignPrefixLength, RowBuffer_),
                            GetKeyPrefixSuccessor(job->UpperPrimaryKey(), Options_.ForeignPrefixLength, RowBuffer_));
                        exactForeignDataSlice->CopyPayloadFrom(*foreignDataSlice);
                        auto inputCookie = GetOrCrash(DataSliceToInputCookie_, foreignDataSlice);
                        exactForeignDataSlice->Tag = inputCookie;
                        ++TotalSliceCount_;
                        // exactForeignDataSlice->TransformToNew(RowBuffer_, Options_.ForeignPrefixLength);
                        job->AddDataSlice(
                            exactForeignDataSlice,
                            inputCookie,
                            /*isPrimary=*/false);
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

DEFINE_REFCOUNTED_TYPE(TLegacySortedJobBuilder)

////////////////////////////////////////////////////////////////////////////////

ILegacySortedJobBuilderPtr CreateLegacySortedJobBuilder(
    const TSortedJobOptions& options,
    IJobSizeConstraintsPtr jobSizeConstraints,
    const TRowBufferPtr& rowBuffer,
    const std::vector<TInputChunkPtr>& teleportChunks,
    int retryIndex,
    const TLogger& logger)
{
    return New<TLegacySortedJobBuilder>(options, std::move(jobSizeConstraints), rowBuffer, teleportChunks, retryIndex, logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
