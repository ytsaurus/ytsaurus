#include "sorted_chunk_pool.h"

#include "job_manager.h"
#include "helpers.h"

#include <yt/server/controller_agent/helpers.h>
#include <yt/server/controller_agent/operation_controller.h>
#include <yt/server/controller_agent/input_chunk_mapping.h>
#include <yt/server/controller_agent/job_size_constraints.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/table_client/chunk_slice_fetcher.h>
#include <yt/client/table_client/row_buffer.h>

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
    Persist(context, LogDetails);
}

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
        , TeleportChunkSampler_(New<TBernoulliSampler>(JobSizeConstraints_->GetSamplingRate()))
        , SupportLocality_(options.SupportLocality)
        , OperationId_(options.OperationId)
        , Task_(options.Task)
    {
        ForeignDataSlicesByStreamIndex_.resize(InputStreamDirectory_.GetDescriptorCount());
        Logger.AddTag("ChunkPoolId: %v", ChunkPoolId_);
        Logger.AddTag("OperationId: %v", OperationId_);
        Logger.AddTag("Task: %v", Task_);
        JobManager_->SetLogger(Logger);

        LOG_DEBUG("Sorted chunk pool created (EnableKeyGuarantee: %v, PrimaryPrefixLength: %v, "
            "ForeignPrefixLength: %v, DataWeightPerJob: %v, "
            "PrimaryDataWeightPerJob: %v, MaxDataSlicesPerJob: %v, InputSliceDataWeight: %v)",
            SortedJobOptions_.EnableKeyGuarantee,
            SortedJobOptions_.PrimaryPrefixLength,
            SortedJobOptions_.ForeignPrefixLength,
            JobSizeConstraints_->GetDataWeightPerJob(),
            JobSizeConstraints_->GetPrimaryDataWeightPerJob(),
            JobSizeConstraints_->GetMaxDataSlicesPerJob(),
            JobSizeConstraints_->GetInputSliceDataWeight());
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

    virtual void Resume(IChunkPoolInput::TCookie cookie) override
    {
        Stripes_[cookie].Resume();
        if (Finished) {
            JobManager_->Resume(cookie);
        }
    }

    virtual void Reset(IChunkPoolInput::TCookie cookie, TChunkStripePtr stripe, TInputChunkMappingPtr mapping) override
    {
        for (int index = 0; index < Stripes_.size(); ++index) {
            auto newStripe = (index == cookie) ? stripe : mapping->GetMappedStripe(Stripes_[index].GetStripe());
            Stripes_[index].Reset(newStripe);
        }
        if (Finished) {
            InvalidateCurrentJobs();
            DoFinish();
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
        Persist(context, ForeignDataSlicesByStreamIndex_);
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
        Persist(context, TeleportChunkSampler_);
        Persist(context, SortedJobOptions_);
        Persist(context, TotalDataSliceCount_);

        if (context.IsLoad()) {
            Logger.AddTag("ChunkPoolId: %v", ChunkPoolId_);
            Logger.AddTag("OperationId: %v", OperationId_);
            Logger.AddTag("Task: %v", Task_);
            JobManager_->SetLogger(Logger);
        }
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortedChunkPool, 0x91bca805);

    //! All options necessary for sorted job builder.
    TSortedJobOptions SortedJobOptions_;

    //! A factory that is used to spawn chunk slice fetcher.
    IChunkSliceFetcherFactoryPtr ChunkSliceFetcherFactory_;

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

    //! Stores all foreign data slices grouped by input stream index.
    std::vector<std::vector<TInputDataSlicePtr>> ForeignDataSlicesByStreamIndex_;

    IJobSizeConstraintsPtr JobSizeConstraints_;
    TBernoulliSamplerPtr TeleportChunkSampler_;

    bool SupportLocality_ = false;

    TLogger Logger = ChunkPoolLogger;

    TOperationId OperationId_;
    TString Task_;

    TGuid ChunkPoolId_ = TGuid::Create();

    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    i64 TotalDataSliceCount_ = 0;

    //! This method processes all input stripes that do not correspond to teleported chunks
    //! and either slices them using ChunkSliceFetcher (for unversioned stripes) or leaves them as is
    //! (for versioned stripes).
    void FetchNonTeleportDataSlices(const ISortedJobBuilderPtr& builder)
    {
        auto chunkSliceFetcher = ChunkSliceFetcherFactory_ ? ChunkSliceFetcherFactory_->CreateChunkSliceFetcher() : nullptr;

        // If chunkSliceFetcher == nullptr, we form chunk slices manually by putting them
        // into this vector.
        std::vector<TInputChunkSlicePtr> unversionedChunkSlices;

        THashMap<TInputChunkPtr, IChunkPoolInput::TCookie> unversionedInputChunkToInputCookie;
        THashMap<TInputChunkPtr, int> unversionedInputChunkToInputStreamIndex;

        std::vector<std::pair<TInputDataSlicePtr, IChunkPoolInput::TCookie>> nonTeleportDataSlices;

        //! Either add data slice to builder or put it into `ForeignDataSlicesByStreamIndex_` depending on whether
        //! it is primary or foreign.
        auto processDataSlice = [&] (const TInputDataSlicePtr& dataSlice, int inputCookie) {
            if (InputStreamDirectory_.GetDescriptor(dataSlice->InputStreamIndex).IsPrimary()) {
                builder->AddPrimaryDataSlice(dataSlice, inputCookie);
            } else {
                dataSlice->Tag = inputCookie;
                ForeignDataSlicesByStreamIndex_[dataSlice->InputStreamIndex].emplace_back(dataSlice);
            }
        };

        for (int inputCookie = 0; inputCookie < Stripes_.size(); ++inputCookie) {
            const auto& suspendableStripe = Stripes_[inputCookie];
            const auto& stripe = suspendableStripe.GetStripe();

            if (suspendableStripe.GetTeleport()) {
                continue;
            }

            for (const auto& dataSlice : stripe->DataSlices) {
                // Unversioned data slices should be additionally sliced using chunkSliceFetcher,
                // while versioned slices are taken as is.
                if (dataSlice->Type == EDataSourceType::UnversionedTable) {
                    auto inputChunk = dataSlice->GetSingleUnversionedChunkOrThrow();
                    if (chunkSliceFetcher) {
                        if (SortedJobOptions_.LogDetails) {
                            LOG_DEBUG("Slicing chunk (ChunkId: %v, DataWeight: %v)",
                                inputChunk->ChunkId(),
                                inputChunk->GetDataWeight());
                        }
                        chunkSliceFetcher->AddChunk(inputChunk);
                    } else {
                        auto chunkSlice = CreateInputChunkSlice(inputChunk);
                        InferLimitsFromBoundaryKeys(chunkSlice, RowBuffer_, PrimaryPrefixLength_);
                        unversionedChunkSlices.emplace_back(std::move(chunkSlice));
                    }

                    unversionedInputChunkToInputCookie[inputChunk] = inputCookie;
                    unversionedInputChunkToInputStreamIndex[inputChunk] = stripe->GetInputStreamIndex();
                } else {
                    processDataSlice(dataSlice, inputCookie);
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
                    processDataSlice(dataSlice, inputCookie);
                }
            } else {
                auto dataSlice = CreateUnversionedInputDataSlice(chunkSlice);
                dataSlice->InputStreamIndex = inputStreamIndex;
                processDataSlice(dataSlice, inputCookie);
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
        THashMap<TKey, int> singleKeySliceNumber;
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

        // Some chunks will be dropped due to sampling.
        int droppedTeleportChunkCount = 0;

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
                if (TeleportChunkSampler_->Sample()) {
                    TeleportChunks_.emplace_back(teleportCandidate);
                } else {
                    // Teleport chunk to /dev/null. He did not make it.
                    ++droppedTeleportChunkCount;
                }
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
                // This is possible only when both chunks contain the same only key or we comparing chunk with itself.
                YCHECK(&lhs == &rhs || lhs->BoundaryKeys()->MinKey == lhs->BoundaryKeys()->MaxKey);
                return false;
            });

        i64 totalTeleportChunkSize = 0;
        for (const auto& teleportChunk : TeleportChunks_) {
            totalTeleportChunkSize += teleportChunk->GetUncompressedDataSize();
        }

        LOG_DEBUG("Chunks teleported (ChunkCount: %v, DroppedChunkCount: %v, TotalSize: %v)",
            TeleportChunks_.size(),
            droppedTeleportChunkCount,
            totalTeleportChunkSize);
    }

    void PrepareForeignDataSlices(const ISortedJobBuilderPtr& builder)
    {
        auto yielder = CreatePeriodicYielder();

        for (int streamIndex = 0; streamIndex < ForeignDataSlicesByStreamIndex_.size(); ++streamIndex) {
            if (!InputStreamDirectory_.GetDescriptor(streamIndex).IsForeign()) {
                continue;
            }

            yielder.TryYield();

            auto& dataSlices = ForeignDataSlicesByStreamIndex_[streamIndex];

            // In most cases the foreign table stripes follow in sorted order, but still let's ensure that.
            auto cmpStripesByKey = [&] (const TInputDataSlicePtr& lhs, const TInputDataSlicePtr& rhs) {
                const auto& lhsLowerLimit = lhs->LowerLimit().Key;
                const auto& lhsUpperLimit = lhs->UpperLimit().Key;
                const auto& rhsLowerLimit = rhs->LowerLimit().Key;
                const auto& rhsUpperLimit = rhs->UpperLimit().Key;
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
            if (!std::is_sorted(dataSlices.begin(), dataSlices.end(), cmpStripesByKey)) {
                std::stable_sort(dataSlices.begin(), dataSlices.end(), cmpStripesByKey);
            }
            for (const auto& dataSlice : dataSlices) {
                // Input cookie is temporarily saved to data slice tag.
                builder->AddForeignDataSlice(dataSlice, *dataSlice->Tag /* inputCookie */);
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

        FindTeleportChunks();

        bool succeeded = false;
        ISortedJobBuilderPtr builder;
        std::vector<std::unique_ptr<TJobStub>> jobStubs;
        std::vector<TError> errors;
        for (int retryIndex = 0; retryIndex < JobSizeConstraints_->GetMaxBuildRetryCount(); ++retryIndex) {
            try {
                builder = CreateSortedJobBuilder(
                    SortedJobOptions_,
                    JobSizeConstraints_,
                    RowBuffer_,
                    TeleportChunks_,
                    false /* inSplit */,
                    retryIndex,
                    Logger);

                FetchNonTeleportDataSlices(builder);
                PrepareForeignDataSlices(builder);
                jobStubs = builder->Build();
                succeeded = true;
                break;
            } catch (TErrorException& ex) {
                if (ex.Error().FindMatching(EErrorCode::DataSliceLimitExceeded)) {
                    LOG_DEBUG(ex,
                        "Retriable error during job building (RetryIndex: %v, MaxBuildRetryCount: %v)",
                        retryIndex,
                        JobSizeConstraints_->GetMaxBuildRetryCount());
                    errors.emplace_back(std::move(ex.Error()));
                    continue;
                }
                throw;
            }
        }
        if (!succeeded) {
            LOG_DEBUG("Retry limit exceeded (MaxBuildRetryCount: %v)", JobSizeConstraints_->GetMaxBuildRetryCount());
            THROW_ERROR_EXCEPTION("Retry limit exceeded while building jobs")
                << errors;
        }
        JobManager_->AddJobs(std::move(jobStubs));

        if (JobSizeConstraints_->GetSamplingRate()) {
            JobManager_->Enlarge(
                JobSizeConstraints_->GetDataWeightPerJob(),
                JobSizeConstraints_->GetPrimaryDataWeightPerJob());
        }

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
            JobSizeConstraints_->GetMaxPrimaryDataWeightPerJob(),
            JobSizeConstraints_->GetInputSliceDataWeight(),
            JobSizeConstraints_->GetInputSliceRowCount(),
            Null /* samplingRate */);

        // Teleport chunks do not affect the job split process since each original
        // job is already located between the teleport chunks.
        std::vector<TInputChunkPtr> teleportChunks;
        auto splitSortedJobOptions = SortedJobOptions_;
        // We do not want to yield during job splitting because it may potentially lead
        // to snapshot creation that will catch pool in inconsistent state.
        splitSortedJobOptions.EnablePeriodicYielder = false;
        auto builder = CreateSortedJobBuilder(
            splitSortedJobOptions,
            std::move(jobSizeConstraints),
            RowBuffer_,
            teleportChunks,
            true /* inSplit */,
            0 /* retryIndex */,
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
        for (auto& stripe : Stripes_) {
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
