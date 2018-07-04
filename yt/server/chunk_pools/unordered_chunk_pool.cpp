#include "unordered_chunk_pool.h"

#include "helpers.h"

#include <yt/ytlib/chunk_client/input_data_slice.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/table_client/row_buffer.h>

#include <yt/server/controller_agent/job_size_adjuster.h>
#include <yt/server/controller_agent/controller_agent.h>

#include <yt/core/misc/numeric_helpers.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYT {
namespace NChunkPools {

using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NScheduler;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

struct TExtractedStripeList
    : public TIntrinsicRefCounted
{
    //! Used only for persistence.
    TExtractedStripeList() = default;
    explicit TExtractedStripeList(IChunkPoolOutput::TCookie cookie)
        : StripeList(New<TChunkStripeList>())
        , Cookie(cookie)
    { }

    int UnavailableStripeCount = 0;
    std::vector<int> StripeIndexes;
    TChunkStripeListPtr StripeList;
    IChunkPoolOutput::TCookie Cookie;

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;
        Persist(context, UnavailableStripeCount);
        Persist(context, StripeIndexes);
        Persist(context, StripeList);
        Persist(context, Cookie);
    }
};

DECLARE_REFCOUNTED_TYPE(TExtractedStripeList)
DEFINE_REFCOUNTED_TYPE(TExtractedStripeList)

////////////////////////////////////////////////////////////////////////////////

class TUnorderedChunkPool
    : public TChunkPoolInputBase
    , public TChunkPoolOutputWithCountersBase
    , public IChunkPool
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
    , public TRefTracked<TUnorderedChunkPool>
{
public:
    //! For persistence only.
    TUnorderedChunkPool()
        : FreePendingDataWeight(-1)
        , SuspendedDataWeight(-1)
        , UnavailableLostCookieCount(-1)
        , MaxBlockSize(-1)
    { }

    TUnorderedChunkPool(
        const TUnorderedChunkPoolOptions& options,
        TInputStreamDirectory directory)
        : OperationId_(options.OperationId)
        , Task_(options.Task)
        , JobSizeConstraints_(options.JobSizeConstraints)
        , Mode(options.Mode)
        , MinTeleportChunkSize_(options.MinTeleportChunkSize)
        , MinTeleportChunkDataWeight_(options.MinTeleportChunkDataWeight)
        , SliceErasureChunksByParts_(options.SliceErasureChunksByParts)
        , InputStreamDirectory_(std::move(directory))
    {
        Logger.AddTag("ChunkPoolId: %v", ChunkPoolId_);
        Logger.AddTag("OperationId: %v", OperationId_);
        Logger.AddTag("Task: %v", Task_);

        if (Mode == EUnorderedChunkPoolMode::Normal) {
            JobCounter->Set(JobSizeConstraints_->GetJobCount());
        } else {
            JobCounter->Set(0);
        }

        if (options.JobSizeAdjusterConfig && JobSizeConstraints_->CanAdjustDataWeightPerJob()) {
            JobSizeAdjuster = CreateJobSizeAdjuster(
                JobSizeConstraints_->GetDataWeightPerJob(),
                options.JobSizeAdjusterConfig);
            // ToDo(psushin): add logging here.
            // ToDo(max42): Hi psushin, which logging do you want here?
        }
    }

    // IChunkPoolInput implementation.

    virtual IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        // No check for finished here, because stripes may be added for interrupted jobs.

        auto cookie = InputCookieToInternalCookies_.size();
        InputCookieToInternalCookies_.emplace_back();
        InputCookieIsSuspended_.emplace_back(false);

        for (const auto& dataSlice : stripe->DataSlices) {
            AddDataSlice(dataSlice, cookie);
        }

        return cookie;
    }

    virtual void Finish() override
    {
        if (Finished) {
            UpdateJobCounter();
        } else {
            TChunkPoolInputBase::Finish();
        }
    }

    virtual void Suspend(IChunkPoolInput::TCookie inputCookie) override
    {
        YCHECK(!InputCookieIsSuspended_[inputCookie]);
        InputCookieIsSuspended_[inputCookie] = true;
        for (auto cookie : InputCookieToInternalCookies_[inputCookie]) {
            DoSuspend(cookie);
        }
    }

    void DoSuspend(IChunkPoolInput::TCookie cookie)
    {
        auto& suspendableStripe = Stripes[cookie];
        suspendableStripe.Suspend();

        auto outputCookie = suspendableStripe.GetExtractedCookie();
        if (outputCookie == IChunkPoolOutput::NullCookie) {
            Unregister(cookie);
            SuspendedDataWeight += suspendableStripe.GetStatistics().DataWeight;
        } else {
            auto it = ExtractedLists.find(outputCookie);
            YCHECK(it != ExtractedLists.end());
            const auto& extractedStripeList = it->second;

            if (LostCookies.find(outputCookie) != LostCookies.end() &&
                extractedStripeList->UnavailableStripeCount == 0)
            {
                ++UnavailableLostCookieCount;
            }
            ++extractedStripeList->UnavailableStripeCount;
        }
    }

    virtual void Resume(IChunkPoolInput::TCookie inputCookie) override
    {
        YCHECK(InputCookieIsSuspended_[inputCookie]);
        InputCookieIsSuspended_[inputCookie] = false;
        for (auto cookie : InputCookieToInternalCookies_[inputCookie]) {
            DoResume(cookie);
        }
    }

    void DoResume(IChunkPoolInput::TCookie cookie)
    {
        auto& suspendableStripe = Stripes[cookie];
        suspendableStripe.Resume();

        auto outputCookie = suspendableStripe.GetExtractedCookie();
        if (outputCookie == IChunkPoolOutput::NullCookie) {
            Register(cookie);
            SuspendedDataWeight -= suspendableStripe.GetStatistics().DataWeight;
            YCHECK(SuspendedDataWeight >= 0);
        } else {
            auto it = ExtractedLists.find(outputCookie);
            YCHECK(it != ExtractedLists.end());
            const auto& extractedStripeList = it->second;
            --extractedStripeList->UnavailableStripeCount;

            if (LostCookies.find(outputCookie) != LostCookies.end() &&
                extractedStripeList->UnavailableStripeCount == 0)
            {
                --UnavailableLostCookieCount;
            }
        }
    }

    // IChunkPoolOutput implementation.

    virtual bool IsCompleted() const override
    {
        return
            Finished &&
            LostCookies.empty() &&
            SuspendedDataWeight == 0 &&
            PendingGlobalStripes.empty() &&
            JobCounter->GetRunning() == 0;
    }

    virtual int GetTotalJobCount() const override
    {
        return Mode == EUnorderedChunkPoolMode::AutoMerge
            ? GetPendingJobCount() + JobCounter->GetRunning() + JobCounter->GetCompletedTotal()
            : JobCounter->GetTotal();
    }

    virtual i64 GetDataSliceCount() const override
    {
        return TotalDataSliceCount;
    }

    virtual int GetPendingJobCount() const override
    {
        if (Mode == EUnorderedChunkPoolMode::Normal) {
            // TODO(babenko): refactor
            bool hasAvailableLostJobs = LostCookies.size() > UnavailableLostCookieCount;
            if (hasAvailableLostJobs) {
                return JobCounter->GetPending() - UnavailableLostCookieCount;
            }

            int freePendingJobCount = GetFreePendingJobCount();
            YCHECK(freePendingJobCount >= 0);
            YCHECK(Mode == EUnorderedChunkPoolMode::AutoMerge ||
                   !(FreePendingDataWeight > 0 && freePendingJobCount == 0 && JobCounter->GetInterruptedTotal() == 0));

            if (freePendingJobCount == 0) {
                return 0;
            }

            if (FreePendingDataWeight == 0) {
                return 0;
            }

            return freePendingJobCount;
        } else {
            return PendingGlobalStripes.empty() ? 0 : 1;
        }
    }

    virtual TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override
    {
        if (!ExtractedLists.empty()) {
            TChunkStripeStatisticsVector result;
            for (const auto& index : ExtractedLists.begin()->second->StripeIndexes) {
                result.push_back(Stripes[index].GetStripe()->GetStatistics());
            }
            return result;
        }

        TChunkStripeStatistics stat;
        // Typically unordered pool has one chunk per stripe.
        // NB: Cannot estimate MaxBlockSize to fill stat field here.
        stat.ChunkCount = std::max(
            static_cast<i64>(1),
            static_cast<i64>(PendingGlobalStripes.size()) / GetPendingJobCount());
        stat.DataWeight = std::max(
            static_cast<i64>(1),
            GetPendingDataWeight() / GetPendingJobCount());
        stat.RowCount = std::max(
            static_cast<i64>(1),
            GetTotalRowCount() / GetTotalJobCount());
        stat.MaxBlockSize = MaxBlockSize;

        TChunkStripeStatisticsVector result;
        result.push_back(stat);
        return result;
    }

    virtual i64 GetLocality(TNodeId nodeId) const override
    {
        auto it = NodeIdToEntry.find(nodeId);
        return it == NodeIdToEntry.end() ? 0 : it->second.Locality;
    }

    virtual IChunkPoolOutput::TCookie Extract(TNodeId nodeId) override
    {
        if (GetPendingJobCount() == 0) {
            return IChunkPoolOutput::NullCookie;
        }

        TChunkStripeListPtr list;
        IChunkPoolOutput::TCookie cookie;

        if (LostCookies.size() == UnavailableLostCookieCount) {
            auto extractedStripeList = CreateAndRegisterExtractedStripeList();
            list = extractedStripeList->StripeList;
            cookie = extractedStripeList->Cookie;

            i64 idealDataWeightPerJob = GetIdealDataWeightPerJob();

            // Take local chunks first.
            if (nodeId != InvalidNodeId) {
                auto it = NodeIdToEntry.find(nodeId);
                if (it != NodeIdToEntry.end()) {
                    const auto& entry = it->second;
                    AddAndUnregisterStripes(
                        extractedStripeList,
                        entry.StripeIndexes.begin(),
                        entry.StripeIndexes.end(),
                        nodeId,
                        idealDataWeightPerJob);
                }
            }

            // Take non-local chunks.
            AddAndUnregisterStripes(
                extractedStripeList,
                PendingGlobalStripes.begin(),
                PendingGlobalStripes.end(),
                nodeId,
                idealDataWeightPerJob);

        } else {
            auto lostIt = LostCookies.begin();
            while (true) {
                cookie = *lostIt;
                auto it = ExtractedLists.find(cookie);
                YCHECK(it != ExtractedLists.end());
                if (it->second->UnavailableStripeCount == 0) {
                    LostCookies.erase(lostIt);
                    YCHECK(ReplayCookies.insert(cookie).second);
                    list = GetStripeList(cookie);
                    break;
                }
                YCHECK(++lostIt != LostCookies.end());
            }
        }

        if (Mode == EUnorderedChunkPoolMode::AutoMerge) {
            JobCounter->Increment(1);
        }
        JobCounter->Start(1);
        DataWeightCounter->Start(list->TotalDataWeight);
        RowCounter->Start(list->TotalRowCount);

        UpdateJobCounter();

        return cookie;
    }

    TExtractedStripeListPtr GetExtractedStripeList(IChunkPoolOutput::TCookie cookie) const
    {
        auto it = ExtractedLists.find(cookie);
        YCHECK(it != ExtractedLists.end());
        return it->second;
    }

    virtual TChunkStripeListPtr GetStripeList(IChunkPoolOutput::TCookie cookie) override
    {
        const auto& stripeList = GetExtractedStripeList(cookie)->StripeList;
        for (const auto& stripe : stripeList->Stripes) {
            for (const auto& dataSlice : stripe->DataSlices) {
                YCHECK(dataSlice->Tag);
            }
        }
        return stripeList;
    }

    virtual int GetStripeListSliceCount(IChunkPoolOutput::TCookie cookie) const override
    {
        return GetExtractedStripeList(cookie)->StripeList->TotalChunkCount;
    }

    virtual void Completed(IChunkPoolOutput::TCookie cookie, const TCompletedJobSummary& jobSummary) override
    {
        const auto& list = GetStripeList(cookie);

        JobCounter->Completed(1, jobSummary.InterruptReason);
        DataWeightCounter->Completed(list->TotalDataWeight);
        RowCounter->Completed(list->TotalRowCount);

        if (jobSummary.InterruptReason != EInterruptReason::None) {
            list->Stripes.clear();
            list->Stripes.reserve(jobSummary.ReadInputDataSlices.size());
            for (const auto& dataSlice : jobSummary.ReadInputDataSlices) {
                list->Stripes.emplace_back(New<TChunkStripe>(dataSlice));
            }
            SplitJob(jobSummary.UnreadInputDataSlices, jobSummary.SplitJobCount);
        }

        //! If we don't have enough pending jobs - don't adjust data size per job.
        if (JobSizeAdjuster && JobCounter->GetPending() > JobCounter->GetRunning()) {
            JobSizeAdjuster->UpdateStatistics(jobSummary);
            UpdateJobCounter();
        }

        // NB: may fail.
        ReplayCookies.erase(cookie);
    }

    void SplitJob(const std::vector<NChunkClient::TInputDataSlicePtr>& dataSlices, int jobCount)
    {
        i64 unreadRowCount = GetCumulativeRowCount(dataSlices);
        i64 rowsPerJob = DivCeil<i64>(unreadRowCount, jobCount);
        i64 rowsToAdd = rowsPerJob;
        int sliceIndex = 0;
        auto currentDataSlice = dataSlices[0];
        TChunkStripePtr stripe = New<TChunkStripe>(false /* foreign */, true /* solid */);
        auto flushStripe = [&] {
            AddStripe(std::move(stripe));
            stripe = New<TChunkStripe>(false /* foreign */, true /* solid */);
        };
        while (true) {
            i64 sliceRowCount = currentDataSlice->GetRowCount();
            if (currentDataSlice->Type == EDataSourceType::UnversionedTable && sliceRowCount > rowsToAdd) {
                auto split = currentDataSlice->SplitByRowIndex(rowsToAdd);
                split.first->Tag = currentDataSlice->Tag;
                split.second->Tag = currentDataSlice->Tag;
                stripe->DataSlices.emplace_back(std::move(split.first));
                rowsToAdd = 0;
                currentDataSlice = std::move(split.second);
            } else {
                stripe->DataSlices.emplace_back(std::move(currentDataSlice));
                rowsToAdd -= sliceRowCount;
                ++sliceIndex;
                if (sliceIndex == static_cast<int>(dataSlices.size())) {
                    break;
                }
                currentDataSlice = dataSlices[sliceIndex];
            }
            if (rowsToAdd <= 0) {
                flushStripe();
                rowsToAdd = rowsPerJob;
            }
        }
        if (!stripe->DataSlices.empty()) {
            flushStripe();
        }
    }

    virtual void Failed(IChunkPoolOutput::TCookie cookie) override
    {
        const auto& extractedStripeList = GetExtractedStripeList(cookie);
        const auto& list = extractedStripeList->StripeList;

        JobCounter->Failed(1);
        DataWeightCounter->Failed(list->TotalDataWeight);
        RowCounter->Failed(list->TotalRowCount);

        ReinstallStripeList(extractedStripeList, cookie);
    }

    virtual void Aborted(IChunkPoolOutput::TCookie cookie, EAbortReason reason) override
    {
        const auto& extractedStripeList = GetExtractedStripeList(cookie);
        const auto& list = extractedStripeList->StripeList;

        JobCounter->Aborted(1, reason);
        DataWeightCounter->Aborted(list->TotalDataWeight, reason);
        RowCounter->Aborted(list->TotalRowCount, reason);

        ReinstallStripeList(extractedStripeList, cookie);
    }

    virtual void Lost(IChunkPoolOutput::TCookie cookie) override
    {
        const auto& extractedStripeList = GetExtractedStripeList(cookie);
        const auto& list = extractedStripeList->StripeList;

        // No need to respect locality for restarted jobs.
        list->LocalChunkCount = 0;
        list->LocalDataWeight = 0;

        JobCounter->Lost(1);
        DataWeightCounter->Lost(list->TotalDataWeight);
        RowCounter->Lost(list->TotalRowCount);

        YCHECK(LostCookies.insert(cookie).second);
        if (extractedStripeList->UnavailableStripeCount > 0) {
            ++UnavailableLostCookieCount;
        }
    }

    // IPersistent implementation.

    virtual void Persist(const TPersistenceContext& context) override
    {
        TChunkPoolInputBase::Persist(context);
        TChunkPoolOutputWithCountersBase::Persist(context);

        using NYT::Persist;
        Persist(context, ChunkPoolId_);
        Persist(context, OperationId_);
        Persist(context, Task_);

        // COMPAT(max42).
        if (context.GetVersion() <= 300011) {
            std::vector<std::vector<int>> old;
            Persist(context, old);
            for (const auto& oldVector : old) {
                InputCookieToInternalCookies_.emplace_back(oldVector.begin(), oldVector.end());
            }
        } else {
            Persist(context, InputCookieToInternalCookies_);
        }
        Persist(context, InputCookieIsSuspended_);
        Persist(context, Stripes);
        Persist(context, JobSizeConstraints_);
        Persist(context, JobSizeAdjuster);
        Persist(context, PendingGlobalStripes);
        Persist(context, FreePendingDataWeight);
        Persist(context, SuspendedDataWeight);
        Persist(context, UnavailableLostCookieCount);
        Persist(context, PendingStripeCount);
        Persist(context, MaxBlockSize);
        Persist(context, NodeIdToEntry);
        Persist(context, OutputCookieGenerator);
        Persist(context, ExtractedLists);
        Persist(context, LostCookies);
        Persist(context, ReplayCookies);
        Persist(context, Mode);
        Persist(context, MinTeleportChunkSize_);
        Persist(context, MinTeleportChunkDataWeight_);
        Persist(context, SliceErasureChunksByParts_);
        Persist(context, InputStreamDirectory_);

        if (context.IsLoad()) {
            Logger.AddTag("ChunkPoolId: %v", ChunkPoolId_);
            Logger.AddTag("OperationId: %v", OperationId_);
            Logger.AddTag("Task: %v", Task_);
        }
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TUnorderedChunkPool, 0xbacd26ad);

    TLogger Logger = ChunkPoolLogger;
    TGuid ChunkPoolId_ = TGuid::Create();
    TOperationId OperationId_;
    TString Task_;

    //! A mappping between input cookies (that are returned and used by controllers) and internal smaller
    //! stripe cookies that are obtained by slicing the input stripes.
    std::vector<THashSet<int>> InputCookieToInternalCookies_;
    std::vector<TSuspendableStripe> Stripes;
    // char is used instead of bool because std::vector<bool> is not currently persistable,
    // and I am too lazy to fix that.
    std::vector<char> InputCookieIsSuspended_;

    IJobSizeConstraintsPtr JobSizeConstraints_;
    std::unique_ptr<IJobSizeAdjuster> JobSizeAdjuster;

    //! Indexes in #Stripes.
    THashSet<int> PendingGlobalStripes;

    i64 FreePendingDataWeight = 0;
    i64 SuspendedDataWeight = 0;
    int UnavailableLostCookieCount = 0;
    i64 PendingStripeCount = 0;

    i64 MaxBlockSize = 0;

    i64 TotalDataSliceCount  = 0;

    struct TLocalityEntry
    {
        TLocalityEntry()
            : Locality(0)
        { }

        //! The total locality associated with this node.
        i64 Locality;

        //! Indexes in #Stripes.
        THashSet<int> StripeIndexes;

        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, Locality);
            Persist(context, StripeIndexes);
        }
    };

    THashMap<TNodeId, TLocalityEntry> NodeIdToEntry;

    TIdGenerator OutputCookieGenerator;

    THashMap<IChunkPoolOutput::TCookie, TExtractedStripeListPtr> ExtractedLists;

    THashSet<IChunkPoolOutput::TCookie> LostCookies;
    THashSet<IChunkPoolOutput::TCookie> ReplayCookies;

    EUnorderedChunkPoolMode Mode;
    i64 MinTeleportChunkSize_ = std::numeric_limits<i64>::max();
    i64 MinTeleportChunkDataWeight_ = std::numeric_limits<i64>::max();
    bool SliceErasureChunksByParts_ = false;

    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    TInputStreamDirectory InputStreamDirectory_;

    // XXX(max42): looks like this comment became obsolete even
    // before I got into this company.
    //! Convert data slice into a list of chunk stripes for further
    //! processing. Each stripe receives exactly one chunk. The
    //! resulting stripes are of approximately equal size. The size
    //! per stripe is either |maxSliceDataSize| or |TotalEstimateInputDataSize / jobCount|,
    //! whichever is smaller. If the resulting list contains less than
    //! |jobCount| stripes then |jobCount| is decreased appropriately.
    void AddDataSlice(const TInputDataSlicePtr dataSlice, IChunkPoolInput::TCookie inputCookie)
    {
        if (dataSlice->Type == EDataSourceType::VersionedTable) {
            dataSlice->Tag = inputCookie;
            AddStripe(New<TChunkStripe>(dataSlice));
        } else {
            const auto& chunk = dataSlice->GetSingleUnversionedChunkOrThrow();

            if ((chunk->IsLargeCompleteChunk(MinTeleportChunkSize_) ||
                chunk->GetDataWeight() >= MinTeleportChunkDataWeight_) &&
                InputStreamDirectory_.GetDescriptor(dataSlice->GetTableIndex()).IsTeleportable())
            {
                TeleportChunks_.emplace_back(chunk);
                return;
            }

            int oldSize = Stripes.size();

            bool hasNontrivialLimits = !chunk->IsCompleteChunk();

            auto codecId = NErasure::ECodec(chunk->GetErasureCodec());
            if (hasNontrivialLimits || codecId == NErasure::ECodec::None || !SliceErasureChunksByParts_) {
                auto slices = dataSlice->ChunkSlices[0]->SliceEvenly(
                    JobSizeConstraints_->GetInputSliceDataWeight(),
                    JobSizeConstraints_->GetInputSliceRowCount(),
                    RowBuffer_);

                for (auto& slice : slices) {
                    auto dataSlice = New<TInputDataSlice>(
                        EDataSourceType::UnversionedTable,
                        TInputDataSlice::TChunkSliceList{slice},
                        slice->LowerLimit(),
                        slice->UpperLimit());
                    dataSlice->Tag = inputCookie;
                    AddStripe(New<TChunkStripe>(dataSlice));
                }
            } else {
                for (const auto& slice : CreateErasureInputChunkSlices(chunk, codecId)) {
                    auto smallerSlices = slice->SliceEvenly(
                        JobSizeConstraints_->GetInputSliceDataWeight(),
                        JobSizeConstraints_->GetInputSliceRowCount(),
                        RowBuffer_);

                    for (auto& smallerSlice : smallerSlices) {
                        auto dataSlice = New <TInputDataSlice>(
                            EDataSourceType::UnversionedTable,
                            TInputDataSlice::TChunkSliceList{std::move(smallerSlice)});
                        dataSlice->Tag = inputCookie;
                        AddStripe(New<TChunkStripe>(dataSlice));
                    }
                }
            }

            LOG_TRACE("Slicing unversioned chunk (ChunkId: %v, DataWeight: %v, SliceDataWeight: %v, SliceRowCount: %v, "
                "SliceCount: %v)",
                chunk->ChunkId(),
                chunk->GetDataWeight(),
                JobSizeConstraints_->GetInputSliceDataWeight(),
                JobSizeConstraints_->GetInputSliceRowCount(),
                Stripes.size() - oldSize);
        }
    }

    void AddStripe(const TChunkStripePtr& stripe)
    {
        int internalCookie = Stripes.size();

        bool suspended = false;
        for (const auto& dataSlice : stripe->DataSlices) {
            YCHECK(dataSlice->Tag);
            auto inputCookie = *dataSlice->Tag;
            InputCookieToInternalCookies_[inputCookie].insert(internalCookie);
            suspended |= InputCookieIsSuspended_[inputCookie];
        }

        ++PendingStripeCount;
        TSuspendableStripe suspendableStripe(stripe);

        Stripes.push_back(suspendableStripe);

        DataWeightCounter->Increment(suspendableStripe.GetStatistics().DataWeight);
        RowCounter->Increment(suspendableStripe.GetStatistics().RowCount);
        MaxBlockSize = std::max(MaxBlockSize, suspendableStripe.GetStatistics().MaxBlockSize);

        TotalDataSliceCount += stripe->DataSlices.size();

        if (stripe->Solid) {
            AddSolid(internalCookie);
        } else {
            Register(internalCookie);
        }

        if (suspended) {
            DoSuspend(internalCookie);
        }
    }

    int GetFreePendingJobCount() const
    {
        return Mode == EUnorderedChunkPoolMode::AutoMerge ? 1 : JobCounter->GetPending() - LostCookies.size();
    }

    i64 GetIdealDataWeightPerJob() const
    {
        if (Mode == EUnorderedChunkPoolMode::AutoMerge) {
            return JobSizeConstraints_->GetDataWeightPerJob();
        }
        int freePendingJobCount = GetFreePendingJobCount();
        YCHECK(freePendingJobCount > 0);
        return std::max(
            static_cast<i64>(1),
            DivCeil<i64>(FreePendingDataWeight + SuspendedDataWeight, freePendingJobCount));
    }

    void UpdateJobCounter()
    {
        if (Mode == EUnorderedChunkPoolMode::AutoMerge) {
            return;
        }

        i64 freePendingJobCount = GetFreePendingJobCount();
        if (Finished && FreePendingDataWeight + SuspendedDataWeight == 0 && freePendingJobCount > 0) {
            // Prune job count if all stripe lists are already extracted.
            JobCounter->Increment(-freePendingJobCount);
            return;
        }

        if (freePendingJobCount == 0 && FreePendingDataWeight + SuspendedDataWeight > 0) {
            // Happens when we hit MaxDataSlicesPerJob or MaxDataWeightPerJob limit.
            JobCounter->Increment(1);
            return;
        }

        if (JobSizeConstraints_->IsExplicitJobCount()) {
            return;
        }

        i64 dataWeightPerJob = JobSizeAdjuster
            ? JobSizeAdjuster->GetDataWeightPerJob()
            : JobSizeConstraints_->GetDataWeightPerJob();

        dataWeightPerJob = std::min(dataWeightPerJob, JobSizeConstraints_->GetMaxDataWeightPerJob());
        i64 newJobCount = DivCeil(FreePendingDataWeight + SuspendedDataWeight, dataWeightPerJob);
        if (newJobCount != freePendingJobCount) {
            JobCounter->Increment(newJobCount - freePendingJobCount);
        }
    }

    void Register(int stripeIndex)
    {
        auto& suspendableStripe = Stripes[stripeIndex];
        YCHECK(suspendableStripe.GetExtractedCookie() == IChunkPoolOutput::NullCookie);

        auto stripe = suspendableStripe.GetStripe();
        for (const auto& dataSlice : stripe->DataSlices) {
            for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                for (auto replica : chunkSlice->GetInputChunk()->GetReplicaList()) {
                    auto locality = chunkSlice->GetLocality(replica.GetReplicaIndex());
                    if (locality > 0) {
                        auto& entry = NodeIdToEntry[replica.GetNodeId()];
                        // NB: do not check that stripe is unique, it may have already been inserted,
                        // since different replicas may reside on the same node during rebalancing.
                        entry.StripeIndexes.insert(stripeIndex);
                        entry.Locality += locality;
                    }
                }
            }
        }

        FreePendingDataWeight += suspendableStripe.GetStatistics().DataWeight;
        YCHECK(PendingGlobalStripes.insert(stripeIndex).second);
    }

    TExtractedStripeListPtr CreateAndRegisterExtractedStripeList()
    {
        auto cookie = OutputCookieGenerator.Next();
        auto pair = ExtractedLists.emplace(cookie, New<TExtractedStripeList>(cookie));
        YCHECK(pair.second);
        const auto& extractedStripeList = pair.first->second;

        return extractedStripeList;
    }

    void AddSolid(int stripeIndex)
    {
        auto& suspendableStripe = Stripes[stripeIndex];
        YCHECK(suspendableStripe.GetExtractedCookie() == IChunkPoolOutput::NullCookie);
        YCHECK(suspendableStripe.GetStripe()->Solid);

        auto extractedStripeList = CreateAndRegisterExtractedStripeList();

        auto stat = suspendableStripe.GetStatistics();

        suspendableStripe.SetExtractedCookie(extractedStripeList->Cookie);
        AddStripeToList(
            suspendableStripe.GetStripe(),
            stat.DataWeight,
            stat.RowCount,
            extractedStripeList->StripeList);

        JobCounter->Increment(1);

        YCHECK(LostCookies.insert(extractedStripeList->Cookie).second);
    }

    void Unregister(int stripeIndex)
    {
        auto& suspendableStripe = Stripes[stripeIndex];

        auto stripe = suspendableStripe.GetStripe();
        for (const auto& dataSlice : stripe->DataSlices) {
            for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                for (auto replica : chunkSlice->GetInputChunk()->GetReplicaList()) {
                    auto locality = chunkSlice->GetLocality(replica.GetReplicaIndex());
                    if (locality > 0) {
                        auto& entry = NodeIdToEntry[replica.GetNodeId()];
                        auto it = entry.StripeIndexes.find(stripeIndex);
                        if (it != entry.StripeIndexes.end()) {
                            entry.StripeIndexes.erase(it);
                        }
                        entry.Locality -= locality;
                    }
                }
            }
        }

        FreePendingDataWeight -= suspendableStripe.GetStatistics().DataWeight;
        YCHECK(PendingGlobalStripes.erase(stripeIndex) == 1);
    }

    template <class TIterator>
    void AddAndUnregisterStripes(
        const TExtractedStripeListPtr& extractedStripeList,
        const TIterator& begin,
        const TIterator& end,
        TNodeId nodeId,
        i64 idealDataWeightPerJob)
    {
        auto& list = extractedStripeList->StripeList;
        size_t oldSize = list->Stripes.size();
        for (auto it = begin; it != end; ++it) {
            if (list->TotalDataWeight >= idealDataWeightPerJob) {
                break;
            }

            // NB: We should ignore check of chunk stripe count in case of last job.
            if (list->Stripes.size() >= JobSizeConstraints_->GetMaxDataSlicesPerJob() &&
                (!JobSizeConstraints_->IsExplicitJobCount() || GetFreePendingJobCount() > 1))
            {
                break;
            }

            auto stripeIndex = *it;
            auto& suspendableStripe = Stripes[stripeIndex];
            auto stat = suspendableStripe.GetStatistics();

            // We should always return at least one stripe, even we get MaxDataWeightPerJob overflow.
            if (list->TotalDataWeight > 0 && list->TotalDataWeight + stat.DataWeight >
                JobSizeConstraints_->GetMaxDataWeightPerJob() &&
                (!JobSizeConstraints_->IsExplicitJobCount() || GetFreePendingJobCount() > 1))
            {
                break;
            }

            // Leave enough stripes if job count is explicitly given.
            if (list->TotalDataWeight > 0 && PendingStripeCount < GetFreePendingJobCount() && JobSizeConstraints_->IsExplicitJobCount()) {
                break;
            }

            extractedStripeList->StripeIndexes.push_back(stripeIndex);
            --PendingStripeCount;

            suspendableStripe.SetExtractedCookie(extractedStripeList->Cookie);
            AddStripeToList(
                suspendableStripe.GetStripe(),
                stat.DataWeight,
                stat.RowCount,
                list,
                nodeId);
        }
        size_t newSize = list->Stripes.size();

        for (size_t index = oldSize; index < newSize; ++index) {
            Unregister(extractedStripeList->StripeIndexes[index]);
        }
    }

    void ReinstallStripeList(const TExtractedStripeListPtr& extractedStripeList, IChunkPoolOutput::TCookie cookie)
    {
        auto replayIt = ReplayCookies.find(cookie);
        if (replayIt == ReplayCookies.end()) {
            for (int stripeIndex : extractedStripeList->StripeIndexes) {
                auto& suspendableStripe = Stripes[stripeIndex];
                suspendableStripe.SetExtractedCookie(IChunkPoolOutput::NullCookie);
                ++PendingStripeCount;
                if (suspendableStripe.IsSuspended()) {
                    SuspendedDataWeight += suspendableStripe.GetStatistics().DataWeight;
                } else {
                    Register(stripeIndex);
                }
            }
            YCHECK(ExtractedLists.erase(cookie) == 1);
        } else {
            ReplayCookies.erase(replayIt);
            YCHECK(LostCookies.insert(cookie).second);
            if (extractedStripeList->UnavailableStripeCount > 0) {
                ++UnavailableLostCookieCount;
            }
        }
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TUnorderedChunkPool);

////////////////////////////////////////////////////////////////////////////////

void TUnorderedChunkPoolOptions::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Mode);
    Persist(context, JobSizeAdjusterConfig);
    Persist(context, JobSizeConstraints);
    Persist(context, MinTeleportChunkSize);
    Persist(context, MinTeleportChunkDataWeight);
    Persist(context, SliceErasureChunksByParts);
    Persist(context, OperationId);
    Persist(context, Task);
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IChunkPool> CreateUnorderedChunkPool(
    const TUnorderedChunkPoolOptions& options,
    TInputStreamDirectory directory)
{
    return std::make_unique<TUnorderedChunkPool>(
        options,
        std::move(directory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
