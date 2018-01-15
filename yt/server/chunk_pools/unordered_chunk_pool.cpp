#include "unordered_chunk_pool.h"

#include "helpers.h"

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/server/controller_agent/job_size_adjuster.h>

#include <yt/core/misc/numeric_helpers.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYT {
namespace NChunkPools {

using namespace NControllerAgent;
using namespace NScheduler;
using namespace NNodeTrackerClient;

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
        IJobSizeConstraintsPtr jobSizeConstraints,
        TJobSizeAdjusterConfigPtr jobSizeAdjusterConfig,
        EUnorderedChunkPoolMode mode)
        : JobSizeConstraints(std::move(jobSizeConstraints))
        , Mode(mode)
    {
        YCHECK(JobSizeConstraints);

        if (Mode == EUnorderedChunkPoolMode::Normal) {
            JobCounter->Set(JobSizeConstraints->GetJobCount());
        } else {
            JobCounter->Set(0);
        }

        if (jobSizeAdjusterConfig && JobSizeConstraints->CanAdjustDataWeightPerJob()) {
            JobSizeAdjuster = CreateJobSizeAdjuster(
                JobSizeConstraints->GetDataWeightPerJob(),
                std::move(jobSizeAdjusterConfig));
            // ToDo(psushin): add logging here.
        }
    }

    // IChunkPoolInput implementation.

    virtual IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        // No check for finished here, because stripes may be added for interrupted jobs.

        auto cookie = Stripes.size();

        ++PendingStripeCount;
        TSuspendableStripe suspendableStripe(stripe);
        Stripes.push_back(suspendableStripe);

        DataWeightCounter->Increment(suspendableStripe.GetStatistics().DataWeight);
        RowCounter->Increment(suspendableStripe.GetStatistics().RowCount);
        MaxBlockSize = std::max(MaxBlockSize, suspendableStripe.GetStatistics().MaxBlockSize);

        TotalDataSliceCount += stripe->DataSlices.size();

        if (stripe->Solid) {
            AddSolid(cookie);
        } else {
            Register(cookie);
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

    virtual void Suspend(IChunkPoolInput::TCookie cookie) override
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

    virtual void Resume(IChunkPoolInput::TCookie cookie, TChunkStripePtr stripe) override
    {
        auto& suspendableStripe = Stripes[cookie];
        suspendableStripe.Resume(stripe);

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
        return GetExtractedStripeList(cookie)->StripeList;
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
        }

        //! If we don't have enough pending jobs - don't adjust data size per job.
        if (JobSizeAdjuster && JobCounter->GetPending() > JobCounter->GetRunning()) {
            JobSizeAdjuster->UpdateStatistics(jobSummary);
            UpdateJobCounter();
        }

        // NB: may fail.
        ReplayCookies.erase(cookie);
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
        Persist(context, Stripes);
        Persist(context, JobSizeConstraints);
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
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TUnorderedChunkPool, 0xbacd26ad);

    std::vector<TSuspendableStripe> Stripes;

    IJobSizeConstraintsPtr JobSizeConstraints;
    std::unique_ptr<IJobSizeAdjuster> JobSizeAdjuster;

    //! Indexes in #Stripes.
    yhash_set<int> PendingGlobalStripes;

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
        yhash_set<int> StripeIndexes;

        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, Locality);
            Persist(context, StripeIndexes);
        }
    };

    yhash<TNodeId, TLocalityEntry> NodeIdToEntry;

    TIdGenerator OutputCookieGenerator;

    yhash<IChunkPoolOutput::TCookie, TExtractedStripeListPtr> ExtractedLists;

    yhash_set<IChunkPoolOutput::TCookie> LostCookies;
    yhash_set<IChunkPoolOutput::TCookie> ReplayCookies;

    EUnorderedChunkPoolMode Mode;

    int GetFreePendingJobCount() const
    {
        return Mode == EUnorderedChunkPoolMode::AutoMerge ? 1 : JobCounter->GetPending() - LostCookies.size();
    }

    i64 GetIdealDataWeightPerJob() const
    {
        if (Mode == EUnorderedChunkPoolMode::AutoMerge) {
            return JobSizeConstraints->GetDataWeightPerJob();
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

        if (JobSizeConstraints->IsExplicitJobCount()) {
            return;
        }

        i64 dataWeightPerJob = JobSizeAdjuster
            ? JobSizeAdjuster->GetDataWeightPerJob()
            : JobSizeConstraints->GetDataWeightPerJob();

        dataWeightPerJob = std::min(dataWeightPerJob, JobSizeConstraints->GetMaxDataWeightPerJob());
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
            if (list->Stripes.size() >= JobSizeConstraints->GetMaxDataSlicesPerJob() &&
                (!JobSizeConstraints->IsExplicitJobCount() || GetFreePendingJobCount() > 1))
            {
                break;
            }

            auto stripeIndex = *it;
            auto& suspendableStripe = Stripes[stripeIndex];
            auto stat = suspendableStripe.GetStatistics();

            // We should always return at least one stripe, even we get MaxDataWeightPerJob overflow.
            if (list->TotalDataWeight > 0 && list->TotalDataWeight + stat.DataWeight >
                JobSizeConstraints->GetMaxDataWeightPerJob() &&
                (!JobSizeConstraints->IsExplicitJobCount() || GetFreePendingJobCount() > 1))
            {
                break;
            }

            // Leave enough stripes if job count is explicitly given.
            if (list->TotalDataWeight > 0 && PendingStripeCount < GetFreePendingJobCount() && JobSizeConstraints->IsExplicitJobCount()) {
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

std::unique_ptr<IChunkPool> CreateUnorderedChunkPool(
    IJobSizeConstraintsPtr jobSizeConstraints,
    TJobSizeAdjusterConfigPtr jobSizeAdjusterConfig,
    EUnorderedChunkPoolMode mode)
{
    return std::make_unique<TUnorderedChunkPool>(
        std::move(jobSizeConstraints),
        std::move(jobSizeAdjusterConfig),
        mode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
