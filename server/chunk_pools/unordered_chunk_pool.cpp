#include "unordered_chunk_pool.h"

#include "helpers.h"

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/server/controller_agent/job_size_adjuster.h>

#include <yt/core/misc/numeric_helpers.h>

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
    , public TChunkPoolOutputBase
    , public IChunkPool
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    //! For persistence only.
    TUnorderedChunkPool()
        : FreePendingDataSize(-1)
        , SuspendedDataSize(-1)
        , UnavailableLostCookieCount(-1)
        , MaxBlockSize(-1)
    { }

    TUnorderedChunkPool(
        IJobSizeConstraintsPtr jobSizeConstraints,
        TJobSizeAdjusterConfigPtr jobSizeAdjusterConfig)
        : JobSizeConstraints(std::move(jobSizeConstraints))
    {
        YCHECK(JobSizeConstraints);

        JobCounter.Set(JobSizeConstraints->GetJobCount());

        if (jobSizeAdjusterConfig && JobSizeConstraints->CanAdjustDataSizePerJob()) {
            JobSizeAdjuster = CreateJobSizeAdjuster(
                JobSizeConstraints->GetDataSizePerJob(),
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

        DataSizeCounter.Increment(suspendableStripe.GetStatistics().DataSize);
        RowCounter.Increment(suspendableStripe.GetStatistics().RowCount);
        MaxBlockSize = std::max(MaxBlockSize, suspendableStripe.GetStatistics().MaxBlockSize);

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
            SuspendedDataSize += suspendableStripe.GetStatistics().DataSize;
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
            SuspendedDataSize -= suspendableStripe.GetStatistics().DataSize;
            YCHECK(SuspendedDataSize >= 0);
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
            SuspendedDataSize == 0 &&
            PendingGlobalStripes.empty() &&
            JobCounter.GetRunning() == 0;
    }

    virtual int GetTotalJobCount() const override
    {
        return JobCounter.GetTotal();
    }

    virtual int GetPendingJobCount() const override
    {
        // TODO(babenko): refactor
        bool hasAvailableLostJobs = LostCookies.size() > UnavailableLostCookieCount;
        if (hasAvailableLostJobs) {
            return JobCounter.GetPending() - UnavailableLostCookieCount;
        }

        int freePendingJobCount = GetFreePendingJobCount();
        YCHECK(freePendingJobCount >= 0);
        YCHECK(!(FreePendingDataSize > 0 && freePendingJobCount == 0 && JobCounter.GetInterruptedTotal() == 0));

        if (freePendingJobCount == 0) {
            return 0;
        }

        if (FreePendingDataSize == 0) {
            return 0;
        }

        return freePendingJobCount;
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
        stat.DataSize = std::max(
            static_cast<i64>(1),
            GetPendingDataSize() / GetPendingJobCount());
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
        YCHECK(Finished);

        if (GetPendingJobCount() == 0) {
            return IChunkPoolOutput::NullCookie;
        }

        TChunkStripeListPtr list;
        IChunkPoolOutput::TCookie cookie;

        if (LostCookies.size() == UnavailableLostCookieCount) {
            auto extractedStripeList = CreateAndRegisterExtractedStripeList();
            list = extractedStripeList->StripeList;
            cookie = extractedStripeList->Cookie;

            i64 idealDataSizePerJob = GetIdealDataSizePerJob();

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
                        idealDataSizePerJob);
                }
            }

            // Take non-local chunks.
            AddAndUnregisterStripes(
                extractedStripeList,
                PendingGlobalStripes.begin(),
                PendingGlobalStripes.end(),
                nodeId,
                idealDataSizePerJob);

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

        JobCounter.Start(1);
        DataSizeCounter.Start(list->TotalDataSize);
        RowCounter.Start(list->TotalRowCount);

        UpdateJobCounter();

        return cookie;
    }

    TExtractedStripeListPtr GetExtractedStripeList(IChunkPoolOutput::TCookie cookie)
    {
        auto it = ExtractedLists.find(cookie);
        YCHECK(it != ExtractedLists.end());
        return it->second;
    }

    virtual TChunkStripeListPtr GetStripeList(IChunkPoolOutput::TCookie cookie) override
    {
        return GetExtractedStripeList(cookie)->StripeList;
    }

    virtual void Completed(IChunkPoolOutput::TCookie cookie, const TCompletedJobSummary& jobSummary) override
    {
        const auto& list = GetStripeList(cookie);

        JobCounter.Completed(1, jobSummary.InterruptReason);
        DataSizeCounter.Completed(list->TotalDataSize);
        RowCounter.Completed(list->TotalRowCount);

        //! If we don't have enough pending jobs - don't adjust data size per job.
        if (JobSizeAdjuster && JobCounter.GetPending() > JobCounter.GetRunning()) {
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

        JobCounter.Failed(1);
        DataSizeCounter.Failed(list->TotalDataSize);
        RowCounter.Failed(list->TotalRowCount);

        ReinstallStripeList(extractedStripeList, cookie);
    }

    virtual void Aborted(IChunkPoolOutput::TCookie cookie, EAbortReason reason) override
    {
        const auto& extractedStripeList = GetExtractedStripeList(cookie);
        const auto& list = extractedStripeList->StripeList;

        JobCounter.Aborted(1, reason);
        DataSizeCounter.Aborted(list->TotalDataSize, reason);
        RowCounter.Aborted(list->TotalRowCount, reason);

        ReinstallStripeList(extractedStripeList, cookie);
    }

    virtual void Lost(IChunkPoolOutput::TCookie cookie) override
    {
        const auto& extractedStripeList = GetExtractedStripeList(cookie);
        const auto& list = extractedStripeList->StripeList;

        // No need to respect locality for restarted jobs.
        list->LocalChunkCount = 0;
        list->LocalDataSize = 0;

        JobCounter.Lost(1);
        DataSizeCounter.Lost(list->TotalDataSize);
        RowCounter.Lost(list->TotalRowCount);

        YCHECK(LostCookies.insert(cookie).second);
        if (extractedStripeList->UnavailableStripeCount > 0) {
            ++UnavailableLostCookieCount;
        }
    }

    // IPersistent implementation.

    virtual void Persist(const TPersistenceContext& context) override
    {
        TChunkPoolInputBase::Persist(context);
        TChunkPoolOutputBase::Persist(context);

        using NYT::Persist;
        Persist(context, Stripes);
        Persist(context, JobSizeConstraints);
        Persist(context, JobSizeAdjuster);
        Persist(context, PendingGlobalStripes);
        Persist(context, FreePendingDataSize);
        Persist(context, SuspendedDataSize);
        Persist(context, UnavailableLostCookieCount);
        Persist(context, PendingStripeCount);
        Persist(context, MaxBlockSize);
        Persist(context, NodeIdToEntry);
        Persist(context, OutputCookieGenerator);
        Persist(context, ExtractedLists);
        Persist(context, LostCookies);
        Persist(context, ReplayCookies);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TUnorderedChunkPool, 0xbacd26ad);

    std::vector<TSuspendableStripe> Stripes;

    IJobSizeConstraintsPtr JobSizeConstraints;
    std::unique_ptr<IJobSizeAdjuster> JobSizeAdjuster;

    //! Indexes in #Stripes.
    yhash_set<int> PendingGlobalStripes;

    i64 FreePendingDataSize = 0;
    i64 SuspendedDataSize = 0;
    int UnavailableLostCookieCount = 0;
    i64 PendingStripeCount = 0;

    i64 MaxBlockSize = 0;

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

    int GetFreePendingJobCount() const
    {
        return JobCounter.GetPending() - LostCookies.size();
    }

    i64 GetIdealDataSizePerJob() const
    {
        int freePendingJobCount = GetFreePendingJobCount();
        YCHECK(freePendingJobCount > 0);
        return std::max(
            static_cast<i64>(1),
            DivCeil<i64>(FreePendingDataSize + SuspendedDataSize, freePendingJobCount));
    }

    void UpdateJobCounter()
    {
        i64 freePendingJobCount = GetFreePendingJobCount();
        if (Finished && FreePendingDataSize + SuspendedDataSize == 0 && freePendingJobCount > 0) {
            // Prune job count if all stripe lists are already extracted.
            JobCounter.Increment(-freePendingJobCount);
            return;
        }

        if (freePendingJobCount == 0 && FreePendingDataSize + SuspendedDataSize > 0) {
            // Happens when we hit MaxDataSlicesPerJob or MaxDataSizePerJob limit.
            JobCounter.Increment(1);
            return;
        }

        if (JobSizeConstraints->IsExplicitJobCount()) {
            return;
        }

        i64 dataSizePerJob = JobSizeAdjuster
                             ? JobSizeAdjuster->GetDataSizePerJob()
                             : JobSizeConstraints->GetDataSizePerJob();

        dataSizePerJob = std::min(dataSizePerJob, JobSizeConstraints->GetMaxDataSizePerJob());
        i64 newJobCount = DivCeil(FreePendingDataSize + SuspendedDataSize, dataSizePerJob);
        if (newJobCount != freePendingJobCount) {
            JobCounter.Increment(newJobCount - freePendingJobCount);
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

        FreePendingDataSize += suspendableStripe.GetStatistics().DataSize;
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
            stat.DataSize,
            stat.RowCount,
            extractedStripeList->StripeList);

        JobCounter.Increment(1);

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

        FreePendingDataSize -= suspendableStripe.GetStatistics().DataSize;
        YCHECK(PendingGlobalStripes.erase(stripeIndex) == 1);
    }

    template <class TIterator>
    void AddAndUnregisterStripes(
        const TExtractedStripeListPtr& extractedStripeList,
        const TIterator& begin,
        const TIterator& end,
        TNodeId nodeId,
        i64 idealDataSizePerJob)
    {
        auto& list = extractedStripeList->StripeList;
        size_t oldSize = list->Stripes.size();
        for (auto it = begin; it != end; ++it) {
            if (list->TotalDataSize >= idealDataSizePerJob) {
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

            // We should always return at least one stripe, even we get MaxDataSizePerJob overflow.
            if (list->TotalDataSize > 0 && list->TotalDataSize + stat.DataSize > JobSizeConstraints->GetMaxDataSizePerJob() &&
                (!JobSizeConstraints->IsExplicitJobCount() || GetFreePendingJobCount() > 1))
            {
                break;
            }

            // Leave enough stripes if job count is explicitly given.
            if (list->TotalDataSize > 0 && PendingStripeCount < GetFreePendingJobCount() && JobSizeConstraints->IsExplicitJobCount()) {
                break;
            }

            extractedStripeList->StripeIndexes.push_back(stripeIndex);
            --PendingStripeCount;

            suspendableStripe.SetExtractedCookie(extractedStripeList->Cookie);
            AddStripeToList(
                suspendableStripe.GetStripe(),
                stat.DataSize,
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
                    SuspendedDataSize += suspendableStripe.GetStatistics().DataSize;
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
    TJobSizeAdjusterConfigPtr jobSizeAdjusterConfig)
{
    return std::unique_ptr<IChunkPool>(new TUnorderedChunkPool(
        std::move(jobSizeConstraints),
        std::move(jobSizeAdjusterConfig)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT