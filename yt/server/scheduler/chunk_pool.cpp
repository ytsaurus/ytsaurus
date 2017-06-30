#include "chunk_pool.h"
#include "helpers.h"
#include "job_size_adjuster.h"
#include "private.h"

#include <yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/misc/numeric_helpers.h>

namespace NYT {
namespace NScheduler {

using namespace NNodeTrackerClient;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NProto;

using NTableClient::NProto::TPartitionsExt;
using NChunkClient::NProto::TMiscExt;

////////////////////////////////////////////////////////////////////////////////

void TChunkStripeStatistics::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, ChunkCount);
    Persist(context, DataSize);
    Persist(context, RowCount);
    Persist(context, MaxBlockSize);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void AddStripeToList(
    const TChunkStripePtr& stripe,
    i64 stripeDataSize,
    i64 stripeRowCount,
    const TChunkStripeListPtr& list,
    TNodeId nodeId = InvalidNodeId)
{
    list->Stripes.push_back(stripe);
    list->TotalDataSize += stripeDataSize;
    list->TotalRowCount += stripeRowCount;
    list->TotalChunkCount += stripe->GetChunkCount();
    if (nodeId == InvalidNodeId) {
        return;
    }
    for (const auto& dataSlice : stripe->DataSlices) {
        for (const auto& chunkSlice : dataSlice->ChunkSlices) {
            bool isLocal = false;
            for (auto replica : chunkSlice->GetInputChunk()->GetReplicaList()) {
                if (replica.GetNodeId() == nodeId) {
                    i64 locality = chunkSlice->GetLocality(replica.GetReplicaIndex());
                    if (locality > 0) {
                        list->LocalDataSize += locality;
                        isLocal = true;
                    }
                }
            }

            if (isLocal) {
                ++list->LocalChunkCount;
            }
        }
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TChunkStripe::TChunkStripe(bool foreign, bool solid)
    : Foreign(foreign)
    , Solid(solid)
{ }

TChunkStripe::TChunkStripe(TInputDataSlicePtr dataSlice, bool foreign)
    : Foreign(foreign)
{
    DataSlices.emplace_back(std::move(dataSlice));
}

TChunkStripeStatistics TChunkStripe::GetStatistics() const
{
    TChunkStripeStatistics result;

    for (const auto& dataSlice : DataSlices) {
        result.DataSize += dataSlice->GetDataSize();
        result.RowCount += dataSlice->GetRowCount();
        ++result.ChunkCount;
        result.MaxBlockSize = std::max(result.MaxBlockSize, dataSlice->GetMaxBlockSize());
    }

    return result;
}

int TChunkStripe::GetChunkCount() const
{
    int result = 0;
    for (const auto& dataSlice : DataSlices) {
        result += dataSlice->GetChunkCount();
    }
    return result;
}

int TChunkStripe::GetTableIndex() const
{
    YCHECK(!DataSlices.empty());
    YCHECK(!DataSlices.front()->ChunkSlices.empty());
    return DataSlices.front()->ChunkSlices.front()->GetInputChunk()->GetTableIndex();
}

int TChunkStripe::GetInputStreamIndex() const
{
    YCHECK(!DataSlices.empty());
    return DataSlices.front()->InputStreamIndex;
}

void TChunkStripe::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, DataSlices);
    Persist(context, WaitingChunkCount);
    Persist(context, Foreign);
    Persist(context, Solid);
}

TChunkStripeStatistics operator + (
    const TChunkStripeStatistics& lhs,
    const TChunkStripeStatistics& rhs)
{
    TChunkStripeStatistics result;
    result.ChunkCount = lhs.ChunkCount + rhs.ChunkCount;
    result.DataSize = lhs.DataSize + rhs.DataSize;
    result.RowCount = lhs.RowCount + rhs.RowCount;
    result.MaxBlockSize = std::max(lhs.MaxBlockSize, rhs.MaxBlockSize);
    return result;
}

TChunkStripeStatistics& operator += (
    TChunkStripeStatistics& lhs,
    const TChunkStripeStatistics& rhs)
{
    lhs.ChunkCount += rhs.ChunkCount;
    lhs.DataSize += rhs.DataSize;
    lhs.RowCount += rhs.RowCount;
    lhs.MaxBlockSize = std::max(lhs.MaxBlockSize, rhs.MaxBlockSize);
    return lhs;
}

TChunkStripeStatisticsVector AggregateStatistics(
    const TChunkStripeStatisticsVector& statistics)
{
    TChunkStripeStatistics sum;
    for (const auto& stat : statistics) {
        sum += stat;
    }
    return TChunkStripeStatisticsVector(1, sum);
}

////////////////////////////////////////////////////////////////////////////////

TChunkStripeList::TChunkStripeList(int stripeCount)
    : Stripes(stripeCount)
{ }

TChunkStripeStatisticsVector TChunkStripeList::GetStatistics() const
{
    TChunkStripeStatisticsVector result;
    result.reserve(Stripes.size());
    for (const auto& stripe : Stripes) {
        result.push_back(stripe->GetStatistics());
    }
    return result;
}

TChunkStripeStatistics TChunkStripeList::GetAggregateStatistics() const
{
    TChunkStripeStatistics result;
    result.ChunkCount = TotalChunkCount;
    if (IsApproximate) {
        result.RowCount = TotalRowCount * ApproximateSizesBoostFactor;
        result.DataSize = TotalDataSize * ApproximateSizesBoostFactor;
    } else {
        result.RowCount = TotalRowCount;
        result.DataSize = TotalDataSize;
    }
    return result;
}

void TChunkStripeList::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Stripes);
    Persist(context, PartitionTag);
    Persist(context, IsApproximate);
    Persist(context, TotalDataSize);
    Persist(context, LocalDataSize);
    Persist(context, TotalRowCount);
    Persist(context, TotalChunkCount);
    Persist(context, LocalChunkCount);
}

////////////////////////////////////////////////////////////////////////////////

bool TInputTable::IsForeign() const
{
    return Path.GetForeign();
}

bool TInputTable::IsPrimary() const
{
    return !IsForeign();
}

void TInputTable::Persist(const TPersistenceContext& context)
{
    TUserObject::Persist(context);

    using NYT::Persist;
    Persist(context, ChunkCount);
    Persist(context, Chunks);
    Persist(context, Schema);
    Persist(context, SchemaMode);
    Persist(context, IsDynamic);
}

////////////////////////////////////////////////////////////////////////////////

void TChunkPoolInputBase::Finish()
{
    Finished = true;
}

void TChunkPoolInputBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Finished);
}

////////////////////////////////////////////////////////////////////////////////

TSuspendableStripe::TSuspendableStripe()
    : ExtractedCookie_(IChunkPoolOutput::NullCookie)
{ }

TSuspendableStripe::TSuspendableStripe(TChunkStripePtr stripe)
    : ExtractedCookie_(IChunkPoolOutput::NullCookie)
    , Stripe_(std::move(stripe))
    , OriginalStripe_(Stripe_)
    , Statistics_(Stripe_->GetStatistics())
{ }

const TChunkStripePtr& TSuspendableStripe::GetStripe() const
{
    return Stripe_;
}

const TChunkStripeStatistics& TSuspendableStripe::GetStatistics() const
{
    return Statistics_;
}

void TSuspendableStripe::Suspend()
{
    YCHECK(Stripe_);
    YCHECK(!Suspended_);

    Suspended_ = true;
}

bool TSuspendableStripe::IsSuspended() const
{
    return Suspended_;
}

void TSuspendableStripe::Resume(TChunkStripePtr stripe)
{
    YCHECK(Stripe_);
    YCHECK(Suspended_);

    // NB: do not update statistics on resume to preserve counters.
    Suspended_ = false;
    Stripe_ = stripe;
}

yhash<TInputChunkPtr, TInputChunkPtr> TSuspendableStripe::ResumeAndBuildChunkMapping(TChunkStripePtr stripe)
{
    YCHECK(Stripe_);
    YCHECK(Suspended_);

    yhash<TInputChunkPtr, TInputChunkPtr> mapping;

    // Our goal is to restore the correspondence between the old data slices and new data slices
    // in order to be able to substitute old references to input chunks in newly created jobs with current
    // ones.

    auto addToMapping = [&mapping] (const TInputDataSlicePtr& originalDataSlice, const TInputDataSlicePtr& newDataSlice) {
        YCHECK(!newDataSlice || originalDataSlice->ChunkSlices.size() == newDataSlice->ChunkSlices.size());
        for (int index = 0; index < originalDataSlice->ChunkSlices.size(); ++index) {
            mapping[originalDataSlice->ChunkSlices[index]->GetInputChunk()] = newDataSlice
                ? newDataSlice->ChunkSlices[index]->GetInputChunk()
                : nullptr;
        }
    };

    yhash<i64, TInputDataSlicePtr> tagToDataSlice;

    for (const auto& dataSlice : stripe->DataSlices) {
        YCHECK(dataSlice->Tag);
        YCHECK(tagToDataSlice.insert(std::make_pair(*dataSlice->Tag, dataSlice)).second);
    }

    for (int index = 0; index < OriginalStripe_->DataSlices.size(); ++index) {
        const auto& originalSlice = OriginalStripe_->DataSlices[index];
        auto it = tagToDataSlice.find(*originalSlice->Tag);
        if (it != tagToDataSlice.end()) {
            const auto& newSlice = it->second;
            if (originalSlice->Type == EDataSourceType::UnversionedTable) {
                const auto& originalChunk = originalSlice->GetSingleUnversionedChunkOrThrow();
                const auto& newChunk = newSlice->GetSingleUnversionedChunkOrThrow();
                if (*originalChunk->BoundaryKeys() != *newChunk->BoundaryKeys()) {
                    THROW_ERROR_EXCEPTION("Corresponding chunks in original and new stripes have different boundary keys")
                        << TErrorAttribute("data_slice_tag", *originalSlice->Tag)
                        << TErrorAttribute("original_chunk_id", originalChunk->ChunkId())
                        << TErrorAttribute("original_boundary_keys", *originalChunk->BoundaryKeys())
                        << TErrorAttribute("new_chunk_id", newChunk->ChunkId())
                        << TErrorAttribute("new_boundary_keys", *newChunk->BoundaryKeys());
                    break;
                }
                if (originalChunk->GetRowCount() != newChunk->GetRowCount()) {
                    THROW_ERROR_EXCEPTION("Corresponding chunks in original and new stripes have different row counts")
                        << TErrorAttribute("data_slice_tag", *originalSlice->Tag)
                        << TErrorAttribute("original_chunk_id", originalChunk->ChunkId())
                        << TErrorAttribute("original_row_count", originalChunk->GetRowCount())
                        << TErrorAttribute("new_chunk_id", newChunk->ChunkId())
                        << TErrorAttribute("new_row_count", newChunk->GetRowCount());
                }
            }
            addToMapping(originalSlice, newSlice);
            tagToDataSlice.erase(it);
        } else {
            addToMapping(originalSlice, nullptr);
        }
    }

    if (!tagToDataSlice.empty()) {
        THROW_ERROR_EXCEPTION("New stripe has extra data slices")
            << TErrorAttribute("extra_data_slice_tag", tagToDataSlice.begin()->first);
    }

    // NB: do not update statistics on resume to preserve counters.
    Suspended_ = false;
    Stripe_ = stripe;

    return mapping;
}

void TSuspendableStripe::ReplaceOriginalStripe()
{
    OriginalStripe_ = Stripe_;
}

void TSuspendableStripe::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, ExtractedCookie_);
    Persist(context, Stripe_);
    Persist(context, OriginalStripe_);
    Persist(context, Teleport_);
    Persist(context, Suspended_);
    Persist(context, Statistics_);
}

////////////////////////////////////////////////////////////////////////////////


TChunkPoolOutputBase::TChunkPoolOutputBase()
    : DataSizeCounter(0)
    , RowCounter(0)
{ }

// IChunkPoolOutput implementation.

i64 TChunkPoolOutputBase::GetTotalDataSize() const
{
    return DataSizeCounter.GetTotal();
}

i64 TChunkPoolOutputBase::GetRunningDataSize() const
{
    return DataSizeCounter.GetRunning();
}

i64 TChunkPoolOutputBase::GetCompletedDataSize() const
{
    return DataSizeCounter.GetCompletedTotal();
}

i64 TChunkPoolOutputBase::GetPendingDataSize() const
{
    return DataSizeCounter.GetPending();
}

i64 TChunkPoolOutputBase::GetTotalRowCount() const
{
    return RowCounter.GetTotal();
}

const TProgressCounter& TChunkPoolOutputBase::GetJobCounter() const
{
    return JobCounter;
}

// IPersistent implementation.

void TChunkPoolOutputBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, DataSizeCounter);
    Persist(context, RowCounter);
    Persist(context, JobCounter);
}

const std::vector<TInputChunkPtr>& TChunkPoolOutputBase::GetTeleportChunks() const
{
    return TeleportChunks_;
}

////////////////////////////////////////////////////////////////////////////////

class TAtomicChunkPool
    : public TChunkPoolInputBase
    , public TChunkPoolOutputBase
    , public IChunkPool
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    TAtomicChunkPool()
    {
        JobCounter.Set(1);
    }

    // IChunkPoolInput implementation.

    virtual IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        YCHECK(!Finished);
        YCHECK(!ExtractedList);

        HasPrimaryStripes = HasPrimaryStripes || !stripe->Foreign;

        auto cookie = static_cast<int>(Stripes.size());

        TSuspendableStripe suspendableStripe(stripe);
        Stripes.push_back(suspendableStripe);

        DataSizeCounter.Increment(suspendableStripe.GetStatistics().DataSize);
        RowCounter.Increment(suspendableStripe.GetStatistics().RowCount);

        return cookie;
    }

    virtual void Finish() override
    {
        if (Finished) {
            JobCounter.Increment(1);
        } else {
            TChunkPoolInputBase::Finish();
        }
    }

    virtual void Suspend(IChunkPoolInput::TCookie cookie) override
    {
        ++SuspendedStripeCount;
        auto& suspendableStripe = Stripes[cookie];
        suspendableStripe.Suspend();
    }

    virtual void Resume(IChunkPoolInput::TCookie cookie, TChunkStripePtr stripe) override
    {
        auto& suspendableStripe = Stripes[cookie];
        suspendableStripe.Resume(stripe);
        --SuspendedStripeCount;
        YCHECK(SuspendedStripeCount >= 0);
    }

    // IChunkPoolOutput implementation.

    virtual TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override
    {
        TChunkStripeStatisticsVector result;
        result.reserve(Stripes.size());
        for (const auto& suspendableStripe : Stripes) {
            auto stripe = suspendableStripe.GetStripe();
            result.push_back(stripe->GetStatistics());
        }
        return result;
    }

    virtual bool IsCompleted() const override
    {
        return
            Finished &&
            GetPendingJobCount() == 0 &&
            SuspendedStripeCount == 0 &&
            JobCounter.GetRunning() == 0;
    }

    virtual int GetTotalJobCount() const override
    {
        return Finished && HasPrimaryStripes && DataSizeCounter.GetTotal() > 0 ? 1 : 0;
    }

    virtual int GetPendingJobCount() const override
    {
        return
            Finished &&
            SuspendedStripeCount == 0 &&
            DataSizeCounter.GetPending() > 0 &&
            HasPrimaryStripes
            ? 1 : 0;
    }

    virtual i64 GetLocality(TNodeId nodeId) const override
    {
        // Pretend we are local to work around locality timeout.
        return 1;
    }

    virtual IChunkPoolOutput::TCookie Extract(TNodeId nodeId) override
    {
        YCHECK(Finished);
        YCHECK(SuspendedStripeCount == 0);

        if (GetPendingJobCount() == 0) {
            return IChunkPoolOutput::NullCookie;
        }

        ExtractedList = New<TChunkStripeList>();
        for (const auto& suspendableStripe : Stripes) {
            auto stripe = suspendableStripe.GetStripe();
            auto stat = stripe->GetStatistics();
            AddStripeToList(
                stripe,
                stat.DataSize,
                stat.RowCount,
                ExtractedList,
                nodeId);
        }

        JobCounter.Start(1);
        DataSizeCounter.Start(DataSizeCounter.GetTotal());
        RowCounter.Start(RowCounter.GetTotal());

        return 0;
    }

    virtual TChunkStripeListPtr GetStripeList(IChunkPoolOutput::TCookie cookie) override
    {
        YCHECK(cookie == 0);
        YCHECK(ExtractedList);
        YCHECK(Finished);

        return ExtractedList;
    }

    virtual void Completed(IChunkPoolOutput::TCookie cookie, const TCompletedJobSummary& jobSummary) override
    {
        YCHECK(cookie == 0);
        YCHECK(ExtractedList);
        YCHECK(Finished);

        JobCounter.Completed(1, jobSummary.InterruptReason);
        DataSizeCounter.Completed(DataSizeCounter.GetTotal());
        RowCounter.Completed(RowCounter.GetTotal());

        ExtractedList = nullptr;
    }

    virtual void Failed(IChunkPoolOutput::TCookie cookie) override
    {
        YCHECK(cookie == 0);
        YCHECK(ExtractedList);
        YCHECK(Finished);

        JobCounter.Failed(1);
        DataSizeCounter.Failed(DataSizeCounter.GetTotal());
        RowCounter.Failed(RowCounter.GetTotal());

        ExtractedList = nullptr;
    }

    virtual void Aborted(IChunkPoolOutput::TCookie cookie) override
    {
        YCHECK(cookie == 0);
        YCHECK(ExtractedList);
        YCHECK(Finished);

        JobCounter.Aborted(1);
        DataSizeCounter.Aborted(DataSizeCounter.GetTotal());
        RowCounter.Aborted(RowCounter.GetTotal());

        ExtractedList = nullptr;
    }

    virtual void Lost(IChunkPoolOutput::TCookie cookie) override
    {
        YCHECK(cookie == 0);
        YCHECK(!ExtractedList);
        YCHECK(Finished);

        JobCounter.Lost(1);
        DataSizeCounter.Lost(DataSizeCounter.GetTotal());
        RowCounter.Lost(RowCounter.GetTotal());
    }

    // IPersistent implementation.

    virtual void Persist(const TPersistenceContext& context) override
    {
        TChunkPoolInputBase::Persist(context);
        TChunkPoolOutputBase::Persist(context);

        using NYT::Persist;
        Persist(context, Stripes);
        Persist(context, ExtractedList);
        Persist(context, SuspendedStripeCount);
        Persist(context, HasPrimaryStripes);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TAtomicChunkPool, 0x76bac510);

    std::vector<TSuspendableStripe> Stripes;
    TChunkStripeListPtr ExtractedList;
    int SuspendedStripeCount = 0;
    bool HasPrimaryStripes = false;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TAtomicChunkPool);

std::unique_ptr<IChunkPool> CreateAtomicChunkPool()
{
    return std::unique_ptr<IChunkPool>(new TAtomicChunkPool());
}

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

    virtual void Aborted(IChunkPoolOutput::TCookie cookie) override
    {
        const auto& extractedStripeList = GetExtractedStripeList(cookie);
        const auto& list = extractedStripeList->StripeList;

        JobCounter.Aborted(1);
        DataSizeCounter.Aborted(list->TotalDataSize);
        RowCounter.Aborted(list->TotalRowCount);

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

DEFINE_ENUM(EShuffleChunkPoolRunState,
    (Initializing)
    (Pending)
    (Running)
    (Completed)
);

class TShuffleChunkPool
    : public TChunkPoolInputBase
    , public IShuffleChunkPool
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    //! For persistence only.
    TShuffleChunkPool()
        : DataSizeThreshold(-1)
    { }

    TShuffleChunkPool(
        int partitionCount,
        i64 dataSizeThreshold)
        : DataSizeThreshold(dataSizeThreshold)
    {
        Outputs.resize(partitionCount);
        for (int index = 0; index < partitionCount; ++index) {
            Outputs[index].reset(new TOutput(this, index));
        }
    }

    // IShuffleChunkPool implementation.

    virtual IChunkPoolInput* GetInput() override
    {
        return this;
    }

    virtual IChunkPoolOutput* GetOutput(int partitionIndex) override
    {
        return Outputs[partitionIndex].get();
    }

    // IChunkPoolInput implementation.

    virtual IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        YCHECK(!Finished);

        auto cookie = static_cast<int>(InputStripes.size());

        TInputStripe inputStripe;
        inputStripe.ElementaryIndexBegin = static_cast<int>(ElementaryStripes.size());

        for (const auto& dataSlice : stripe->DataSlices) {
            // NB: TShuffleChunkPool contains only chunks from unversioned tables.
            const auto& chunkSpec = dataSlice->GetSingleUnversionedChunkOrThrow();

            int elementaryIndex = static_cast<int>(ElementaryStripes.size());
            auto elementaryStripe = New<TChunkStripe>(dataSlice);
            ElementaryStripes.push_back(elementaryStripe);

            const auto* partitionsExt = chunkSpec->PartitionsExt().get();
            YCHECK(partitionsExt);
            YCHECK(partitionsExt->row_counts_size() == Outputs.size());
            YCHECK(partitionsExt->uncompressed_data_sizes_size() == Outputs.size());

            for (int index = 0; index < static_cast<int>(Outputs.size()); ++index) {
                YCHECK(partitionsExt->row_counts(index) <= RowCountThreshold);
                Outputs[index]->AddStripe(
                    elementaryIndex,
                    partitionsExt->uncompressed_data_sizes(index),
                    partitionsExt->row_counts(index));
            }

            chunkSpec->ReleaseBoundaryKeys();
            chunkSpec->ReleasePartitionsExt();
        }

        inputStripe.ElementaryIndexEnd = static_cast<int>(ElementaryStripes.size());
        InputStripes.push_back(inputStripe);

        return cookie;
    }

    virtual void Suspend(IChunkPoolInput::TCookie cookie) override
    {
        const auto& inputStripe = InputStripes[cookie];
        for (int index = inputStripe.ElementaryIndexBegin; index < inputStripe.ElementaryIndexEnd; ++index) {
            for (const auto& output : Outputs) {
                output->SuspendStripe(index);
            }
        }
    }

    virtual void Resume(IChunkPoolInput::TCookie cookie, TChunkStripePtr stripe) override
    {
        // Remove all partition extensions.
        for (const auto& dataSlice : stripe->DataSlices) {
            // NB: TShuffleChunkPool contains only chunks from unversioned tables.
            const auto& chunkSpec = dataSlice->GetSingleUnversionedChunkOrThrow();
            chunkSpec->ReleaseBoundaryKeys();
            chunkSpec->ReleasePartitionsExt();
        }

        // Although the sizes and even the row count may have changed (mind unordered reader and
        // possible undetermined mappers in partition jobs), we ignore it and use counter values
        // from the initial stripes, hoping that nobody will recognize it. This may lead to
        // incorrect memory consumption estimates but significant bias is very unlikely.
        const auto& inputStripe = InputStripes[cookie];
        int stripeCount = inputStripe.ElementaryIndexEnd - inputStripe.ElementaryIndexBegin;
        int limit = std::min(static_cast<int>(stripe->DataSlices.size()), stripeCount - 1);

        // Fill the initial range of elementary stripes with new chunks (one per stripe).
        for (int index = 0; index < limit; ++index) {
            auto dataSlice = stripe->DataSlices[index];
            int elementaryIndex = index + inputStripe.ElementaryIndexBegin;
            ElementaryStripes[elementaryIndex] = New<TChunkStripe>(dataSlice);
        }

        // Cleanup the rest of elementary stripes.
        for (int elementaryIndex = inputStripe.ElementaryIndexBegin + limit;
            elementaryIndex < inputStripe.ElementaryIndexEnd;
            ++elementaryIndex)
        {
            ElementaryStripes[elementaryIndex] = New<TChunkStripe>();
        }

        // Put remaining chunks (if any) into the last stripe.
        auto& lastElementaryStripe = ElementaryStripes[inputStripe.ElementaryIndexBegin + limit];
        for (int index = limit; index < static_cast<int>(stripe->DataSlices.size()); ++index) {
            auto dataSlice = stripe->DataSlices[index];
            lastElementaryStripe->DataSlices.push_back(dataSlice);
        }

        for (int elementaryIndex = inputStripe.ElementaryIndexBegin;
            elementaryIndex < inputStripe.ElementaryIndexEnd;
            ++elementaryIndex)
        {
            for (const auto& output : Outputs) {
                output->ResumeStripe(elementaryIndex);
            }
        }
    }

    virtual void Finish() override
    {
        if (Finished)
            return;

        TChunkPoolInputBase::Finish();

        for (const auto& output : Outputs) {
            output->FinishInput();
        }
    }

    // IPersistent implementation.

    virtual void Persist(const TPersistenceContext& context) override
    {
        TChunkPoolInputBase::Persist(context);

        using NYT::Persist;
        Persist(context, DataSizeThreshold);
        Persist(context, Outputs);
        Persist(context, InputStripes);
        Persist(context, ElementaryStripes);
    }

private:
    using ERunState = EShuffleChunkPoolRunState;

    DECLARE_DYNAMIC_PHOENIX_TYPE(TShuffleChunkPool, 0xbacd518a);

    // NB: sort job cannot handle more than numeric_limits<i32>::max() rows.
    static const i64 RowCountThreshold = std::numeric_limits<i32>::max();

    i64 DataSizeThreshold;

    class TOutput
        : public TChunkPoolOutputBase
        , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
    {
    public:
        //! For persistence only.
        TOutput()
            : Owner(nullptr)
            , PartitionIndex(-1)
        { }

        explicit TOutput(
            TShuffleChunkPool* owner,
            int partitionIndex)
            : Owner(owner)
            , PartitionIndex(partitionIndex)
        {
            AddNewRun();
        }

        void AddStripe(int elementaryIndex, i64 dataSize, i64 rowCount)
        {
            auto* run = &Runs.back();
            if (run->TotalDataSize > 0) {
                if (run->TotalDataSize + dataSize > Owner->DataSizeThreshold ||
                    run->TotalRowCount + rowCount > Owner->RowCountThreshold)
                {
                    SealLastRun();
                    AddNewRun();
                    run = &Runs.back();
                }
            }

            YCHECK(elementaryIndex == run->ElementaryIndexEnd);
            run->ElementaryIndexEnd = elementaryIndex + 1;
            run->TotalDataSize += dataSize;
            run->TotalRowCount += rowCount;

            DataSizeCounter.Increment(dataSize);
            RowCounter.Increment(rowCount);
        }

        void SuspendStripe(int elementaryIndex)
        {
            auto* run = FindRun(elementaryIndex);
            if (run) {
                run->IsApproximate = true;
                ++run->SuspendCount;
                UpdatePendingRunSet(*run);
            }
        }

        void ResumeStripe(int elementaryIndex)
        {
            auto* run = FindRun(elementaryIndex);
            if (run) {
                --run->SuspendCount;
                YCHECK(run->SuspendCount >= 0);
                UpdatePendingRunSet(*run);
            }
        }

        void FinishInput()
        {
            auto& lastRun = Runs.back();
            if (lastRun.TotalDataSize > 0) {
                SealLastRun();
            } else {
                Runs.pop_back();
            }
        }

        // IChunkPoolOutput implementation.

        virtual TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override
        {
            YCHECK(!Runs.empty());
            YCHECK(GetPendingJobCount() > 0);

            TChunkStripeStatisticsVector result(1);

            // This is the next run to be given by #Extract.
            auto it = PendingRuns.begin();
            auto cookie = *it;
            auto& run = Runs[cookie];

            auto& stat = result.front();

            // NB: cannot estimate MaxBlockSize here.
            stat.ChunkCount = run.ElementaryIndexEnd - run.ElementaryIndexBegin;
            stat.DataSize = run.TotalDataSize;
            stat.RowCount = run.TotalRowCount;

            if (run.IsApproximate) {
                stat.DataSize *= ApproximateSizesBoostFactor;
                stat.RowCount *= ApproximateSizesBoostFactor;
            }

            return result;
        }

        virtual bool IsCompleted() const override
        {
            return
                Owner->Finished &&
                JobCounter.GetCompletedTotal() == Runs.size();
        }

        virtual int GetTotalJobCount() const override
        {
            int result = static_cast<int>(Runs.size());
            // Handle empty last run properly.
            if (!Runs.empty() && Runs.back().TotalDataSize == 0) {
                --result;
            }
            return result;
        }

        virtual int GetPendingJobCount() const override
        {
            return static_cast<int>(PendingRuns.size());
        }

        virtual i64 GetLocality(TNodeId /*nodeId*/) const override
        {
            Y_UNREACHABLE();
        }

        virtual TCookie Extract(TNodeId /*nodeId*/) override
        {
            if (GetPendingJobCount() == 0) {
                return NullCookie;
            }

            auto it = PendingRuns.begin();
            auto cookie = *it;
            PendingRuns.erase(it);

            auto& run = Runs[cookie];
            YCHECK(run.State == ERunState::Pending);
            run.State = ERunState::Running;

            JobCounter.Start(1);
            DataSizeCounter.Start(run.TotalDataSize);
            RowCounter.Start(run.TotalRowCount);

            return cookie;
        }

        virtual TChunkStripeListPtr GetStripeList(TCookie cookie) override
        {
            const auto& run = Runs[cookie];

            auto list = New<TChunkStripeList>();
            list->PartitionTag = PartitionIndex;
            list->Stripes.reserve(run.ElementaryIndexEnd - run.ElementaryIndexBegin);

            for (int index = run.ElementaryIndexBegin; index < run.ElementaryIndexEnd; ++index) {
                list->Stripes.emplace_back(Owner->ElementaryStripes[index]);
            }

            // NB: never ever make TotalDataSize and TotalBoostFactor approximate.
            // Otherwise sort data size and row counters will be severely corrupted
            list->TotalDataSize = run.TotalDataSize;
            list->TotalRowCount = run.TotalRowCount;
            // In shuffle chunk pools almost all stripes consist of a single data slice consisting of a single chunk.
            // Sometimes during resume some elementary stripes may get more than one data slice.
            // Still we don't care, since this counter is just for diagnostic and much cheaper.
            list->TotalChunkCount = run.ElementaryIndexEnd - run.ElementaryIndexBegin;

            list->IsApproximate = run.IsApproximate;

            return list;
        }

        virtual void Completed(TCookie cookie, const TCompletedJobSummary& /* jobSummary */) override
        {
            auto& run = Runs[cookie];
            YCHECK(run.State == ERunState::Running);
            run.State = ERunState::Completed;

            JobCounter.Completed(1);
            DataSizeCounter.Completed(run.TotalDataSize);
            RowCounter.Completed(run.TotalRowCount);
        }

        virtual void Failed(TCookie cookie) override
        {
            auto& run = Runs[cookie];
            YCHECK(run.State == ERunState::Running);
            run.State = ERunState::Pending;

            UpdatePendingRunSet(run);

            JobCounter.Failed(1);
            DataSizeCounter.Failed(run.TotalDataSize);
            RowCounter.Failed(run.TotalRowCount);
        }

        virtual void Aborted(TCookie cookie) override
        {
            auto& run = Runs[cookie];
            YCHECK(run.State == ERunState::Running);
            run.State = ERunState::Pending;

            UpdatePendingRunSet(run);

            JobCounter.Aborted(1);
            DataSizeCounter.Aborted(run.TotalDataSize);
            RowCounter.Aborted(run.TotalRowCount);
        }

        virtual void Lost(TCookie cookie) override
        {
            auto& run = Runs[cookie];
            YCHECK(run.State == ERunState::Completed);
            run.State = ERunState::Pending;

            UpdatePendingRunSet(run);

            JobCounter.Lost(1);
            DataSizeCounter.Lost(run.TotalDataSize);
            RowCounter.Lost(run.TotalRowCount);
        }

        // IPersistent implementation.

        virtual void Persist(const TPersistenceContext& context) override
        {
            TChunkPoolOutputBase::Persist(context);

            using NYT::Persist;
            Persist(context, Owner);
            Persist(context, PartitionIndex);
            Persist(context, Runs);
            Persist(context, PendingRuns);
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TShuffleChunkPool::TOutput, 0xba17acf7);

        friend class TShuffleChunkPool;

        TShuffleChunkPool* Owner;
        int PartitionIndex;

        struct TRun
        {
            int ElementaryIndexBegin = 0;
            int ElementaryIndexEnd = 0;
            i64 TotalDataSize = 0;
            i64 TotalRowCount = 0;
            int SuspendCount = 0;
            ERunState State = ERunState::Initializing;
            bool IsApproximate = false;

            void Persist(const TPersistenceContext& context)
            {
                using NYT::Persist;
                Persist(context, ElementaryIndexBegin);
                Persist(context, ElementaryIndexEnd);
                Persist(context, TotalDataSize);
                Persist(context, TotalRowCount);
                Persist(context, SuspendCount);
                Persist(context, State);
                Persist(context, IsApproximate);
            }
        };

        std::vector<TRun> Runs;
        yhash_set<TCookie> PendingRuns;


        void UpdatePendingRunSet(const TRun& run)
        {
            TCookie cookie = &run - Runs.data();
            if (run.State == ERunState::Pending && run.SuspendCount == 0) {
                PendingRuns.insert(cookie);
            } else {
                PendingRuns.erase(cookie);
            }
        }

        void AddNewRun()
        {
            TRun run;
            run.ElementaryIndexBegin = Runs.empty() ? 0 : Runs.back().ElementaryIndexEnd;
            run.ElementaryIndexEnd = run.ElementaryIndexBegin;
            Runs.push_back(run);
        }

        TRun* FindRun(int elementaryIndex)
        {
            if (Runs.empty() || elementaryIndex >= Runs.back().ElementaryIndexEnd) {
                return nullptr;
            }

            int lo = 0;
            int hi = static_cast<int>(Runs.size());
            while (lo + 1 < hi) {
                int mid = (lo + hi) / 2;
                const auto& run = Runs[mid];
                if (run.ElementaryIndexBegin <= elementaryIndex) {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }

            auto& run = Runs[lo];
            YCHECK(run.ElementaryIndexBegin <= elementaryIndex && run.ElementaryIndexEnd > elementaryIndex);
            return &run;
        }

        void SealLastRun()
        {
            auto& run = Runs.back();
            YCHECK(run.TotalDataSize > 0);
            YCHECK(run.State == ERunState::Initializing);
            run.State = ERunState::Pending;
            UpdatePendingRunSet(run);
        }
    };

    std::vector<std::unique_ptr<TOutput>> Outputs;

    struct TInputStripe
    {
        int ElementaryIndexBegin;
        int ElementaryIndexEnd;

        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, ElementaryIndexBegin);
            Persist(context, ElementaryIndexEnd);
        }
    };

    std::vector<TInputStripe> InputStripes;
    std::vector<TChunkStripePtr> ElementaryStripes;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TShuffleChunkPool);
DEFINE_DYNAMIC_PHOENIX_TYPE(TShuffleChunkPool::TOutput);

std::unique_ptr<IShuffleChunkPool> CreateShuffleChunkPool(
    int partitionCount,
    i64 dataSizeThreshold)
{
    return std::unique_ptr<IShuffleChunkPool>(new TShuffleChunkPool(
        partitionCount,
        dataSizeThreshold));
}

////////////////////////////////////////////////////////////////////////////////

TInputStreamDescriptor::TInputStreamDescriptor(bool isTeleportable, bool isPrimary, bool isVersioned)
    : IsTeleportable_(isTeleportable)
    , IsPrimary_(isPrimary)
    , IsVersioned_(isVersioned)
{ }

bool TInputStreamDescriptor::IsTeleportable() const
{
    return IsTeleportable_;
}

bool TInputStreamDescriptor::IsForeign() const
{
    return !IsPrimary_;
}

bool TInputStreamDescriptor::IsPrimary() const
{
    return IsPrimary_;
}

bool TInputStreamDescriptor::IsVersioned() const
{
    return IsVersioned_;
}

bool TInputStreamDescriptor::IsUnversioned() const
{
    return !IsVersioned_;
}

void TInputStreamDescriptor::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, IsTeleportable_);
    Persist(context, IsPrimary_);
    Persist(context, IsVersioned_);
}

////////////////////////////////////////////////////////////////////////////////

TInputStreamDescriptor IntermediateInputStreamDescriptor(false /* isTeleportable */, true /* isPrimary */, false /* isVersioned */);

////////////////////////////////////////////////////////////////////////////////

TInputStreamDirectory::TInputStreamDirectory(
    std::vector<TInputStreamDescriptor> descriptors,
    TInputStreamDescriptor defaultDescriptor)
    : Descriptors_(std::move(descriptors))
    , DefaultDescriptor_(defaultDescriptor)
{
    YCHECK(DefaultDescriptor_.IsPrimary());
}

const TInputStreamDescriptor& TInputStreamDirectory::GetDescriptor(int inputStreamIndex) const
{
    if (0 <= inputStreamIndex && inputStreamIndex < static_cast<int>(Descriptors_.size())) {
        return Descriptors_[inputStreamIndex];
    } else {
        return DefaultDescriptor_;
    }
}

int TInputStreamDirectory::GetDescriptorCount() const
{
    return Descriptors_.size();
}

void TInputStreamDirectory::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Descriptors_);
    Persist(context, DefaultDescriptor_);
}

////////////////////////////////////////////////////////////////////////////////

TInputStreamDirectory IntermediateInputStreamDirectory({}, IntermediateInputStreamDescriptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
