#include "stdafx.h"
#include "chunk_pool.h"

#include <ytlib/misc/id_generator.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/table_client/key.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

namespace NYT {
namespace NScheduler {

using namespace NChunkServer;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////

TChunkStripe::TChunkStripe()
{ }

TChunkStripe::TChunkStripe(TRefCountedInputChunkPtr inputChunk)
{
    Chunks.push_back(inputChunk);
}

void TChunkStripe::GetStatistics(i64* totalDataSize, i64* totalRowCount)
{
    if (totalDataSize) {
        *totalDataSize = 0;
    }
    if (totalRowCount) {
        *totalRowCount = 0;
    }
    FOREACH (auto chunk, Chunks) {
        i64 dataSize, rowCount;
        chunk->GetStatistics(&dataSize, &rowCount);
        *totalDataSize += dataSize;
        *totalRowCount += rowCount;
    }
}

////////////////////////////////////////////////////////////////////

TChunkStripeList::TChunkStripeList()
    : TotalDataSize(0)
    , TotalRowCount(0)
    , TotalChunkCount(0)
    , LocalChunkCount(0)
    , NonLocalChunkCount(0)
{ }

bool TChunkStripeList::TryAddStripe(
    TChunkStripePtr stripe,
    const TNullable<Stroka>& address,
    i64 dataSizeThreshold)
{
    i64 stripeDataSize;
    i64 stripeRowCount;
    stripe->GetStatistics(&stripeDataSize, &stripeRowCount);

    if (TotalChunkCount > 0 && TotalDataSize + stripeDataSize > dataSizeThreshold) {
        return false;
    }

    Stripes.push_back(stripe);
    
    TotalDataSize += stripeDataSize;
    TotalRowCount += stripeRowCount;

    TotalChunkCount += stripe->Chunks.size();
    if (address) {
        FOREACH (const auto& chunk, stripe->Chunks) {
            const auto& chunkAddresses = chunk->node_addresses();
            if (std::find_if(
                chunkAddresses.begin(),
                chunkAddresses.end(),
                [&] (const Stroka& chunkAddress) { return address.Get() == chunkAddress; })
                != chunkAddresses.end())
            {
                ++LocalChunkCount;
            } else {
                ++NonLocalChunkCount;
            }
        }
    } else {
        NonLocalChunkCount += stripe->Chunks.size();
    }

    return true;
}

////////////////////////////////////////////////////////////////////

class TChunkPoolInputBase
    : public virtual IChunkPoolInput
{
public:
    TChunkPoolInputBase()
        : Finished(false)
    { }

    // IChunkPoolInput implementation.

    virtual void Finish() override
    {
        YCHECK(!Finished);
        Finished = true;
    }

protected:
    bool Finished;

};

////////////////////////////////////////////////////////////////////

class TSuspendableStripe
{
public:
    TSuspendableStripe()
        : Suspended(false)
        , DataSize(0)
        , RowCount(0)
    { }

    explicit TSuspendableStripe(TChunkStripePtr stripe)
        : Suspended(false)
        , Stripe(stripe)
    {
        stripe->GetStatistics(&DataSize, &RowCount);
    }

    i64 GetDataSize() const
    {
        return DataSize;
    }

    i64 GetRowCount() const
    {
        return RowCount;
    }

    TChunkStripePtr GetStripe() const
    {
        return Stripe;
    }

    void Suspend()
    {
        YCHECK(Stripe);
        YCHECK(!Suspended);
        Suspended = true;
    }

    bool Resume(TChunkStripePtr stripe)
    {
        YCHECK(Stripe);
        YCHECK(Suspended);

        i64 stripeDataSize;
        i64 stripeRowCount;
        stripe->GetStatistics(&stripeDataSize, &stripeRowCount);

        if (DataSize != stripeDataSize || RowCount != stripeRowCount) {
            return false;
        }

        Suspended = false;
        Stripe = stripe;
        return true;
    }
    
private:
    TChunkStripePtr Stripe;
    bool Suspended;
    i64 DataSize;
    i64 RowCount;

};

////////////////////////////////////////////////////////////////////

class TChunkPoolOutputBase
    : public virtual IChunkPoolOutput
{
public:
    TChunkPoolOutputBase()
        : DataSizeCounter(0)
        , RowCounter(0)
    { }

    virtual i64 GetTotalDataSize() const override
    {
        return DataSizeCounter.GetTotal();
    }

    virtual i64 GetCompletedDataSize() const override
    {
        return DataSizeCounter.GetCompleted();
    }

    virtual i64 GetPendingDataSize() const override
    {
        return DataSizeCounter.GetPending();
    }

    virtual i64 GetTotalRowCount() const override
    {
        return RowCounter.GetTotal();
    }

    virtual bool IsCompleted() const override
    {
        return GetCompletedDataSize() == GetTotalDataSize();
    }

protected:
    TProgressCounter DataSizeCounter;
    TProgressCounter RowCounter;

};

////////////////////////////////////////////////////////////////////

class TAtomicChunkPool
    : public TChunkPoolInputBase
    , public TChunkPoolOutputBase
    , public IChunkPool
{
public:
    // IChunkPoolInput implementation.

    virtual IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        YCHECK(!Finished);

        auto cookie = static_cast<int>(Stripes.size());

        TSuspendableStripe suspendableStripe(stripe);
        Stripes.push_back(suspendableStripe);

        DataSizeCounter.Increment(suspendableStripe.GetDataSize());
        RowCounter.Increment(suspendableStripe.GetRowCount());

        FOREACH (const auto& chunk, stripe->Chunks) {
            FOREACH (const auto& address, chunk->node_addresses()) {
                AddressToLocality[address] += suspendableStripe.GetDataSize();
            }
        }

        return cookie;
    }

    virtual int GetTotalStripeCount() const override
    {
        return static_cast<int>(Stripes.size());
    }

    virtual void Suspend(IChunkPoolInput::TCookie cookie) override
    {
        Stripes[cookie].Suspend();
    }

    virtual bool Resume(IChunkPoolInput::TCookie cookie, TChunkStripePtr stripe) override
    {
        return Stripes[cookie].Resume(stripe);
    }

    // IChunkPoolOutput implementation.

    virtual int GetTotalJobCount() const override
    {
        return 1;
    }

    virtual int GetPendingJobCount() const override
    {
        return Finished && !ExtractedList ? 1 : 0;
    }

    virtual i64 GetLocality(const Stroka& address) const override
    {
        if (ExtractedList) {
            return 0;
        }

        auto it = AddressToLocality.find(address);
        return it == AddressToLocality.end() ? 0 : it->second;
    }

    virtual IChunkPoolOutput::TCookie Extract(const Stroka& address) override
    {
        YCHECK(Finished);
        YCHECK(!ExtractedList);

        ExtractedList = New<TChunkStripeList>();
        FOREACH (const auto& suspendableStripe, Stripes) {
            YCHECK(ExtractedList->TryAddStripe(suspendableStripe.GetStripe(), address));
        }

        DataSizeCounter.Start(ExtractedList->TotalDataSize);
        RowCounter.Start(ExtractedList->TotalRowCount);

        return 0;
    }

    virtual TChunkStripeListPtr GetStripeList(IChunkPoolOutput::TCookie cookie) override
    {
        YCHECK(cookie == 0);
        YCHECK(ExtractedList);
        YCHECK(Finished);

        return ExtractedList;
    }
    
    virtual void Completed(IChunkPoolOutput::TCookie cookie) override
    {
        YCHECK(cookie == 0);
        YCHECK(ExtractedList);
        YCHECK(Finished);

        DataSizeCounter.Completed(ExtractedList->TotalDataSize);
        RowCounter.Completed(ExtractedList->TotalRowCount);
    }

    virtual void Failed(IChunkPoolOutput::TCookie cookie) override
    {
        YCHECK(cookie == 0);
        YCHECK(ExtractedList);
        YCHECK(Finished);

        DataSizeCounter.Failed(ExtractedList->TotalDataSize);
        RowCounter.Failed(ExtractedList->TotalRowCount);

        ExtractedList = NULL;
    }

    virtual void Lost(IChunkPoolOutput::TCookie cookie) override
    {
        YCHECK(cookie == 0);
        YCHECK(ExtractedList);
        YCHECK(Finished);

        DataSizeCounter.Lost(ExtractedList->TotalDataSize);
        RowCounter.Lost(ExtractedList->TotalRowCount);

        ExtractedList = NULL;
    }

private:
    std::vector<TSuspendableStripe> Stripes;

    //! Addresses of added chunks.
    yhash_map<Stroka, i64> AddressToLocality;

    //! Instance returned from #Extract.
    TChunkStripeListPtr ExtractedList;

};

TAutoPtr<IChunkPool> CreateAtomicChunkPool()
{
    return new TAtomicChunkPool();
}

////////////////////////////////////////////////////////////////////

class TUnorderedChunkPool
    : public TChunkPoolInputBase
    , public TChunkPoolOutputBase
    , public IChunkPool
{
public:
    TUnorderedChunkPool(
        int jobCount,
        i64 dataSizeThreshold)
        : DataSizeThreshold(dataSizeThreshold)
        , StripeCount(0)
    {
        JobCounter.Set(jobCount);
    }

    // IChunkPoolInput implementation.

    virtual IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        YCHECK(!Finished);

        i64 stripeDataSize;
        i64 stripeRowCount;
        stripe->GetStatistics(&stripeDataSize, &stripeRowCount);

        DataSizeCounter.Increment(stripeDataSize);
        RowCounter.Increment(stripeRowCount);
        ++StripeCount;

        Register(stripe);

        return InputCookieGenerator.Next();
    }

    virtual int GetTotalStripeCount() const override
    {
        return StripeCount;
    }

    virtual void Suspend(IChunkPoolInput::TCookie cookie) override
    {
        UNUSED(cookie);
        YUNREACHABLE();
    }

    virtual bool Resume(IChunkPoolInput::TCookie cookie, TChunkStripePtr stripe) override
    {
        UNUSED(cookie);
        UNUSED(stripe);
        YUNREACHABLE();
    }

    // IChunkPoolOutput implementation.

    virtual int GetTotalJobCount() const override
    {
        return JobCounter.GetTotal();
    }

    virtual int GetPendingJobCount() const override
    {
        return JobCounter.GetPending();
    }

    virtual i64 GetLocality(const Stroka& address) const override
    {
        auto it = LocalChunks.find(address);
        return it == LocalChunks.end() ? 0 : it->second.TotalDataSize;
    }

    virtual IChunkPoolOutput::TCookie Extract(const Stroka& address) override
    {
        TChunkStripeListPtr list;
        IChunkPoolOutput::TCookie cookie;

        if (LostCookies.empty()) {
            i64 dataSizeThreshold = DataSizeThreshold;
            if (JobCounter.GetPending() > 0) {
                i64 dataSizePerJob = static_cast<i64>(std::ceil((double) DataSizeCounter.GetPending() / JobCounter.GetPending()));
                dataSizeThreshold = std::min(dataSizeThreshold, dataSizePerJob);
            }

            cookie = OutputCookieGenerator.Next();
            list = New<TChunkStripeList>();
            YCHECK(ExtractedLists.insert(std::make_pair(cookie, list)).second);

            // Take local chunks first.
            auto it = LocalChunks.find(address);
            if (it != LocalChunks.end()) {
                const auto& entry = it->second;
                AddAndUnregisterStripes(
                    list,
                    entry.Stripes.begin(),
                    entry.Stripes.end(),
                    dataSizeThreshold,
                    address);
            }

            // Take remote chunks.
            AddAndUnregisterStripes(
                list,
                GlobalChunks.begin(),
                GlobalChunks.end(),
                dataSizeThreshold,
                address);
        } else {
            cookie = LostCookies.back();
            LostCookies.pop_back();
            list = GetStripeList(cookie);
        }

        JobCounter.Start(1);
        DataSizeCounter.Start(list->TotalDataSize);
        RowCounter.Start(list->TotalRowCount);

        return cookie;
    }

    virtual TChunkStripeListPtr GetStripeList(IChunkPoolOutput::TCookie cookie) override
    {
        auto it = ExtractedLists.find(cookie);
        YCHECK(it != ExtractedLists.end());
        return it->second;
    }

    virtual void Failed(IChunkPoolOutput::TCookie cookie) override
    {
        auto list = GetStripeListAndEvict(cookie);

        JobCounter.Failed(1);
        DataSizeCounter.Failed(list->TotalDataSize);
        RowCounter.Failed(list->TotalRowCount);

        FOREACH (auto stripe, list->Stripes) {
            Register(stripe);
        }
    }

    virtual void Completed(IChunkPoolOutput::TCookie cookie) override
    {
        auto list = GetStripeList(cookie);

        JobCounter.Completed(1);
        DataSizeCounter.Completed(list->TotalDataSize);
        RowCounter.Completed(list->TotalRowCount);
    }

    virtual void Lost(IChunkPoolOutput::TCookie cookie) override
    {
        // No need to respect locality for restarted jobs.
        auto list = GetStripeList(cookie);
        list->NonLocalChunkCount += list->LocalChunkCount;
        list->LocalChunkCount = 0;
        LostCookies.push_back(cookie);

        JobCounter.Lost(1);
        DataSizeCounter.Lost(list->TotalDataSize);
        RowCounter.Lost(list->TotalRowCount);
    }

private:
    TProgressCounter JobCounter;
    i64 DataSizeThreshold;
    int StripeCount;

    yhash_set<TChunkStripePtr> GlobalChunks;

    struct TChunkStripeHasher
    {
        size_t operator () (const TChunkStripePtr& stripe) const
        {
            size_t result = THash<TChunkStripe*>()(~stripe);
            if (!stripe->Chunks.empty()) {
                const auto& firstChunk = stripe->Chunks.front();
                result += 17 * firstChunk->slice().start_limit().row_index();
            }
            return result;
        }
    };

    struct TLocalityEntry
    {
        TLocalityEntry()
            : TotalDataSize(0)
        { }

        i64 TotalDataSize;
        yhash_set<TChunkStripePtr, TChunkStripeHasher> Stripes;
    };
    
    yhash_map<Stroka, TLocalityEntry> LocalChunks;

    TIdGenerator<IChunkPoolInput::TCookie> InputCookieGenerator;
    TIdGenerator<IChunkPoolOutput::TCookie> OutputCookieGenerator;

    yhash_map<IChunkPoolOutput::TCookie, TChunkStripeListPtr> ExtractedLists;

    std::vector<IChunkPoolOutput::TCookie> LostCookies;


    void Register(TChunkStripePtr stripe)
    {
        FOREACH (const auto& chunk, stripe->Chunks) {
            i64 dataSize, rowCount;
            chunk->GetStatistics(&dataSize, &rowCount);
            FOREACH (const auto& address, chunk->node_addresses()) {
                auto& entry = LocalChunks[address];
                YCHECK(entry.Stripes.insert(stripe).second);
                entry.TotalDataSize += dataSize;
            }
        }

        YCHECK(GlobalChunks.insert(stripe).second);
    }

    void Unregister(TChunkStripePtr stripe)
    {
        FOREACH (const auto& chunk, stripe->Chunks) {
            i64 dataSize, rowCount;
            chunk->GetStatistics(&dataSize, &rowCount);
            FOREACH (const auto& address, chunk->node_addresses()) {
                auto& entry = LocalChunks[address];
                YCHECK(entry.Stripes.erase(stripe) == 1);
                entry.TotalDataSize -= dataSize;
            }
        }

        YCHECK(GlobalChunks.erase(stripe) == 1);
    }

    template <class TIterator>
    void AddAndUnregisterStripes(
        TChunkStripeListPtr list,
        const TIterator& begin,
        const TIterator& end,
        i64 dataSizeThreshold,
        const Stroka& address)
    {
        size_t oldSize = list->Stripes.size();
        for (auto it = begin; it != end; ++it) {
            if (!list->TryAddStripe(*it, address, dataSizeThreshold)) {
                break;
            }
        }
        size_t newSize = list->Stripes.size();
        for (size_t index = oldSize; index < newSize; ++index) {
            Unregister(list->Stripes[index]);
        }
    }

    TChunkStripeListPtr GetStripeListAndEvict(IChunkPoolOutput::TCookie cookie)
    {
        auto it = ExtractedLists.find(cookie);
        YCHECK(it != ExtractedLists.end());
        auto list = it->second;
        ExtractedLists.erase(it);
        return list;
    }
};

TAutoPtr<IChunkPool> CreateUnorderedChunkPool(
    int jobCount,
    i64 dataSizeThreshold)
{
    return new TUnorderedChunkPool(
        jobCount,
        dataSizeThreshold);
}

////////////////////////////////////////////////////////////////////

class TShuffleChunkPool
    : public TChunkPoolInputBase
    , public IShuffleChunkPool
{
public:
    explicit TShuffleChunkPool(
        int partitionCount,
        i64 dataSizeThreshold)
        : DataSizeThreshold(dataSizeThreshold)
    {
        Outputs.resize(partitionCount);
        for (int index = 0; index < partitionCount; ++index) {
            Outputs[index] = new TOutput(this);
        }
    }

    // IShuffleChunkPool implementation.

    virtual IChunkPoolInput* GetInput() override
    {
        return this;
    }

    virtual IChunkPoolOutput* GetOutput(int partitionIndex) override
    {
        return ~Outputs[partitionIndex];
    }

    // IChunkPoolInput implementation.

    virtual IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        YCHECK(!Finished);

        auto cookie = static_cast<int>(Stripes.size());
        
        TSuspendableStripe suspendableStripe(stripe);
        Stripes.push_back(suspendableStripe);

        FOREACH (const auto& chunk, stripe->Chunks) {
            auto partitionsExt = GetProtoExtension<NTableClient::NProto::TPartitionsExt>(chunk->extensions());
            YCHECK(partitionsExt.partitions_size() == Outputs.size());

            for (int index = 0; index < partitionsExt.partitions_size(); ++index) {
                auto& output = Outputs[index];
                const auto& partitionAttributes = partitionsExt.partitions(index);
                output->AddData(
                    cookie,
                    partitionAttributes.uncompressed_data_size(),
                    partitionAttributes.row_count());
            }
        }

        return cookie;
    }

    virtual int GetTotalStripeCount() const override
    {
        return static_cast<int>(Stripes.size());
    }

    virtual void Suspend(IChunkPoolInput::TCookie cookie) override
    {
        Stripes[cookie].Suspend();

        FOREACH (const auto& output, Outputs) {
            output->SuspendStripe(cookie);
        }
    }

    virtual bool Resume(IChunkPoolInput::TCookie cookie, TChunkStripePtr stripe) override
    {
        YCHECK(!Finished);

        if (!Stripes[cookie].Resume(stripe)) {
            return false;
        }

        FOREACH (const auto& chunk, stripe->Chunks) {
            auto partitionsExt = GetProtoExtension<NTableClient::NProto::TPartitionsExt>(chunk->extensions());
            YCHECK(partitionsExt.partitions_size() == Outputs.size());

            for (int index = 0; index < partitionsExt.partitions_size(); ++index) {
                auto& output = Outputs[index];
                const auto& partitionAttributes = partitionsExt.partitions(index);
                output->AddData(
                    cookie,
                    partitionAttributes.uncompressed_data_size(),
                    partitionAttributes.row_count());
            }
        }

        FOREACH (const auto& output, Outputs) {
            output->ResumeStripe(cookie);
        }

        return true;
    }

    virtual void Finish() override
    {
        TChunkPoolInputBase::Finish();

        FOREACH (const auto& output, Outputs) {
            output->FinishInput();
        }
    }

private:
    i64 DataSizeThreshold;

    class TOutput
        : public TChunkPoolOutputBase
    {
    public:
        explicit TOutput(TShuffleChunkPool* owner)
            : Owner(owner)
        { }

        void AddData(int stripeIndex, i64 dataSize, i64 rowCount)
        {
            auto& run = GetRun(stripeIndex);
            run.TotalDataSize += dataSize;
            run.TotalRowCount += rowCount;

            DataSizeCounter.Increment(dataSize);
            RowCounter.Increment(rowCount);

            if (IsLastRun(run) && run.TotalDataSize >= Owner->DataSizeThreshold) {
                FinishRun(run);
                NewRun();
            }
        }

        void SuspendStripe(int stripeIndex)
        {
            auto& run = GetRun(stripeIndex);
            ++run.SuspendedStripeCount;

            UpdatePendingRunSet(run);
        }

        void ResumeStripe(int stripeIndex)
        {
            auto& run = GetRun(stripeIndex);
            --run.SuspendedStripeCount;
            YCHECK(run.SuspendedStripeCount >= 0);

            UpdatePendingRunSet(run);
        }

        void FinishInput()
        {
            FinishRun(Runs.back());
        }

        // IChunkPoolOutput implementation.

        virtual int GetTotalJobCount() const override
        {
            return static_cast<int>(Runs.size());
        }

        virtual int GetPendingJobCount() const override
        {
            return static_cast<int>(PendingRuns.size());
        }

        virtual i64 GetLocality(const Stroka& address) const override
        {
            UNUSED(address);
            YUNREACHABLE();
        }

        virtual TCookie Extract(const Stroka& address) override
        {
            if (PendingRuns.empty()) {
                return NullCookie;
            }

            auto it = PendingRuns.begin();
            auto cookie = *it;
            PendingRuns.erase(it);

            auto& run = Runs[cookie];
            YCHECK(run.State == ERunState::Pending);
            run.State = ERunState::Running;

            DataSizeCounter.Start(run.TotalDataSize);
            RowCounter.Start(run.TotalRowCount);

            return cookie;
        }

        virtual TChunkStripeListPtr GetStripeList(TCookie cookie) override
        {
            const auto& run = Runs[cookie];
            YCHECK(run.SuspendedStripeCount == 0);

            auto list = New<TChunkStripeList>();
            for (int index = run.StripeIndexBegin; index < run.StripeIndexEnd; ++index) {
                YCHECK(list->TryAddStripe(Owner->Stripes[index].GetStripe()));
            }

            list->TotalDataSize = run.TotalDataSize;
            list->TotalRowCount = run.TotalRowCount;

            return list;
        }

        virtual void Completed(TCookie cookie) override
        {
            auto& run = Runs[cookie];
            YCHECK(run.State == ERunState::Pending);
            run.State = ERunState::Completed;

            DataSizeCounter.Completed(run.TotalDataSize);
            RowCounter.Completed(run.TotalRowCount);
        }

        virtual void Failed(TCookie cookie) override
        {
            auto& run = Runs[cookie];
            YCHECK(run.State == ERunState::Pending);
            run.State = ERunState::Pending;

            UpdatePendingRunSet(run);

            DataSizeCounter.Failed(run.TotalDataSize);
            RowCounter.Failed(run.TotalRowCount);
        }

        virtual void Lost(TCookie cookie) override
        {
            auto& run = Runs[cookie];
            YCHECK(run.State == ERunState::Completed);
            run.State = ERunState::Pending;

            UpdatePendingRunSet(run);

            DataSizeCounter.Lost(run.TotalDataSize);
            RowCounter.Lost(run.TotalRowCount);
        }

    private:
        friend class TShuffleChunkPool;

        TShuffleChunkPool* Owner;

        DECLARE_ENUM(ERunState,
            (Initializing)
            (Pending)
            (Running)
            (Completed)
        );

        struct TRun
        {
            TRun()
                : StripeIndexBegin(0)
                , StripeIndexEnd(0)
                , TotalDataSize(0)
                , TotalRowCount(0)
                , SuspendedStripeCount(0)
                , State(ERunState::Initializing)
            { }

            int StripeIndexBegin;
            int StripeIndexEnd;
            i64 TotalDataSize;
            i64 TotalRowCount;
            int SuspendedStripeCount;
            ERunState State;
        };

        std::vector<TRun> Runs;
        yhash_set<TCookie> PendingRuns;


        void UpdatePendingRunSet(const TRun& run)
        {
            TCookie cookie = &run - Runs.data();
            if (run.State == ERunState::Pending && run.SuspendedStripeCount == 0) {
                PendingRuns.insert(cookie);
            } else {
                PendingRuns.erase(cookie);
            }
        }

        void NewRun()
        {
            Runs.push_back(TRun());
            auto& newRun = Runs.back();
            newRun.StripeIndexBegin = newRun.StripeIndexEnd = static_cast<int>(Owner->Stripes.size());
        }

        TRun& GetRun(int stripeIndex)
        {
            // Fast lane: first run ever or last run.
            if (Runs.empty()) {
                YCHECK(stripeIndex == 0);
                Runs.push_back(TRun());
                return Runs.front();
            }

            auto& lastRun = Runs.back();
            if (lastRun.StripeIndexBegin <= stripeIndex) {
                if (lastRun.StripeIndexEnd >= stripeIndex) {
                    YCHECK(lastRun.StripeIndexEnd == stripeIndex);
                    ++lastRun.StripeIndexEnd;
                }
                return lastRun;
            }

            // Slow lane: binary search.
            int runBegin = 0;
            int runEnd = static_cast<int>(Runs.size());
            while (runBegin < runEnd) {
                int runMid = (runBegin + runEnd) / 2;
                const auto& run = Runs[runMid];
                if (run.StripeIndexBegin <= stripeIndex) {
                    runBegin = runMid;
                } else {
                    runEnd = runMid;
                }
            }

            auto& run = Runs[runBegin];
            YCHECK(run.StripeIndexBegin <= stripeIndex && run.StripeIndexEnd > stripeIndex);
            return run;
        }

        bool IsLastRun(const TRun& run)
        {
            return &run - Runs.data() == Runs.size() - 1;
        }

        void FinishRun(TRun& run)
        {
            YCHECK(run.State == ERunState::Initializing);
            run.State = ERunState::Pending;
            UpdatePendingRunSet(run);
        }

    };

    // One should use TAutoPtr with care :)
    std::vector< TAutoPtr<TOutput> > Outputs;

    std::vector<TSuspendableStripe> Stripes;
};

TAutoPtr<IShuffleChunkPool> CreateShuffleChunkPool(
    int partitionCount,
    i64 dataSizeThreshold)
{
    return new TShuffleChunkPool(
        partitionCount,
        dataSizeThreshold);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

