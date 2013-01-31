#include "stdafx.h"
#include "chunk_pool.h"

#include <ytlib/misc/id_generator.h>

#include <ytlib/table_client/helpers.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

namespace NYT {
namespace NScheduler {

using namespace NChunkServer;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////

TChunkStripe::TChunkStripe()
{ }

TChunkStripe::TChunkStripe(TRefCountedInputChunkPtr inputChunk)
{
    Chunks.push_back(inputChunk);
}

TChunkStripe::TChunkStripe(const TChunkStripe& other)
{
    FOREACH (const auto& chunk, other.Chunks) {
        Chunks.push_back(New<TRefCountedInputChunk>(*chunk));
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
        : Stripe(std::move(stripe))
        , Suspended(false)
    {
        GetStatistics(Stripe, &DataSize, &RowCount);
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
        GetStatistics(stripe, &stripeDataSize, &stripeRowCount);

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

    virtual i64 GetRunningDataSize() const override
    {
        return DataSizeCounter.GetRunning();
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

    virtual bool IsCompleted() const override
    {
        return Finished && GetCompletedDataSize() == GetTotalDataSize();
    }

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
            auto stripe = suspendableStripe.GetStripe();
            i64 stripeDataSize;
            i64 stripeRowCount;
            GetStatistics(stripe, &stripeDataSize, &stripeRowCount);
            AddStripeToList(stripe, stripeDataSize, stripeRowCount, ExtractedList, address);
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
    TUnorderedChunkPool(int jobCount)
        : JobCounter(jobCount)
    { }

    // IChunkPoolInput implementation.

    virtual IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        YCHECK(!Finished);

        auto cookie = IChunkPoolInput::TCookie(Stripes.size());

        TSuspendableStripe suspendableStripe(stripe);
        Stripes.push_back(suspendableStripe);

        DataSizeCounter.Increment(suspendableStripe.GetDataSize());
        RowCounter.Increment(suspendableStripe.GetRowCount());
        
        Register(stripe);

        return cookie;
    }

    virtual int GetTotalStripeCount() const override
    {
        return static_cast<int>(Stripes.size());
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

    virtual bool IsCompleted() const override
    {
        return Finished && GetCompletedDataSize() == GetTotalDataSize();
    }

    virtual int GetTotalJobCount() const override
    {
        return IsCompleted() ? JobCounter.GetCompleted() : JobCounter.GetTotal();
    }

    virtual int GetPendingJobCount() const override
    {
        // NB: Pending data size can be zero while JobCounter indicates
        // that some jobs are pending. This may happen due to unevenness
        // of workload partitioning and cause the task to start less jobs than
        // suggested.
        return GetPendingDataSize() == 0 ? 0 : JobCounter.GetPending();
    }

    virtual i64 GetLocality(const Stroka& address) const override
    {
        auto it = LocalChunks.find(address);
        return it == LocalChunks.end() ? 0 : it->second.TotalDataSize;
    }

    virtual IChunkPoolOutput::TCookie Extract(const Stroka& address) override
    {
        YCHECK(Finished);

        TChunkStripeListPtr list;
        IChunkPoolOutput::TCookie cookie;

        if (LostCookies.empty()) {
            if (GetPendingDataSize() == 0) {
                return IChunkPoolOutput::NullCookie;
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
                    address);
            }

            // Take non-local chunks.
            AddAndUnregisterStripes(
                list,
                GlobalChunks.begin(),
                GlobalChunks.end(),
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

        FOREACH (const auto& stripe, list->Stripes) {
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

    std::vector<TSuspendableStripe> Stripes;

    yhash_set<TChunkStripePtr> GlobalChunks;

    struct TLocalityEntry
    {
        TLocalityEntry()
            : TotalDataSize(0)
        { }

        i64 TotalDataSize;
        yhash_set<TChunkStripePtr> Stripes;
    };
    
    yhash_map<Stroka, TLocalityEntry> LocalChunks;

    TIdGenerator<IChunkPoolOutput::TCookie> OutputCookieGenerator;

    yhash_map<IChunkPoolOutput::TCookie, TChunkStripeListPtr> ExtractedLists;

    std::vector<IChunkPoolOutput::TCookie> LostCookies;


    void Register(TChunkStripePtr stripe)
    {
        FOREACH (const auto& chunk, stripe->Chunks) {
            i64 chunkDataSize;
            GetStatistics(*chunk, &chunkDataSize);
            FOREACH (const auto& address, chunk->node_addresses()) {
                auto& entry = LocalChunks[address];
                YCHECK(entry.Stripes.insert(stripe).second);
                entry.TotalDataSize += chunkDataSize;
            }
        }

        YCHECK(GlobalChunks.insert(stripe).second);
    }

    void Unregister(TChunkStripePtr stripe)
    {
        FOREACH (const auto& chunk, stripe->Chunks) {
            i64 chunkDataSize;
            GetStatistics(*chunk, &chunkDataSize);
            FOREACH (const auto& address, chunk->node_addresses()) {
                auto& entry = LocalChunks[address];
                YCHECK(entry.Stripes.erase(stripe) == 1);
                entry.TotalDataSize -= chunkDataSize;
            }
        }

        YCHECK(GlobalChunks.erase(stripe) == 1);
    }

    template <class TIterator>
    void AddAndUnregisterStripes(
        TChunkStripeListPtr list,
        const TIterator& begin,
        const TIterator& end,
        const Stroka& address)
    {
        i64 idealDataSizePerJob = std::max(static_cast<i64>(1), GetPendingDataSize() / GetPendingJobCount());

        size_t oldSize = list->Stripes.size();
        for (auto it = begin; it != end && list->TotalDataSize < idealDataSizePerJob; ++it) {
            const auto& stripe = *it;
            
            i64 stripeDataSize;
            i64 stripeRowCount;
            GetStatistics(stripe, &stripeDataSize, &stripeRowCount);

            AddStripeToList(stripe, stripeDataSize, stripeRowCount, list, address);
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

TAutoPtr<IChunkPool> CreateUnorderedChunkPool(int jobCount)
{
    return new TUnorderedChunkPool(jobCount);
}

////////////////////////////////////////////////////////////////////

class TShuffleChunkPool
    : public TChunkPoolInputBase
    , public IShuffleChunkPool
{
public:
    explicit TShuffleChunkPool(const std::vector<i64>& dataSizeThresholds)
    {
        Outputs.resize(dataSizeThresholds.size());
        for (int index = 0; index < static_cast<int>(dataSizeThresholds.size()); ++index) {
            Outputs[index] = new TOutput(this, index, dataSizeThresholds[index]);
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

        auto cookie = static_cast<int>(InputStripes.size());

        TInputStripe inputStripe;
        inputStripe.ElementaryIndexBegin = static_cast<int>(ElementaryStripes.size());
       
        FOREACH (const auto& chunk, stripe->Chunks) {
            int elementaryIndex = static_cast<int>(ElementaryStripes.size());
            auto elementaryStripe = New<TChunkStripe>(chunk);
            ElementaryStripes.push_back(TSuspendableStripe(elementaryStripe));

            auto partitionsExt = GetProtoExtension<NTableClient::NProto::TPartitionsExt>(chunk->extensions());
            YCHECK(partitionsExt.partitions_size() == Outputs.size());

            for (int index = 0; index < static_cast<int>(Outputs.size()); ++index) {
                const auto& partitionAttributes = partitionsExt.partitions(index);
                Outputs[index]->AddStripe(
                    elementaryIndex,
                    partitionAttributes.uncompressed_data_size(),
                    partitionAttributes.row_count());
            }

            RemoveProtoExtension<NTableClient::NProto::TPartitionsExt>(chunk->mutable_extensions());
        }

        inputStripe.ElementaryIndexEnd = static_cast<int>(ElementaryStripes.size());
        InputStripes.push_back(inputStripe);

        return cookie;
    }

    virtual int GetTotalStripeCount() const override
    {
        return static_cast<int>(InputStripes.size());
    }

    virtual void Suspend(IChunkPoolInput::TCookie cookie) override
    {
        const auto& inputStripe = InputStripes[cookie];
        for (int index = inputStripe.ElementaryIndexBegin; index < inputStripe.ElementaryIndexEnd; ++index) {
            ElementaryStripes[index].Suspend();
            FOREACH (const auto& output, Outputs) {
                output->SuspendStripe(index);
            }
        }
    }

    virtual bool Resume(IChunkPoolInput::TCookie cookie, TChunkStripePtr stripe) override
    {
        YCHECK(!Finished);

        const auto& inputStripe = InputStripes[cookie];
        if (stripe->Chunks.size() != inputStripe.ElementaryIndexEnd - inputStripe.ElementaryIndexBegin) {
            return false;
        }

        for (int index = inputStripe.ElementaryIndexBegin; index < inputStripe.ElementaryIndexEnd; ++index) {
            auto elementaryStripe = New<TChunkStripe>(stripe->Chunks[index - inputStripe.ElementaryIndexBegin]);
            if (!ElementaryStripes[index].Resume(elementaryStripe)) {
                return false;
            }
            FOREACH (const auto& output, Outputs) {
                output->ResumeStripe(index);
            }
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
    class TOutput
        : public TChunkPoolOutputBase
    {
    public:
        explicit TOutput(
            TShuffleChunkPool* owner,
            int partitionIndex,
            i64 dataSizeThreshold)
            : Owner(owner)
            , PartitionIndex(partitionIndex)
            , DataSizeThreshold(dataSizeThreshold)
        {
            NewRun();
        }

        struct TStripeInfo
        {
            TStripeInfo()
                : DataSize(0)
                , RowCount(0)
            { }

            i64 DataSize;
            i64 RowCount;
        };

        void AddStripe(int elementaryIndex, i64 dataSize, i64 rowCount)
        {
            auto* run = &Runs.back();
            if (run->ElementaryIndexBegin != run->ElementaryIndexEnd &&
                run->TotalDataSize + dataSize > DataSizeThreshold)
            {
                FinishLastRun();
                NewRun();
                run = &Runs.back();
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
            auto& run = GetRun(elementaryIndex);
            ++run.SuspendedCount;

            UpdatePendingRunSet(run);
        }

        void ResumeStripe(int elementaryIndex)
        {
            auto& run = GetRun(elementaryIndex);
            --run.SuspendedCount;
            YCHECK(run.SuspendedCount >= 0);

            UpdatePendingRunSet(run);
        }

        void FinishInput()
        {
            FinishLastRun();
        }

        // IChunkPoolOutput implementation.

        virtual bool IsCompleted() const override
        {
            return Owner->Finished && GetCompletedDataSize() == GetTotalDataSize();
        }

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
            YCHECK(run.SuspendedCount == 0);

            auto list = New<TChunkStripeList>();
            list->PartitionTag = PartitionIndex;
            for (int index = run.ElementaryIndexBegin; index < run.ElementaryIndexEnd; ++index) {
                auto stripe = Owner->ElementaryStripes[index].GetStripe();
                list->Stripes.push_back(stripe);
                list->TotalChunkCount += stripe->Chunks.size();
            }

            list->TotalDataSize = run.TotalDataSize;
            list->TotalRowCount = run.TotalRowCount;
            list->LocalChunkCount = 0;
            list->NonLocalChunkCount = list->TotalChunkCount;

            return list;
        }

        virtual void Completed(TCookie cookie) override
        {
            auto& run = Runs[cookie];
            YCHECK(run.State == ERunState::Running);
            run.State = ERunState::Completed;

            DataSizeCounter.Completed(run.TotalDataSize);
            RowCounter.Completed(run.TotalRowCount);
        }

        virtual void Failed(TCookie cookie) override
        {
            auto& run = Runs[cookie];
            YCHECK(run.State == ERunState::Running);
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
        int PartitionIndex;
        i64 DataSizeThreshold;

        DECLARE_ENUM(ERunState,
            (Initializing)
            (Pending)
            (Running)
            (Completed)
        );

        struct TRun
        {
            TRun()
                : ElementaryIndexBegin(0)
                , ElementaryIndexEnd(0)
                , TotalDataSize(0)
                , TotalRowCount(0)
                , SuspendedCount(0)
                , State(ERunState::Initializing)
            { }

            int ElementaryIndexBegin;
            int ElementaryIndexEnd;
            i64 TotalDataSize;
            i64 TotalRowCount;
            int SuspendedCount;
            ERunState State;
        };

        std::vector<TRun> Runs;
        yhash_set<TCookie> PendingRuns;


        void UpdatePendingRunSet(const TRun& run)
        {
            TCookie cookie = &run - Runs.data();
            if (run.State == ERunState::Pending && run.SuspendedCount == 0) {
                PendingRuns.insert(cookie);
            } else {
                PendingRuns.erase(cookie);
            }
        }
        
        void NewRun()
        {
            TRun run;
            run.ElementaryIndexBegin = Runs.empty() ? 0 : Runs.back().ElementaryIndexEnd;
            run.ElementaryIndexEnd = run.ElementaryIndexBegin;
            Runs.push_back(run);
        }

        TRun& GetRun(int elementaryIndex)
        {
            int runBegin = 0;
            int runEnd = static_cast<int>(Runs.size());
            while (runBegin < runEnd) {
                int runMid = (runBegin + runEnd) / 2;
                const auto& run = Runs[runMid];
                if (run.ElementaryIndexBegin <= elementaryIndex) {
                    runBegin = runMid;
                } else {
                    runEnd = runMid;
                }
            }

            auto& run = Runs[runBegin];
            YCHECK(run.ElementaryIndexBegin <= elementaryIndex && run.ElementaryIndexEnd > elementaryIndex);
            return run;
        }

        void FinishLastRun()
        {
            auto& run = Runs.back();
            YCHECK(run.State == ERunState::Initializing);
            run.State = ERunState::Pending;
            if (run.TotalDataSize > 0) {
                UpdatePendingRunSet(run);
            }
        }

    };

    // One should use TAutoPtr with care :)
    std::vector< TAutoPtr<TOutput> > Outputs;

    struct TInputStripe
    {
        int ElementaryIndexBegin;
        int ElementaryIndexEnd;
    };

    std::vector<TInputStripe> InputStripes;
    std::vector<TSuspendableStripe> ElementaryStripes;
};

TAutoPtr<IShuffleChunkPool> CreateShuffleChunkPool(
    const std::vector<i64>& dataSizeThresholds)
{
    return new TShuffleChunkPool(dataSizeThresholds);
}

////////////////////////////////////////////////////////////////////

void GetStatistics(
    const TChunkStripePtr& stripe,
    i64* totalDataSize,
    i64* totalRowCount)
{
    if (totalDataSize) {
        *totalDataSize = 0;
    }
    if (totalRowCount) {
        *totalRowCount = 0;
    }

    FOREACH (const auto& chunk, stripe->Chunks) {
        i64 chunkDataSize;
        i64 chunkRowCount;
        GetStatistics(*chunk, &chunkDataSize, &chunkRowCount);
        if (totalDataSize) {
            *totalDataSize += chunkDataSize;
        }
        if (totalRowCount) {
            *totalRowCount += chunkRowCount;
        }
    }
}

void AddStripeToList(
    const TChunkStripePtr& stripe,
    i64 stripeDataSize,
    i64 stripeRowCount,
    const TChunkStripeListPtr& list,
    const TNullable<Stroka>& address)
{
    list->Stripes.push_back(stripe);
    list->TotalDataSize += stripeDataSize;
    list->TotalRowCount += stripeRowCount;

    list->TotalChunkCount += stripe->Chunks.size();
    if (address) {
        FOREACH (const auto& chunk, stripe->Chunks) {
            const auto& chunkAddresses = chunk->node_addresses();
            if (std::find_if(
                chunkAddresses.begin(),
                chunkAddresses.end(),
                [&] (const Stroka& chunkAddress) { return address.Get() == chunkAddress; })
                != chunkAddresses.end())
            {
                ++list->LocalChunkCount;
            } else {
                ++list->NonLocalChunkCount;
            }
        }
    } else {
        list->NonLocalChunkCount += stripe->Chunks.size();
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

