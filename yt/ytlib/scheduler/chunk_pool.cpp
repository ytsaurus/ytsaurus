#include "stdafx.h"
#include "chunk_pool.h"

#include <ytlib/chunk_holder/chunk_meta_extensions.h>
#include <ytlib/table_client/key.h>

namespace NYT {
namespace NScheduler {

using namespace NChunkServer;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////

TWeightedChunk::TWeightedChunk()
    : DataSizeOverride(0)
    , RowCountOverride(0)
{ }

////////////////////////////////////////////////////////////////////

TChunkStripe::TChunkStripe()
{ }

TChunkStripe::TChunkStripe(TRefCountedInputChunkPtr inputChunk)
{
    AddChunk(inputChunk);
}

TChunkStripe::TChunkStripe(TRefCountedInputChunkPtr inputChunk, i64 dataSizeOverride, i64 rowCountOverride)
{
    AddChunk(
        inputChunk,
        dataSizeOverride,
        rowCountOverride);
}

void TChunkStripe::AddChunk(TRefCountedInputChunkPtr inputChunk)
{
    AddChunk(
        inputChunk,
        inputChunk->uncompressed_data_size(),
        inputChunk->row_count());
}

void TChunkStripe::AddChunk(TRefCountedInputChunkPtr inputChunk, i64 dataSizeOverride, i64 rowCountOverride)
{
    Chunks.push_back(TWeightedChunk());
    auto& weightedChunk = Chunks.back();

    weightedChunk.InputChunk = inputChunk;
    weightedChunk.DataSizeOverride = dataSizeOverride;
    weightedChunk.RowCountOverride = rowCountOverride;
}

std::vector<NChunkServer::TChunkId> TChunkStripe::GetChunkIds() const
{
    std::vector<NChunkServer::TChunkId> result;
    FOREACH (const auto& chunk, Chunks) {
        result.push_back(TChunkId::FromProto(chunk.InputChunk->slice().chunk_id()));
    }
    return result;
}

////////////////////////////////////////////////////////////////////

TPoolExtractionResult::TPoolExtractionResult()
    : TotalDataSize(0)
    , TotalChunkCount(0)
    , LocalChunkCount(0)
    , RemoteChunkCount(0)
{ }

void TPoolExtractionResult::AddStripe(TChunkStripePtr stripe, const Stroka& address)
{
    Stripes.push_back(stripe);
    TotalChunkCount += stripe->Chunks.size();
    FOREACH (const auto& chunk, stripe->Chunks) {
        TotalDataSize += chunk.DataSizeOverride;
        const auto& chunkAddresses = chunk.InputChunk->node_addresses();
        if (std::find_if(
            chunkAddresses.begin(),
            chunkAddresses.end(),
            [=] (const Stroka& chunkAddress) { return address == chunkAddress; })
            != chunkAddresses.end())
        {
            ++LocalChunkCount;
        } else {
            ++RemoteChunkCount;
        }
    }
}

void TPoolExtractionResult::AddStripe(TChunkStripePtr stripe)
{
    // NB: Keep this in sync with the above.
    Stripes.push_back(stripe);
    TotalChunkCount += stripe->Chunks.size();
    RemoteChunkCount += stripe->Chunks.size();
    FOREACH (const auto& chunk, stripe->Chunks) {
        TotalDataSize += chunk.DataSizeOverride;
    }
}

////////////////////////////////////////////////////////////////////

class TChunkPoolBase
    : public IChunkPool
{
public:
    virtual const TProgressCounter& DataSizeCounter() const override
    {
        return DataSizeCounter_;
    }

    virtual const TProgressCounter& ChunkCounter() const override
    {
        return ChunkCounter_;
    }

    virtual const TProgressCounter& StripeCounter() const override
    {
        return StripeCounter_;
    }

    virtual bool IsCompleted() const override
    {
        return DataSizeCounter_.GetCompleted() == DataSizeCounter_.GetTotal();
    }

    virtual bool IsPending() const override
    {
        return DataSizeCounter_.GetPending() > 0;
    }


protected:
    TProgressCounter DataSizeCounter_;
    TProgressCounter ChunkCounter_;
    TProgressCounter StripeCounter_;

};

////////////////////////////////////////////////////////////////////

class TUnorderedChunkPool
    : public TChunkPoolBase
{
public:
    explicit TUnorderedChunkPool(bool trackLocality)
        : TrackLocality(trackLocality)
    { }

    virtual void Add(TChunkStripePtr stripe) override
    {
        ChunkCounter_.Increment(stripe->Chunks.size());
        StripeCounter_.Increment(1);

        FOREACH (const auto& chunk, stripe->Chunks) {
            DataSizeCounter_.Increment(chunk.DataSizeOverride);
        }

        Register(stripe);
    }

    virtual TPoolExtractionResultPtr Extract(
        const Stroka& address,
        TNullable<i64> dataSizeThreshold) override
    {
        auto result = New<TPoolExtractionResult>();

        if (TrackLocality) {
            // Take local chunks first.
            auto addressIt = LocalChunks.find(address);
            if (addressIt != LocalChunks.end()) {
                const auto& entry = addressIt->second;
                AddAndUnregisterStripes(
                    result,
                    entry.Stripes.begin(),
                    entry.Stripes.end(),
                    dataSizeThreshold,
                    address);
            }
        }

        // Take remote chunks.
        AddAndUnregisterStripes(
            result,
            GlobalChunks.begin(),
            GlobalChunks.end(),
            dataSizeThreshold,
            address);

        DataSizeCounter_.Start(result->TotalDataSize);
        ChunkCounter_.Start(result->TotalChunkCount);
        StripeCounter_.Start(result->Stripes.size());

        return result;
    }

    virtual void OnFailed(TPoolExtractionResultPtr result) override
    {
        DataSizeCounter_.Failed(result->TotalDataSize);
        ChunkCounter_.Failed(result->TotalChunkCount);
        StripeCounter_.Failed(result->Stripes.size());

        FOREACH (auto stripe, result->Stripes) {
            Register(stripe);
        }
    }

    virtual void OnCompleted(TPoolExtractionResultPtr result) override
    {
        DataSizeCounter_.Completed(result->TotalDataSize);
        ChunkCounter_.Completed(result->TotalChunkCount);
        StripeCounter_.Completed(result->Stripes.size());
    }

    virtual i64 GetLocality(const Stroka& address) const override
    {
        YASSERT(TrackLocality);
        auto it = LocalChunks.find(address);
        return it == LocalChunks.end() ? 0 : it->second.TotalDataSize;
    }

private:
    bool TrackLocality;

    yhash_set<TChunkStripePtr> GlobalChunks;

    struct TChunkStripeHasher
    {
        size_t operator () (const TChunkStripePtr& stripe) const
        {
            size_t result = THash<TChunkStripe*>()(~stripe);
            if (!stripe->Chunks.empty()) {
                const auto& firstChunk = stripe->Chunks.front();
                result += 17 * firstChunk.InputChunk->slice().start_limit().row_index();
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
    
    yhash_map<Stroka,  TLocalityEntry> LocalChunks;

    void Register(TChunkStripePtr stripe)
    {
        if (TrackLocality) {
            FOREACH (const auto& chunk, stripe->Chunks) {
                auto inputChunk = chunk.InputChunk;
                FOREACH (const auto& address, inputChunk->node_addresses()) {
                    auto& entry = LocalChunks[address];
                    YVERIFY(entry.Stripes.insert(stripe).second);
                    entry.TotalDataSize += chunk.DataSizeOverride;
                }
            }
        }

        YVERIFY(GlobalChunks.insert(stripe).second);
    }

    void Unregister(TChunkStripePtr stripe)
    {
        if (TrackLocality) {
            FOREACH (const auto& chunk, stripe->Chunks) {
                auto inputChunk = chunk.InputChunk;
                FOREACH (const auto& address, inputChunk->node_addresses()) {
                    auto& entry = LocalChunks[address];
                    YVERIFY(entry.Stripes.erase(stripe) == 1);
                    entry.TotalDataSize -= chunk.DataSizeOverride;
                }
            }
        }

        YVERIFY(GlobalChunks.erase(stripe) == 1);
    }

    template <class TIterator>
    void AddAndUnregisterStripes(
        TPoolExtractionResultPtr result,
        const TIterator& begin,
        const TIterator& end,
        TNullable<i64> dataSizeThreshold,
        const Stroka& address)
    {
        int oldSize = static_cast<int>(result->Stripes.size());
        for (auto it = begin; it != end; ++it) {
            if (dataSizeThreshold && result->TotalDataSize >= dataSizeThreshold.Get()) {
                break;
            }
            if (TrackLocality) {
                result->AddStripe(*it, address);
            } else {
                result->AddStripe(*it);
            }
        }
        int newSize = static_cast<int>(result->Stripes.size());
        for (int index = oldSize; index < newSize; ++index) {
            Unregister(result->Stripes[index]);
        }
    }
};

TAutoPtr<IChunkPool> CreateUnorderedChunkPool(bool trackLocality)
{
    return new TUnorderedChunkPool(trackLocality);
}

////////////////////////////////////////////////////////////////////

class TAtomicChunkPool
    : public TChunkPoolBase
{
public:
    TAtomicChunkPool()
        : Extracted(false)
        , Initialized(false)
    { }

    virtual void Add(TChunkStripePtr stripe) override
    {
        YCHECK(!Initialized);

        ChunkCounter_.Increment(stripe->Chunks.size());
        StripeCounter_.Increment(1);

        Stripes.push_back(stripe);
        
        FOREACH (const auto& chunk, stripe->Chunks) {
            DataSizeCounter_.Increment(chunk.DataSizeOverride);
            auto inputChunk = chunk.InputChunk;
            FOREACH (const auto& address, inputChunk->node_addresses()) {
                AddressToLocality[address] += chunk.DataSizeOverride;
            }
        }
    }

    virtual TPoolExtractionResultPtr Extract(
        const Stroka& address,
        TNullable<i64> dataSizeThreshold) override
    {
        UNUSED(dataSizeThreshold);

        Initialized = true;
        YCHECK(!Extracted);

        auto result = New<TPoolExtractionResult>();
        FOREACH (auto stripe, Stripes) {
            result->AddStripe(stripe, address);
        }

        Extracted = true;
        DataSizeCounter_.Start(result->TotalDataSize);
        ChunkCounter_.Start(result->TotalChunkCount);
        StripeCounter_.Start(result->Stripes.size());

        return result;
    }

    virtual void OnFailed(TPoolExtractionResultPtr result) override
    {
        YCHECK(Initialized);
        YCHECK(Extracted);

        Extracted = false;
        DataSizeCounter_.Failed(result->TotalDataSize);
        ChunkCounter_.Failed(result->TotalChunkCount);
        StripeCounter_.Failed(result->Stripes.size());
    }

    virtual void OnCompleted(TPoolExtractionResultPtr result) override
    {
        YCHECK(Initialized);
        YCHECK(Extracted);

        DataSizeCounter_.Completed(result->TotalDataSize);
        ChunkCounter_.Completed(result->TotalChunkCount);
        StripeCounter_.Completed(result->Stripes.size());
    }

    virtual i64 GetLocality(const Stroka& address) const override
    {
        if (Extracted) {
            return 0;
        }
        auto it = AddressToLocality.find(address);
        return it == AddressToLocality.end() ? 0 : it->second;
    }

private:
    std::vector<TChunkStripePtr> Stripes;

    //! Addresses of added chunks.
    yhash_map<Stroka, i64> AddressToLocality;

    //! Have the stripes been #Extract'ed?
    bool Extracted;

    //! Has any #Extract call been made already?
    bool Initialized;

};

TAutoPtr<IChunkPool> CreateAtomicChunkPool()
{
    return new TAtomicChunkPool();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

