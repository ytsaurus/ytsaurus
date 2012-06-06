#include "stdafx.h"
#include "chunk_pool.h"
#include "private.h"

namespace NYT {
namespace NScheduler {

using namespace NChunkServer;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////

TChunkStripe::TChunkStripe()
    : Weight(0)
{ }

TChunkStripe::TChunkStripe(const TInputChunk& inputChunk, i64 weight )
    : Weight(weight)
{
    InputChunks.push_back(inputChunk);
}

TChunkStripe::TChunkStripe(const std::vector<TInputChunk>& inputChunks, i64 weight)
    : Weight(weight)
{ }

void TChunkStripe::AddChunk(const NTableClient::NProto::TInputChunk& inputChunk, i64 weight)
{
    InputChunks.push_back(inputChunk);
    Weight += weight;
}

std::vector<NChunkServer::TChunkId> TChunkStripe::GetChunkIds() const
{
    std::vector<NChunkServer::TChunkId> result;
    FOREACH (const auto& chunk, InputChunks) {
        result.push_back(TChunkId::FromProto(chunk.slice().chunk_id()));
    }
    return result;
}

////////////////////////////////////////////////////////////////////

TPoolExtractionResult::TPoolExtractionResult()
    : TotalChunkWeight(0)
    , TotalChunkCount(0)
    , LocalChunkCount(0)
    , RemoteChunkCount(0)
{ }

void TPoolExtractionResult::Add(TChunkStripePtr stripe, const Stroka& address)
{
    Stripes.push_back(stripe);
    TotalChunkWeight += stripe->Weight;
    FOREACH (const auto& chunk, stripe->InputChunks) {
        ++TotalChunkCount;
        if (std::find_if(
            chunk.node_addresses().begin(),
            chunk.node_addresses().end(),
            [&] (const Stroka& chunkAddress) { return address == chunkAddress; })
            != chunk.node_addresses().end())
        {
            ++LocalChunkCount;
        } else {
            ++RemoteChunkCount;
        }
    }
}

////////////////////////////////////////////////////////////////////

class TUnorderedChunkPool::TImpl
{
public:
    TImpl()
        : TotalWeight(0)
        , PendingWeight(0)
        , CompletedWeight(0)
    { }

    void Add(TChunkStripePtr stripe)
    {
        YASSERT(stripe->Weight > 0);
        TotalWeight += stripe->Weight;
        PendingWeight += stripe->Weight;
        FOREACH (const auto& inputChunk, stripe->InputChunks) {
            FOREACH (const auto& address, inputChunk.node_addresses()) {
                YVERIFY(LocalChunks[address].insert(stripe).second);
            }
        }
        YVERIFY(GlobalChunks.insert(stripe).second);
    }

    TPoolExtractionResultPtr Extract(
        const Stroka& address,
        i64 weightThreshold,
        bool needLocal)
    {
        auto result = New<TPoolExtractionResult>();

        // Take local chunks first.
        auto addressIt = LocalChunks.find(address);
        if (addressIt != LocalChunks.end()) {
            const auto& localChunks = addressIt->second;
            AddStripes(
                result,
                localChunks.begin(),
                localChunks.end(),
                weightThreshold,
                address);
        }

        if (result->LocalChunkCount == 0 && needLocal) {
            // Could not find any local chunks but requested to do so.
            // Don't look at remote ones.
            return NULL;
        }

        // Unregister taken local chunks.
        // We have to do this right away, otherwise we risk getting same chunks
        // in the next phase.
        for (int index = 0; index < result->LocalChunkCount; ++index) {
            Unregister(result->Stripes[index]);
        }

        // Take remote chunks.
        AddStripes(
            result,
            GlobalChunks.begin(),
            GlobalChunks.end(),
            weightThreshold,
            address);

        // Unregister taken remote chunks.
        for (int index = result->LocalChunkCount; index < result->LocalChunkCount + result->RemoteChunkCount; ++index) {
            Unregister(result->Stripes[index]);
        }

        return result;
    }

    void Failed(TPoolExtractionResultPtr result)
    {
        FOREACH (const auto& stripe, result->Stripes) {
            Add(stripe);
        }
    }

    void Completed(TPoolExtractionResultPtr result)
    {
        CompletedWeight += result->TotalChunkWeight;
    }

    i64 GetTotalWeight() const
    {
        return TotalWeight;
    }

    i64 GetPendingWeight() const
    {
        return PendingWeight;
    }

    i64 GetCompletedWeight() const
    {
        return CompletedWeight;
    }

    bool IsCompleted() const
    {
        return CompletedWeight == TotalWeight;
    }

    bool HasPendingChunks() const
    {
        return !GlobalChunks.empty();
    }

    bool HasPendingLocalChunksAt(const Stroka& address) const
    {
        auto it = LocalChunks.find(address);
        return it == LocalChunks.end() ? false : !it->second.empty();
    }

private:
    i64 TotalWeight;
    i64 PendingWeight;
    i64 CompletedWeight;
    yhash_map<Stroka, yhash_set<TChunkStripePtr> > LocalChunks;
    yhash_set<TChunkStripePtr> GlobalChunks;
    
    void Unregister(TChunkStripePtr stripe)
    {
        FOREACH (const auto& inputChunk, stripe->InputChunks) {
            FOREACH (const auto& address, inputChunk.node_addresses()) {
                YVERIFY(LocalChunks[address].erase(stripe) == 1);
            }
        }
        YVERIFY(GlobalChunks.erase(stripe) == 1);
        PendingWeight -= stripe->Weight;
    }

    template <class TIterator>
    void AddStripes(
        TPoolExtractionResultPtr result,
        const TIterator& begin,
        const TIterator& end,
        i64 weightThreshold,
        const Stroka& address)
    {
        for (auto it = begin; it != end; ++it) {
            if (result->TotalChunkWeight >= weightThreshold) {
                break;
            }
            result->Add(*it, address);
        }
    }
};

////////////////////////////////////////////////////////////////////

TUnorderedChunkPool::TUnorderedChunkPool()
    : Impl(new TImpl())
{ }

TUnorderedChunkPool::~TUnorderedChunkPool()
{ }

void TUnorderedChunkPool::Add(TChunkStripePtr stripe)
{
    Impl->Add(stripe);
}

TPoolExtractionResultPtr TUnorderedChunkPool::Extract(
    const Stroka& address,
    i64 weightThreshold,
    bool needLocal)
{
    return Impl->Extract(
        address,
        weightThreshold,
        needLocal);
}

void TUnorderedChunkPool::Failed(TPoolExtractionResultPtr result)
{
    Impl->Failed(result);
}

void TUnorderedChunkPool::Completed(TPoolExtractionResultPtr result)
{
    return Impl->Completed(result);
}

i64 TUnorderedChunkPool::GetTotalWeight() const
{
    return Impl->GetTotalWeight();
}

i64 TUnorderedChunkPool::GetPendingWeight() const
{
    return Impl->GetPendingWeight();
}

i64 TUnorderedChunkPool::GetCompletedWeight() const
{
    return Impl->GetCompletedWeight();
}

bool TUnorderedChunkPool::IsCompleted() const
{
    return Impl->IsCompleted();
}

bool TUnorderedChunkPool::HasPendingChunks() const
{
    return Impl->HasPendingChunks();
}

bool TUnorderedChunkPool::HasPendingLocalChunksAt(const Stroka& address) const
{
    return Impl->HasPendingLocalChunksAt(address);
}

////////////////////////////////////////////////////////////////////

class TAtomicChunkPool::TImpl
{
public:
    TImpl()
        : TotalWeight(0)
        , Extracted(false)
        , Initialized(false)
        , Completed_(false)
    { }

    void Add(TChunkStripePtr stripe)
    {
        YASSERT(!Initialized);
        TotalWeight += stripe->Weight;
        Stripes.push_back(stripe);
        FOREACH (const auto& inputChunk, stripe->InputChunks) {
            FOREACH (const auto& address, inputChunk.node_addresses()) {
                Addresses.insert(address);
            }
        }
    }

    TPoolExtractionResultPtr Extract(
        const Stroka& address,
        bool needLocal)
    {
        Initialized = true;
        YASSERT(!Extracted);

        if (needLocal && !HasPendingLocalChunksAt(address)) {
            return NULL;
        }

        auto result = New<TPoolExtractionResult>();
        FOREACH (const auto& stripe, Stripes) {
            result->Add(stripe, address);
        }

        Extracted = true;
        return result;
    }

    void Failed(TPoolExtractionResultPtr result)
    {
        YASSERT(Initialized);
        YASSERT(Extracted);
        Extracted = false;
    }

    void Completed(TPoolExtractionResultPtr result)
    {
        YASSERT(Initialized);
        YASSERT(Extracted);
    }

    i64 GetTotalWeight() const
    {
        return TotalWeight;
    }

    i64 GetPendingWeight() const
    {
        return Extracted ? 0 : TotalWeight;
    }

    i64 GetCompletedWeight() const
    {
        return Completed_ ? TotalWeight : 0;
    }

    bool IsCompleted() const
    {
        return Completed_;
    }

    bool HasPendingChunks() const
    {
        return !Extracted && !Stripes.empty();
    }

    bool HasPendingLocalChunksAt(const Stroka& address) const
    {
        return
            Extracted
            ? false
            : Addresses.find(address) != Addresses.end();
    }

private:
    i64 TotalWeight;
    std::vector<TChunkStripePtr> Stripes;
    //! Addresses of added chunks.
    yhash_set<Stroka> Addresses;
    //! Have the stripes been #Extract'ed?
    bool Extracted;
    //! Has any #Extract call been made already?
    bool Initialized;
    //! Were extracted chunks processed successfully?
    bool Completed_;

};

////////////////////////////////////////////////////////////////////

TAtomicChunkPool::TAtomicChunkPool()
    : Impl(new TImpl())
{ }

TAtomicChunkPool::~TAtomicChunkPool()
{ }

void TAtomicChunkPool::Add(TChunkStripePtr stripe)
{
    Impl->Add(stripe);
}

TPoolExtractionResultPtr TAtomicChunkPool::Extract(
    const Stroka& address,
    bool needLocal)
{
    return Impl->Extract(
        address,
        needLocal);
}

void TAtomicChunkPool::Failed(TPoolExtractionResultPtr result)
{
    Impl->Failed(result);
}

void TAtomicChunkPool::Completed(TPoolExtractionResultPtr result)
{
    Impl->Completed(result);
}

i64 TAtomicChunkPool::GetTotalWeight() const
{
    return Impl->GetTotalWeight();
}

i64 TAtomicChunkPool::GetPendingWeight() const
{
    return Impl->GetPendingWeight();
}

i64 TAtomicChunkPool::GetCompletedWeight() const
{
    return Impl->GetCompletedWeight();
}

bool TAtomicChunkPool::IsCompleted() const
{
    return Impl->IsCompleted();
}

bool TAtomicChunkPool::HasPendingChunks() const
{
    return Impl->HasPendingChunks();
}

bool TAtomicChunkPool::HasPendingLocalChunksAt(const Stroka& address) const
{
    return Impl->HasPendingLocalChunksAt(address);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

