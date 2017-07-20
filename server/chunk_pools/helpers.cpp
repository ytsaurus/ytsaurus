#include "helpers.h"

#include "chunk_pool.h"

namespace NYT {
namespace NChunkPools {

using namespace NNodeTrackerClient;
using namespace NControllerAgent;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

void AddStripeToList(
    const TChunkStripePtr& stripe,
    i64 stripeDataSize,
    i64 stripeRowCount,
    const TChunkStripeListPtr& list,
    TNodeId nodeId)
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

////////////////////////////////////////////////////////////////////////////////

TChunkStripeListPtr ApplyChunkMappingToStripe(
    const TChunkStripeListPtr& stripeList,
    const yhash<TInputChunkPtr, TInputChunkPtr>& inputChunkMapping)
{
    auto mappedStripeList = New<TChunkStripeList>(stripeList->Stripes.size());
    for (int stripeIndex = 0; stripeIndex < stripeList->Stripes.size(); ++stripeIndex) {
        const auto& stripe = stripeList->Stripes[stripeIndex];
        YCHECK(stripe);
        const auto& mappedStripe = (mappedStripeList->Stripes[stripeIndex] = New<TChunkStripe>(stripe->Foreign));
        for (const auto& dataSlice : stripe->DataSlices) {
            TInputDataSlice::TChunkSliceList mappedChunkSlices;
            for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                auto iterator = inputChunkMapping.find(chunkSlice->GetInputChunk());
                YCHECK(iterator != inputChunkMapping.end());
                mappedChunkSlices.emplace_back(New<TInputChunkSlice>(*chunkSlice));
                mappedChunkSlices.back()->SetInputChunk(iterator->second);
            }

            mappedStripe->DataSlices.emplace_back(New<TInputDataSlice>(
                    dataSlice->Type,
                    std::move(mappedChunkSlices),
                    dataSlice->LowerLimit(),
                    dataSlice->UpperLimit()));
            mappedStripe->DataSlices.back()->Tag = dataSlice->Tag;
            mappedStripe->DataSlices.back()->InputStreamIndex = dataSlice->InputStreamIndex;
        }
    }

    mappedStripeList->IsApproximate = stripeList->IsApproximate;
    mappedStripeList->TotalDataSize = stripeList->TotalDataSize;
    mappedStripeList->LocalDataSize = stripeList->LocalDataSize;
    mappedStripeList->TotalRowCount = stripeList->TotalRowCount;
    mappedStripeList->TotalChunkCount = stripeList->TotalChunkCount;
    mappedStripeList->LocalChunkCount = stripeList->LocalChunkCount;

    return mappedStripeList;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT

