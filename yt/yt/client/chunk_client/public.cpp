#include "public.h"

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

const TChunkId NullChunkId = NObjectClient::NullObjectId;
const TChunkViewId NullChunkViewId = NObjectClient::NullObjectId;
const TChunkListId NullChunkListId = NObjectClient::NullObjectId;
const TChunkTreeId NullChunkTreeId = NObjectClient::NullObjectId;

const TString DefaultStoreAccountName("sys");
const TString DefaultStoreMediumName("default");
const TString DefaultCacheMediumName("cache");
const TString DefaultSlotsMediumName("default");

////////////////////////////////////////////////////////////////////////////////

static_assert(TypicalMediumCount <= MaxMediumCount, "Typical medium count exceeds max medium count");
static_assert(MaxMediumCount <= RealMediumIndexBound, "Max medium count exceeds bound on real medium indexes");
static_assert(RealMediumIndexBound <= MediumIndexBound, "Real medium index bound exceeds medium index bound");

constexpr auto GetSentinemMediumIndexesImpl()
{
    std::array<int, 2 + (MediumIndexBound - RealMediumIndexBound)> sentinels = {GenericMediumIndex, AllMediaIndex};
    for (int index = RealMediumIndexBound; index < MediumIndexBound; ++index) {
        sentinels[2 + (index - RealMediumIndexBound)] = index;
    }
    return sentinels;
}

std::span<const int> GetSentinelMediumIndexes()
{
    static const auto sentinels = GetSentinemMediumIndexesImpl();
    return std::span<const int>(sentinels);
}

bool IsValidRealMediumIndex(int mediumIndex)
{
    return mediumIndex >= 0 && mediumIndex < RealMediumIndexBound &&
        mediumIndex != GenericMediumIndex && mediumIndex != AllMediaIndex;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
