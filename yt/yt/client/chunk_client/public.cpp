#include "public.h"

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

const TChunkId NullChunkId = NObjectClient::NullObjectId;
const TChunkViewId NullChunkViewId = NObjectClient::NullObjectId;
const TChunkListId NullChunkListId = NObjectClient::NullObjectId;
const TChunkTreeId NullChunkTreeId = NObjectClient::NullObjectId;

const std::string DefaultStoreAccountName("sys");
const std::string DefaultStoreMediumName("default");
const std::string DefaultCacheMediumName("cache");
const std::string DefaultSlotsMediumName("default");

////////////////////////////////////////////////////////////////////////////////

static_assert(TypicalMediumCount <= MaxMediumCount, "Typical medium count exceeds max medium count");
static_assert(MaxMediumCount <= RealMediumIndexBound, "Max medium count exceeds bound on real medium indexes");
static_assert(RealMediumIndexBound <= MediumIndexBound, "Real medium index bound exceeds medium index bound");

bool IsSentinelMediumIndex(int mediumIndex)
{
    return mediumIndex == GenericMediumIndex || mediumIndex == AllMediaIndex || (MediumIndexBound <= mediumIndex && mediumIndex < MediumIndexBound);
}

bool IsValidRealMediumIndex(int mediumIndex)
{
    return mediumIndex >= 0 && mediumIndex < RealMediumIndexBound &&
        mediumIndex != GenericMediumIndex && mediumIndex != AllMediaIndex;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
