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
static_assert(MaxMediumCount <= MediumIndexBound, "Max medium count exceeds medium index bound");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
