#include "public.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

const TChunkId NullChunkId = NObjectClient::NullObjectId;
const TChunkListId NullChunkListId = NObjectClient::NullObjectId;
const TChunkTreeId NullChunkTreeId = NObjectClient::NullObjectId;

const TString DefaultStoreAccountName("sys");
const TString DefaultStoreMediumName("default");
const TString DefaultCacheMediumName("cache");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
