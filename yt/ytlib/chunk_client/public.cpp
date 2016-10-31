#include "public.h"

#include <yt/ytlib/misc/workload.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

const TChunkId NullChunkId = NObjectClient::NullObjectId;
const TChunkListId NullChunkListId = NObjectClient::NullObjectId;
const TChunkTreeId NullChunkTreeId = NObjectClient::NullObjectId;

const Stroka DefaultStoreMediumName("default");
const Stroka DefaultCacheMediumName("cache");

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
