#include "replication_card_cache.h"

#include <yt/yt/core/misc/format.h>
#include <yt/yt/core/misc/hash.h>

namespace NYT::NChaosClient {

///////////////////////////////////////////////////////////////////////////////

TReplicationCardCacheKey::operator size_t() const
{
    size_t result = 0;
    HashCombine(result, Token);
    HashCombine(result, RequestHistory);
    HashCombine(result, RequestProgress);
    HashCombine(result, RequestCoordinators);
    return result;
};

void FormatValue(TStringBuilderBase* builder, const TReplicationCardCacheKey& key, TStringBuf /*spec*/)
{
    builder->AppendFormat("{Token: %v, History: %v, Progress: %v, Coordinators: %v}",
        key.Token,
        key.RequestHistory,
        key.RequestProgress,
        key.RequestCoordinators);
}

TString ToString(const TReplicationCardCacheKey& key)
{
    return ToStringViaBuilder(key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
