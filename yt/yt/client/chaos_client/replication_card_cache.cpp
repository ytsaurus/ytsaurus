#include "replication_card_cache.h"

#include <library/cpp/yt/string/format.h>
#include <library/cpp/yt/string/guid.h>

#include <util/digest/multi.h>

namespace NYT::NChaosClient {

///////////////////////////////////////////////////////////////////////////////

TReplicationCardCacheKey::operator size_t() const
{
    return MultiHash(
        CardId,
        RequestHistory,
        RequestCoordinators,
        RequestProgress);
};

void FormatValue(TStringBuilderBase* builder, const TReplicationCardCacheKey& key, TStringBuf /*spec*/)
{
    builder->AppendFormat("{CardId: %v, History: %v, Progress: %v, Coordinators: %v}",
        key.CardId,
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
