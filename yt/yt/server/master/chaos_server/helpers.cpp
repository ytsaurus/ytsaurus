#include "helpers.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/options.h>
#include <yt/yt/client/api/table_client.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NChaosServer {

using namespace NApi;
using namespace NChaosClient;

////////////////////////////////////////////////////////////////////////////////

TErrorOr<int> GetMinimalTabletCount(std::vector<TErrorOr<int>> tabletCounts)
{
    if (tabletCounts.empty()) {
        return 0;
    }

    std::erase_if(
        tabletCounts,
        [] (const auto& tabletCount) {
            return !tabletCount.IsOK();
        });

    auto minElementIt = std::min_element(
        tabletCounts.begin(),
        tabletCounts.end(),
        [] (const auto& a, const auto& b) {
            return a.Value() < b.Value();
        });

    if (minElementIt == tabletCounts.end()) {
        return TError("Failed to get tablet count from any replica");
    }

    return minElementIt->Value();
}

TFuture<TReplicationCardPtr> GetReplicationCard(
    const NNative::IConnectionPtr& connection,
    TReplicationCardId replicationCardId,
    const TReplicationCardFetchOptions& options)
{
    TGetReplicationCardOptions getCardOptions;
    static_cast<TReplicationCardFetchOptions&>(getCardOptions) = options;
    getCardOptions.BypassCache = true;

    auto clientOptions = TClientOptions::FromAuthenticationIdentity(NRpc::GetCurrentAuthenticationIdentity());
    auto client = connection->CreateClient(clientOptions);

    return client->GetReplicationCard(replicationCardId, getCardOptions)
        .Apply(BIND([client] (const TReplicationCardPtr& card) {
            return card;
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
