#include "sync_replica_cache.h"
#include "connection.h"
#include "tablet_helpers.h"

#include <exception>
#include <yt/yt/client/api/client.h>

namespace NYT::NApi::NNative {

using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

TSyncReplicaCache::TSyncReplicaCache(
    TAsyncExpiringCacheConfigPtr config,
    IConnectionPtr connection,
    const NLogging::TLogger& logger)
    : TAsyncExpiringCache(
        std::move(config),
        logger.WithTag("Cache: SyncReplicaCache"))
    , Connection_(std::move(connection))
{ }

TFuture<TTableReplicaInfoPtrList> TSyncReplicaCache::DoGet(
    const TSyncReplicaCacheKey& key,
    bool /*isPeriodicUpdate*/) noexcept
{
    auto connection = Connection_.Lock();
    if (!connection) {
        return MakeFuture<TTableReplicaInfoPtrList>(TError(NYT::EErrorCode::Canceled, "Connection destroyed"));
    }

    try {
        auto tableInfoFuture = connection->GetTableMountCache()->GetTableInfo(key);
        if (auto tableInfoOrError = tableInfoFuture.TryGet()) {
            return PickInSyncReplicas(
                connection,
                tableInfoOrError->ValueOrThrow(),
                TTabletReadOptions{});
        } else {
            return tableInfoFuture.Apply(BIND([=] (const TTableMountInfoPtr& tableInfo) {
                return PickInSyncReplicas(
                    connection,
                    tableInfo,
                    TTabletReadOptions{});
            }));
        }
    } catch (const std::exception& ex) {
        return MakeFuture<TTableReplicaInfoPtrList>(TError(ex));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
