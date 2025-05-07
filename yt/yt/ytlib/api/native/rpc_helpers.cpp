#include "rpc_helpers.h"
#include "config.h"

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

namespace NYT::NApi::NNative {

using namespace NRpc;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsCachingEnabled(
    const NApi::NNative::IConnectionPtr& connection,
    const TMasterReadOptions& options)
{
    switch (options.ReadFrom) {
        case EMasterChannelKind::Leader:
        case EMasterChannelKind::Follower:
            return false;
        case EMasterChannelKind::LocalCache:
        case EMasterChannelKind::MasterCache:
            return true;
        case EMasterChannelKind::Cache:
            return connection->GetMasterCellDirectory()->IsMasterCacheConfigured();
    }
    YT_ABORT();
}

} // namespace

NApi::EMasterChannelKind GetEffectiveMasterChannelKind(
    const IConnectionPtr& connection,
    NApi::EMasterChannelKind kind)
{
    if (kind == NApi::EMasterChannelKind::Cache &&
        !connection->GetMasterCellDirectory()->IsMasterCacheConfigured())
    {
        // If master cache is not configured then all |EMasterChannelKind::Cache| requests
        // will actually be routed to followers.
        return NApi::EMasterChannelKind::Follower;
    }

    return kind;
}

void SetCachingHeader(
    const IClientRequestPtr& request,
    const NApi::NNative::IConnectionPtr& connection,
    const TMasterReadOptions& options,
    NHydra::TRevision refreshRevision)
{
    if (!IsCachingEnabled(connection, options)) {
        return;
    }

    auto* cachingHeaderExt = request->Header().MutableExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext);
    cachingHeaderExt->set_disable_per_user_cache(options.DisablePerUserCache);
    cachingHeaderExt->set_expire_after_successful_update_time(ToProto(options.ExpireAfterSuccessfulUpdateTime));
    cachingHeaderExt->set_expire_after_failed_update_time(ToProto(options.ExpireAfterFailedUpdateTime));
    cachingHeaderExt->set_success_staleness_bound(ToProto(options.SuccessStalenessBound));
    if (refreshRevision != NHydra::NullRevision) {
        cachingHeaderExt->set_refresh_revision(ToProto(refreshRevision));
    }
}

void SetBalancingHeader(
    const TObjectServiceProxy::TReqExecuteBatchPtr& request,
    const NApi::NNative::IConnectionPtr& connection,
    const TMasterReadOptions& options)
{
    if (!IsCachingEnabled(connection, options)) {
        return;
    }

    request->SetStickyGroupSize(options.CacheStickyGroupSize.value_or(
        connection->GetConfig()->DefaultCacheStickyGroupSize));
    request->SetEnableClientStickiness(options.EnableClientCacheStickiness);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
