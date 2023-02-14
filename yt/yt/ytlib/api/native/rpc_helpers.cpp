#include "rpc_helpers.h"
#include "config.h"

namespace NYT::NApi::NNative {

using namespace NRpc;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsCachingEnabled(
    const NApi::NNative::IConnectionPtr& connection,
    const TMasterReadOptions& options)
{
    if (options.ReadFrom == EMasterChannelKind::LocalCache) {
        return true;
    }

    const auto& config = connection->GetStaticConfig()->MasterCache;

    if (!config) {
        return false;
    }

    if (!config->EnableMasterCacheDiscovery && !config->Endpoints && (!config->Addresses || config->Addresses->empty())) {
        return false;
    }

    return
        options.ReadFrom == EMasterChannelKind::Cache ||
        options.ReadFrom == EMasterChannelKind::MasterCache;
}

} // namespace

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
    cachingHeaderExt->set_expire_after_successful_update_time(ToProto<i64>(options.ExpireAfterSuccessfulUpdateTime));
    cachingHeaderExt->set_expire_after_failed_update_time(ToProto<i64>(options.ExpireAfterFailedUpdateTime));
    cachingHeaderExt->set_success_staleness_bound(ToProto<i64>(options.SuccessStalenessBound));
    if (refreshRevision != NHydra::NullRevision) {
        cachingHeaderExt->set_refresh_revision(refreshRevision);
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
