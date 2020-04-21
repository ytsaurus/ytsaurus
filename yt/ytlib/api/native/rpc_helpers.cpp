#include "rpc_helpers.h"
#include "config.h"

namespace NYT::NApi::NNative {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

bool IsMasterCacheEnabled(
    const TConnectionConfigPtr& config,
    const TMasterReadOptions& options)
{
    const auto& cache = config->MasterCache;
    if (!cache) {
        return false;
    }

    if (!cache->EnableMasterCacheDiscovery && cache->Addresses.empty()) {
        return false;
    }
    return options.ReadFrom == EMasterChannelKind::Cache || options.ReadFrom == EMasterChannelKind::SecondLevelCache;
}

void SetCachingHeader(
    const IClientRequestPtr& request,
    const TConnectionConfigPtr& config,
    const TMasterReadOptions& options,
    NHydra::TRevision refreshRevision)
{
    if (!IsMasterCacheEnabled(config, options)) {
        return;
    }
    auto* cachingHeaderExt = request->Header().MutableExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext);
    cachingHeaderExt->set_success_expiration_time(ToProto<i64>(options.ExpireAfterSuccessfulUpdateTime));
    cachingHeaderExt->set_failure_expiration_time(ToProto<i64>(options.ExpireAfterFailedUpdateTime));
    if (refreshRevision != NHydra::NullRevision) {
        cachingHeaderExt->set_refresh_revision(refreshRevision);
    }
}

void SetBalancingHeader(
    const IClientRequestPtr& request,
    const TConnectionConfigPtr& config,
    const TMasterReadOptions& options)
{
    if (!IsMasterCacheEnabled(config, options)) {
        return;
    }
    auto* balancingHeaderExt = request->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext);
    balancingHeaderExt->set_enable_stickness(true);
    balancingHeaderExt->set_sticky_group_size(max(config->CacheStickyGroupSizeOverride, options.CacheStickyGroupSize));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
