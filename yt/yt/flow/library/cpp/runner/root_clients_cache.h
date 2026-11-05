#pragma once

#include <yt/yt/client/api/options.h>

#include <yt/yt/client/cache/cache.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/public.h>

#include <functional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TRootClientsCacheOptions
{
    NYPath::TRichYPath PipelinePath;
    NClient::NCache::TClientsCacheConfigPtr ClientsCacheConfig;
    //! Role to reach the pipeline cluster with; applied to the connection configs that do not carry
    //! a role of their own.
    std::optional<std::string> ProxyRole;
    NApi::TClientOptions ClientOptions;
    //! The `clients_cache_factory` block of the node (resp. runner) config; may be null.
    //! Its schema is defined by the installed factory.
    NYTree::INodePtr Parameters;
};

using TRootClientsCacheFactory = std::function<NClient::NCache::IClientsCachePtr(const TRootClientsCacheOptions& options)>;

//! Installs the process-wide factory of the clients cache that a flow node and the runner take
//! their YT clients from, replacing the built-in one. This is the extension point for client
//! implementations living outside of flow; it must be called before #RunFlowNode() and before
//! running the runner program.
/*!
 *  The cache backs the pipeline connector, the queue log writer, the runner's pipeline client,
 *  the vanilla operation launch (#LaunchInVanillaJob()) and everything reached through
 *  `TComputationContext::ClientsCache` — computations, connectors and state managers. The pipeline
 *  authenticator and #StartFlowVanillaOperation() build their own clients and are not affected.
 */
void SetRootClientsCacheFactory(TRootClientsCacheFactory factory);

//! Fills |options.ProxyRole| into the connection configs that carry no role of their own and hands
//! the result to the installed factory.
NClient::NCache::IClientsCachePtr CreateRootClientsCache(const TRootClientsCacheOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
