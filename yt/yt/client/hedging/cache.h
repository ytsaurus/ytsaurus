#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <util/generic/strbuf.h>

namespace NYT::NClient::NHedging::NRpc {

class TConfig;

////////////////////////////////////////////////////////////////////////////////

//! Cache of  clients per cluster.
class IClientsCache
    : public NYT::TRefCounted 
{
public:
    virtual NYT::NApi::IClientPtr GetClient(TStringBuf clusterUrl) = 0;
};

DECLARE_REFCOUNTED_TYPE(IClientsCache);
DEFINE_REFCOUNTED_TYPE(IClientsCache);

////////////////////////////////////////////////////////////////////////////////

//! Creates clients cache which shares same config (except server name).
IClientsCachePtr CreateClientsCache(const TConfig& config, const NYT::NApi::TClientOptions& options);

//! Shortcut to use client options from env.
IClientsCachePtr CreateClientsCache(const TConfig& config);

//! Shortcut to create cache with custom options and proxy role.
IClientsCachePtr CreateClientsCache(const NYT::NApi::TClientOptions& options);

//! Shortcut to create cache with default config.
IClientsCachePtr CreateClientsCache();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
