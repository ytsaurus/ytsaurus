#pragma once

#include "public.h"

#include <yt/core/http/public.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/net/public.h>

namespace NYT {
namespace NCrypto {

////////////////////////////////////////////////////////////////////////////////

NHttp::IServerPtr CreateHttpsServer(
    const TSslContextPtr& sslContext,
    const NHttp::TServerConfigPtr& config,
    const NConcurrency::IPollerPtr& poller);

NHttp::IServerPtr CreateHttpsServer(
    const TSslContextPtr& sslContext,
    const NNet::IListenerPtr& listener,
    const NConcurrency::IPollerPtr& poller);

NHttp::IClientPtr CreateHttpsClient(
    const TSslContextPtr& sslContext,
    const NHttp::TClientConfigPtr& config,
    const NConcurrency::IPollerPtr& poller);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCrypto
} // namespace NYT
