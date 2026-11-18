#pragma once

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/http/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! The most companion replies the proxy holds at once, each until its client has read it.
inline constexpr int MaxCompanionProfilingRequestsInFlight = 2;

//! Serves |/companion/ytprof/...| on |server| by forwarding requests to the
//! |/ytprof/...| endpoints of the companion monitoring server on |companionMonitoringPort|.
/*!
 *  |/ytprof/binary| is not served. Requests beyond
 *  #MaxCompanionProfilingRequestsInFlight are rejected with 429 Too Many Requests.
 */
void RegisterCompanionProfilingProxy(
    const NHttp::IServerPtr& server,
    const NHttp::TClientConfigPtr& clientConfig,
    const NConcurrency::IPollerPtr& poller,
    int companionMonitoringPort);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
