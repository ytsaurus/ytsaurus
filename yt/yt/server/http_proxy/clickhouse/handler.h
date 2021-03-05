#pragma once

#include <yt/yt/server/http_proxy/public.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/scheduler/public.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/library/syncmap/map.h>

#include "private.h"

namespace NYT::NHttpProxy::NClickHouse {

////////////////////////////////////////////////////////////////////////////////

class TClickHouseHandler
    : public NHttp::IHttpHandler
{
public:
    explicit TClickHouseHandler(TBootstrap* bootstrap);

    virtual void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

private:
    TBootstrap* const Bootstrap_;
    const TCoordinatorPtr Coordinator_;
    const TStaticClickHouseConfigPtr Config_;
    const NHttp::IClientPtr HttpClient_;
    const NApi::IClientPtr Client_;

    IInvokerPtr ControlInvoker_;

    NConcurrency::TSyncMap<TString, std::pair<int, NProfiling::TGauge>> UserToRunningQueryCount_;

    //! Used for alias resolving and ACL fetching.
    NScheduler::TOperationCachePtr OperationCache_;
    //! Used for validating user access against operation ACL.
    NSecurityClient::TPermissionCachePtr PermissionCache_;
    //! Used for instance discovery.
    TDiscoveryCachePtr DiscoveryCache_;

    NProfiling::TCounter QueryCount_;
    NProfiling::TCounter ForceUpdateCount_;
    NProfiling::TCounter BannedCount_;

    void AdjustQueryCount(const TString& user, int delta);
};

DEFINE_REFCOUNTED_TYPE(TClickHouseHandler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
