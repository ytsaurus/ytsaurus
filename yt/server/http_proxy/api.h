#pragma once

#include "public.h"

#include <yt/core/http/http.h>

#include <yt/ytlib/driver/driver.h>

namespace NYT {
namespace NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

class TSemaphoreGuard
{ };

////////////////////////////////////////////////////////////////////////////////

class TApi
    : public NHttp::IHttpHandler
{
public:
    explicit TApi(TBootstrap* bootstrap);

    virtual void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

    const NDriver::IDriverPtr& GetDriverV3() const;
    const NDriver::IDriverPtr& GetDriverV4() const;
    const THttpAuthenticatorPtr& GetHttpAuthenticator() const;
    const TCoordinatorPtr& GetCoordinator() const;

    bool IsUserBannedInCache(const TString& user);
    void PutUserIntoBanCache(const TString& user);

    TNullable<TSemaphoreGuard> AcquireSemaphore(const TString& user, const TString& command);

private:
    const TApiConfigPtr Config_;

    const NDriver::IDriverPtr DriverV3_;
    const NDriver::IDriverPtr DriverV4_;

    const THttpAuthenticatorPtr HttpAuthenticator_;
    const TCoordinatorPtr Coordinator_;

    TSpinLock Lock_;
    THashMap<TString, TInstant> BanCache_;
};

DEFINE_REFCOUNTED_TYPE(TApi)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT
