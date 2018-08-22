#include "api.h"

#include "bootstrap.h"
#include "config.h"
#include "context.h"
#include "private.h"

#include <yt/core/http/helpers.h>

namespace NYT {
namespace NHttpProxy {

using namespace NHttp;

static auto& Logger = HttpProxyLogger;

////////////////////////////////////////////////////////////////////////////////

TApi::TApi(TBootstrap* bootstrap)
    : Config_(bootstrap->GetConfig()->Api)
    , DriverV3_(bootstrap->GetDriverV3())
    , DriverV4_(bootstrap->GetDriverV4())
    , HttpAuthenticator_(bootstrap->GetHttpAuthenticator())
    , Coordinator_(bootstrap->GetCoordinator())
{ }

const NDriver::IDriverPtr& TApi::GetDriverV3() const
{
    return DriverV3_;
}

const NDriver::IDriverPtr& TApi::GetDriverV4() const
{
    return DriverV4_;
}

const THttpAuthenticatorPtr& TApi::GetHttpAuthenticator() const
{
    return HttpAuthenticator_;
}

const TCoordinatorPtr& TApi::GetCoordinator() const
{
    return Coordinator_;
}

bool TApi::IsUserBannedInCache(const TString& user)
{
    auto now = TInstant::Now();
    auto guard = Guard(Lock_);
    auto it = BanCache_.find(user);
    if (it != BanCache_.end()) {
        return now < it->second;
    }

    return false;
}

void TApi::PutUserIntoBanCache(const TString& user)
{
    auto guard = Guard(Lock_);
    BanCache_[user] = TInstant::Now() + Config_->BanCacheExpirationTime;
}

void TApi::HandleRequest(
    const IRequestPtr& req,
    const IResponseWriterPtr& rsp)
{
    if (MaybeHandleCors(req, rsp)) {
        return;
    }

    auto context = New<TContext>(MakeStrong(this), req, rsp);
    if (!context->TryPrepare()) {
        return;
    }
    try {
        context->FinishPrepare();
        context->Run();
    } catch (const std::exception& ex) {
        context->SetError(TError(ex));
        LOG_ERROR(ex, "Command failed");
    }

    context->Finalize();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT
