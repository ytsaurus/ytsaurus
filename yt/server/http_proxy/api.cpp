#include "api.h"

#include "bootstrap.h"
#include "config.h"
#include "context.h"
#include "private.h"

#include <yt/core/http/helpers.h>

#include <yt/core/profiling/profile_manager.h>

namespace NYT {
namespace NHttpProxy {

using namespace NConcurrency;
using namespace NHttp;

static auto& Logger = HttpProxyLogger;

////////////////////////////////////////////////////////////////////////////////

TSemaphoreGuard::TSemaphoreGuard(TApi* api, const TSemaphoreKey& key)
    : Api_(api)
    , Key_(key)
{ }

TSemaphoreGuard::~TSemaphoreGuard()
{
    if (Api_) {
        Api_->ReleaseSemaphore(Key_);
    }
}

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
    TReaderGuard guard(BanCacheLock_);
    auto it = BanCache_.find(user);
    if (it != BanCache_.end()) {
        return now < it->second;
    }

    return false;
}

void TApi::PutUserIntoBanCache(const TString& user)
{
    TWriterGuard guard(BanCacheLock_);
    BanCache_[user] = TInstant::Now() + Config_->BanCacheExpirationTime;
}

TNullable<TSemaphoreGuard> TApi::AcquireSemaphore(const TString& user, const TString& command)
{
    auto value = GlobalSemaphore_.load();
    do {
        if (value >= Config_->ConcurrencyLimit) {
            return {};
        }
    } while (!GlobalSemaphore_.compare_exchange_weak(value, value + 1));

    auto key = std::make_pair(user, command);
    auto semaphore = GetSemaphore(key);

    HttpProxyProfiler.Increment(semaphore->Value);

    return TSemaphoreGuard(this, key);
}

void TApi::ReleaseSemaphore(const TSemaphoreKey& key)
{
    auto semaphore = GetSemaphore(key);
    GlobalSemaphore_.fetch_add(-1);
    HttpProxyProfiler.Increment(semaphore->Value, -1);
}

TApi::TSemaphore* TApi::GetSemaphore(const TSemaphoreKey& key)
{
    {
        TReaderGuard guard(SemaphoresLock_);
        auto semaphore = Semaphores_.find(key);
        if (semaphore != Semaphores_.end()) {
            return semaphore->second.get();
        }
    }

    auto semaphore = std::make_unique<TSemaphore>();
    semaphore->Value = {
        "/concurrency_semaphore", {
            NProfiling::TProfileManager::Get()->RegisterTag("user", key.first),
            NProfiling::TProfileManager::Get()->RegisterTag("command", key.second),
        }
    };

    TWriterGuard guard(SemaphoresLock_);
    auto result = Semaphores_.emplace(key, std::move(semaphore));
    return result.first->second.get();
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
