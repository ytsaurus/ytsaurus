#pragma once

#include "public.h"

#include <yt/core/http/http.h>

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/ytlib/driver/driver.h>

namespace NYT {
namespace NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

// Key is a (user, command) pair.
typedef std::pair<TString, TString> TSemaphoreKey;

class TSemaphoreGuard
{
public:
    TSemaphoreGuard(TSemaphoreGuard&&) = default;
    TSemaphoreGuard& operator = (TSemaphoreGuard&&) = default;
    
    TSemaphoreGuard(TApi* api, const TSemaphoreKey& key);
    ~TSemaphoreGuard();

private:
    struct TEmptyDeleter
    {
        void operator () (TApi* api)
        { }
    };

    std::unique_ptr<TApi, TEmptyDeleter> Api_;
    TSemaphoreKey Key_;
};

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
    void ReleaseSemaphore(const TSemaphoreKey& key);

private:
    const TApiConfigPtr Config_;

    const NDriver::IDriverPtr DriverV3_;
    const NDriver::IDriverPtr DriverV4_;

    const THttpAuthenticatorPtr HttpAuthenticator_;
    const TCoordinatorPtr Coordinator_;

    NConcurrency::TReaderWriterSpinLock BanCacheLock_;
    THashMap<TString, TInstant> BanCache_;

    struct TSemaphore
    {
        NProfiling::TSimpleGauge Value;
    };

    std::atomic<int> GlobalSemaphore_{0};

    NConcurrency::TReaderWriterSpinLock SemaphoresLock_;
    THashMap<TSemaphoreKey, std::unique_ptr<TSemaphore>> Semaphores_;

    TSemaphore* GetSemaphore(const TSemaphoreKey& key);
};

DEFINE_REFCOUNTED_TYPE(TApi)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT
