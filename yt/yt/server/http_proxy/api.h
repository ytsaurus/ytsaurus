#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/ytlib/misc/public.h>

#include <yt/yt/client/driver/driver.h>

#include <yt/yt/core/http/http.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/syncmap/map.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

using TUserCommandPair = std::pair<std::string, TString>;
using TUserCounterMap = NConcurrency::TSyncMap<std::pair<std::string, std::string>, NProfiling::TCounter>;

class TSemaphoreGuard
{
public:
    TSemaphoreGuard(TSemaphoreGuard&&) = default;
    TSemaphoreGuard& operator = (TSemaphoreGuard&&) = default;

    TSemaphoreGuard(TApi* api, const TUserCommandPair& key);
    ~TSemaphoreGuard();

private:
    struct TEmptyDeleter
    {
        void operator () (TApi* /*api*/)
        { }
    };

    std::unique_ptr<TApi, TEmptyDeleter> Api_;
    TUserCommandPair Key_;
};

////////////////////////////////////////////////////////////////////////////////

class TApi
    : public NHttp::IHttpHandler
{
public:
    explicit TApi(TBootstrap* bootstrap);

    void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

    const NDriver::IDriverPtr& GetDriverV3() const;
    const NDriver::IDriverPtr& GetDriverV4() const;
    const TCompositeHttpAuthenticatorPtr& GetHttpAuthenticator() const;
    const TCoordinatorPtr& GetCoordinator() const;
    const TApiConfigPtr& GetConfig() const;
    TApiDynamicConfigPtr GetDynamicConfig() const;
    const NConcurrency::IPollerPtr& GetPoller() const;
    const INodeMemoryTrackerPtr& GetMemoryUsageTracker() const;

    bool IsUserBannedInCache(const std::string& user);
    void PutUserIntoBanCache(const std::string& user);

    TError CheckAccess(const std::string& user);

    std::optional<TSemaphoreGuard> AcquireSemaphore(const std::string& user, const TString& command);
    void ReleaseSemaphore(const TUserCommandPair& key);

    void IncrementBytesOutProfilingCounters(
        const std::string& user,
        const NNet::TNetworkAddress& clientAddress,
        i64 bytesOut,
        const std::optional<NFormats::TFormat>& outputFormat,
        const std::optional<NHttp::TContentEncoding>& outputCompression);

    void IncrementBytesInProfilingCounters(
        const std::string& user,
        const NNet::TNetworkAddress& clientAddress,
        i64 bytesIn,
        const std::optional<NFormats::TFormat>& inputFormat,
        const std::optional<NHttp::TContentEncoding>& inputCompression);

    void IncrementProfilingCounters(
        const std::string& user,
        const TString& command,
        std::optional<NHttp::EStatusCode> httpStatusCode,
        TErrorCode apiErrorCode,
        TDuration wallTime,
        TDuration cpuTime,
        const NNet::TNetworkAddress& clientAddress);

    void IncrementHttpCode(NHttp::EStatusCode httpStatusCode);

    int GetNumberOfConcurrentRequests();

    void BuildOrchid(NYson::IYsonConsumer* consumer);

    NYTree::IYPathServicePtr CreateOrchidService();

private:
    const TApiConfigPtr Config_;
    TAtomicIntrusivePtr<TApiDynamicConfig> DynamicConfig_;

    const NDriver::IDriverPtr DriverV3_;
    const NDriver::IDriverPtr DriverV4_;

    const TCompositeHttpAuthenticatorPtr HttpAuthenticator_;
    const TCoordinatorPtr Coordinator_;
    const IAccessCheckerPtr AccessChecker_;

    const IInvokerPtr ControlInvoker_;

    const NConcurrency::IPollerPtr Poller_;

    const INodeMemoryTrackerPtr MemoryUsageTracker_;

    const NProfiling::TProfiler SparseProfiler_ = HttpProxyProfiler.WithSparse();

    std::vector<std::pair<NNet::TIP6Network, TString>> Networks_;
    TString DefaultNetworkName_;

    std::string GetNetworkNameForAddress(const NNet::TNetworkAddress& address) const;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, BanCacheLock_);
    THashMap<TString, TInstant> BanCache_;

    struct TProfilingCounters
    {
        std::atomic<int> LocalSemaphore{0};

        NProfiling::TGauge ConcurrencySemaphore;
        NProfiling::TCounter RequestCount;
        NProfiling::TEventTimer RequestWallTime;
        NProfiling::TTimeCounter CumulativeRequestCpuTime;

        NConcurrency::TSyncMap<TErrorCode, NProfiling::TCounter> ApiErrors;
    };

    std::atomic<int> GlobalSemaphore_{0};

    NConcurrency::TSyncMap<TUserCommandPair, std::unique_ptr<TProfilingCounters>> Counters_;

    TUserCounterMap BytesIn_;
    TUserCounterMap BytesOut_;

    TUserCounterMap InputFormatBytes_;
    TUserCounterMap OutputFormatBytes_;

    TUserCounterMap InputCompressionBytes_;
    TUserCounterMap OutputCompressionBytes_;

    NConcurrency::TSyncMap<NHttp::EStatusCode, NProfiling::TCounter> HttpCodes_;
    NConcurrency::TSyncMap<std::pair<std::string, NHttp::EStatusCode>, NProfiling::TCounter> HttpCodesByUser_;
    NConcurrency::TSyncMap<std::pair<TString, NHttp::EStatusCode>, NProfiling::TCounter> HttpCodesByCommand_;

    TProfilingCounters* GetProfilingCounters(const TUserCommandPair& key);

    NProfiling::TCounter PrepareErrorCount_ = HttpProxyProfiler.Counter("/request_prepare_error_count");

    void DoIncrementHttpCode(
        THashMap<NHttp::EStatusCode, NProfiling::TCounter>* counters,
        NHttp::EStatusCode httpStatusCode,
        NProfiling::TTagIdList tags);

    void OnDynamicConfigChanged(
        const TProxyDynamicConfigPtr& /*oldConfig*/,
        const TProxyDynamicConfigPtr& newConfig);

    void IncrementUserCounter(
        TUserCounterMap* counterMap,
        const std::string& user,
        const std::string& networkName,
        const TString& counterName,
        const TString& tagName,
        const TString& tagValue,
        i64 value);
};

DEFINE_REFCOUNTED_TYPE(TApi)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
