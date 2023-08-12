#include "api.h"

#include "access_checker.h"
#include "bootstrap.h"
#include "config.h"
#include "context.h"
#include "dynamic_config_manager.h"
#include "private.h"

#include <yt/yt/core/http/helpers.h>

#include <yt/yt/core/misc/finally.h>

namespace NYT::NHttpProxy {

using namespace NConcurrency;
using namespace NHttp;
using namespace NProfiling;
using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////////////////

TSemaphoreGuard::TSemaphoreGuard(TApi* api, const TUserCommandPair& key)
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
    , DynamicConfig_(New<TApiDynamicConfig>())
    , DriverV3_(bootstrap->GetDriverV3())
    , DriverV4_(bootstrap->GetDriverV4())
    , HttpAuthenticator_(bootstrap->GetHttpAuthenticator())
    , Coordinator_(bootstrap->GetCoordinator())
    , AccessChecker_(bootstrap->GetAccessChecker())
    , Poller_(bootstrap->GetPoller())
    , DefaultNetworkName_(bootstrap->GetConfig()->DefaultNetwork)
{
    for (const auto& network : bootstrap->GetConfig()->Networks) {
        for (const auto& prefix : network.second) {
            Networks_.emplace_back(prefix, network.first);
        }
    }

    std::sort(Networks_.begin(), Networks_.end(), [] (auto&& lhs, auto&& rhs) {
        return lhs.first.GetMaskSize() > rhs.first.GetMaskSize();
    });

    const auto& dynamicConfigManager = bootstrap->GetDynamicConfigManager();
    dynamicConfigManager->SubscribeConfigChanged(BIND(&TApi::OnDynamicConfigChanged, MakeWeak(this)));
}

TString TApi::GetNetworkNameForAddress(const NNet::TNetworkAddress& address) const
{
    if (!address.IsIP6()) {
        return DefaultNetworkName_;
    }

    auto ip6Address = address.ToIP6Address();
    for (const auto& network : Networks_) {
        if (network.first.Contains(ip6Address)) {
            return network.second;
        }
    }

    return DefaultNetworkName_;
}

const NDriver::IDriverPtr& TApi::GetDriverV3() const
{
    return DriverV3_;
}

const NDriver::IDriverPtr& TApi::GetDriverV4() const
{
    return DriverV4_;
}

const TCompositeHttpAuthenticatorPtr& TApi::GetHttpAuthenticator() const
{
    return HttpAuthenticator_;
}

const TCoordinatorPtr& TApi::GetCoordinator() const
{
    return Coordinator_;
}

const TApiConfigPtr& TApi::GetConfig() const
{
    return Config_;
}

TApiDynamicConfigPtr TApi::GetDynamicConfig() const
{
    return DynamicConfig_.Acquire();
}

const IPollerPtr& TApi::GetPoller() const
{
    return Poller_;
}

bool TApi::IsUserBannedInCache(const TString& user)
{
    auto now = TInstant::Now();
    auto guard = ReaderGuard(BanCacheLock_);
    auto it = BanCache_.find(user);
    if (it != BanCache_.end()) {
        return now < it->second;
    }

    return false;
}

void TApi::PutUserIntoBanCache(const TString& user)
{
    auto guard = WriterGuard(BanCacheLock_);
    BanCache_[user] = TInstant::Now() + Config_->BanCacheExpirationTime;
}

TError TApi::CheckAccess(const TString& user)
{
    return AccessChecker_->CheckAccess(user);
}

int TApi::GetNumberOfConcurrentRequests()
{
    return GlobalSemaphore_.load();
}

std::optional<TSemaphoreGuard> TApi::AcquireSemaphore(const TString& user, const TString& command)
{
    auto value = GlobalSemaphore_.load();
    do {
        if (value >= Config_->ConcurrencyLimit * 2) {
            return {};
        }
    } while (!GlobalSemaphore_.compare_exchange_weak(value, value + 1));

    auto key = std::make_pair(user, command);
    auto counters = GetProfilingCounters(key);
    if (counters->LocalSemaphore >= Config_->ConcurrencyLimit) {
        GlobalSemaphore_.fetch_add(-1);
        return {};
    }

    counters->ConcurrencySemaphore.Update(++counters->LocalSemaphore);

    return TSemaphoreGuard(this, key);
}

void TApi::ReleaseSemaphore(const TUserCommandPair& key)
{
    auto counters = GetProfilingCounters(key);

    GlobalSemaphore_.fetch_add(-1);
    counters->ConcurrencySemaphore.Update(--counters->LocalSemaphore);
}

TApi::TProfilingCounters* TApi::GetProfilingCounters(const TUserCommandPair& key)
{
    return Counters_.FindOrInsert(key, [&, this] {
        auto profiler = SparseProfiler_
            .WithTag("user", key.first)
            .WithTag("command", key.second);

        auto counters = std::make_unique<TProfilingCounters>();
        counters->ConcurrencySemaphore = profiler.Gauge("/concurrency_semaphore");
        counters->RequestCount = profiler.Counter("/request_count");
        counters->RequestWallTime = profiler.Timer("/request_duration");
        counters->CumulativeRequestCpuTime = profiler.TimeCounter("/cumulative_request_cpu_time");
        return counters;
    }).first->get();
}

void TApi::IncrementHttpCode(EStatusCode httpStatusCode)
{
    auto counter = HttpCodes_.FindOrInsert(httpStatusCode, [&] {
        return HttpProxyProfiler
            .WithTag("http_code", ToString(static_cast<int>(httpStatusCode)))
            .Counter("/http_code_count");
    }).first;

    counter->Increment();
}

void TApi::IncrementProfilingCounters(
    const TString& user,
    const TString& command,
    std::optional<EStatusCode> httpStatusCode,
    TErrorCode apiErrorCode,
    TDuration wallTime,
    TDuration cpuTime,
    const NNet::TNetworkAddress& clientAddress,
    i64 bytesIn,
    i64 bytesOut,
    const std::optional<NFormats::TFormat>& inputFormat,
    const std::optional<NFormats::TFormat>& outputFormat,
    std::optional<TContentEncoding> inputCompression,
    std::optional<TContentEncoding> outputCompression)
{
    auto networkName = GetNetworkNameForAddress(clientAddress);

    auto* counters = GetProfilingCounters({user, command});

    counters->RequestCount.Increment();
    counters->RequestWallTime.Record(wallTime);
    counters->CumulativeRequestCpuTime.Add(cpuTime);

    if (httpStatusCode) {
        HttpCodesByCommand_.FindOrInsert({command, *httpStatusCode}, [&, this] {
            return SparseProfiler_
                .WithTag("http_code", ToString(static_cast<int>(*httpStatusCode)))
                .WithTag("command", command)
                .Counter("/command_http_code_count");
        }).first->Increment();

        HttpCodesByUser_.FindOrInsert({user, *httpStatusCode}, [&, this] {
            return SparseProfiler_
                .WithTag("http_code", ToString(static_cast<int>(*httpStatusCode)))
                .WithTag("user", user)
                .Counter("/user_http_code_count");
        }).first->Increment();
    }

    if (apiErrorCode) {
        counters->ApiErrors.FindOrInsert(apiErrorCode, [&, this] {
            return SparseProfiler_
                .WithTag("user", user)
                .WithTag("command", command)
                .WithTag("error_code", ToString(static_cast<int>(apiErrorCode)))
                .Counter("/api_error_count");
        }).first->Increment();
    }

    auto incrementUserCounter = [&, this] (auto& counterMap, auto counterName, auto tagName, auto tagValue, auto value) {
        counterMap.FindOrInsert(std::make_pair(user, networkName), [&, this] {
            return SparseProfiler_
                .WithTag("user", user)
                .WithTag(tagName, tagValue)
                .Counter(counterName);
        }).first->Increment(value);
    };

    incrementUserCounter(BytesIn_, "/bytes_in", "network", networkName, bytesIn);
    incrementUserCounter(BytesOut_, "/bytes_out", "network", networkName, bytesOut);

    if (inputFormat) {
        incrementUserCounter(InputFormatBytes_, "/bytes_in_by_format", "format", FormatEnum(inputFormat->GetType()), bytesIn);
    }
    if (outputFormat) {
        incrementUserCounter(OutputFormatBytes_, "/bytes_out_by_format", "format", FormatEnum(outputFormat->GetType()), bytesOut);
    }

    if (inputCompression) {
        incrementUserCounter(InputCompressionBytes_, "/bytes_in_by_compression", "compression", *inputCompression, bytesIn);
    }
    if (outputCompression) {
        incrementUserCounter(OutputCompressionBytes_, "/bytes_out_by_compression", "compression", *outputCompression, bytesOut);
    }
}

void TApi::HandleRequest(
    const IRequestPtr& req,
    const IResponseWriterPtr& rsp)
{
    auto context = New<TContext>(MakeStrong(this), req, rsp);
    try {
        if (!context->TryPrepare()) {
            PrepareErrorCount_.Increment();
            auto statusCode = rsp->GetStatus();
            if (statusCode) {
                IncrementHttpCode(*statusCode);
            }
            return;
        }

        context->FinishPrepare();
        context->Run();
    } catch (const std::exception& ex) {
        context->SetEnrichedError(TError(ex));
    }

    auto finally = Finally([&] {
        context->LogAndProfile();
    });
    context->Finalize();
}

void TApi::OnDynamicConfigChanged(
    const TProxyDynamicConfigPtr& /*oldConfig*/,
    const TProxyDynamicConfigPtr& newConfig)
{
    VERIFY_THREAD_AFFINITY_ANY();

    DynamicConfig_.Store(newConfig->Api);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
