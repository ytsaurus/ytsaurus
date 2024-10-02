#include "request_tracker.h"

#include "config.h"
#include "private.h"

#include <yt/yt/core/concurrency/config.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NOrm::NServer::NAccessControl {

using namespace NConcurrency;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TUserThrottlers
{
    IReconfigurableThroughputThrottlerPtr ThroughputThrottler;
    TThroughputThrottlerConfigPtr ThrottlerConfig;
    std::atomic<int> RequestQueueSizeLimit = 0;
    std::atomic<int> RequestQueueSize = 0;
    const NProfiling::TCounter ThrottledCounter;

    TUserThrottlers(
        IReconfigurableThroughputThrottlerPtr throttler,
        TThroughputThrottlerConfigPtr config,
        int queueSizeLimit,
        const NProfiling::TProfiler& profiler)
        : ThroughputThrottler(std::move(throttler))
        , ThrottlerConfig(std::move(config))
        , RequestQueueSizeLimit(queueSizeLimit)
        , ThrottledCounter(profiler.Counter("/throttled"))
    { }
};

struct TUserLimits
{
    i64 RequestWeightRateLimit = 0;
    int RequestQueueSizeLimit = 0;
};

TThroughputThrottlerConfigPtr CreateThroughputThrottlerConfig(i64 requestWeightRateLimit)
{
    // NB: Cast 0 -> nullopt to get unlimited throttler.
    return TThroughputThrottlerConfig::Create(requestWeightRateLimit
        ? std::optional<i64>(requestWeightRateLimit)
        : std::nullopt);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TRequestTracker::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(TRequestTrackerConfigPtr config)
        : Config_(std::move(config))
    { }

    ~TImpl() = default;

    void ReconfigureUsersBatch(const std::vector<TRequestTracker::TUserSetup>& update)
    {
        if (update.empty() || !Config_->Enabled) {
            return;
        }

        auto guard = WriterGuard(RatesMapLock_);
        int processedSubjects = 0;
        for (const auto& [userId, requestWeightRateLimit, requestQueueSizeLimit] : update) {
            // NB: Sanitize limits.
            auto limits = TUserLimits {
                .RequestWeightRateLimit = std::max<i64>(0, requestWeightRateLimit),
                .RequestQueueSizeLimit = std::max(0, requestQueueSizeLimit),
            };

            UpdateUserThrottlers(userId, limits);
            UsersLimits_[userId] = std::move(limits);

            ++processedSubjects;
            if (processedSubjects % Config_->ReconfigureBatchSize == 0) {
                // NB: Let readers process requests.
                auto unguard = Unguard(guard);
            }
        }
        YT_LOG_INFO("Reconfigured user request throttlers (Count: %v)", update.size());
    }

    TFuture<void> ThrottleUserRequest(std::string_view userId, i64 requestWeight, bool soft)
    {
        if (!Config_->Enabled) {
            return VoidFuture;
        }

        auto guard = ReaderGuard(RatesMapLock_);
        const auto& userThrottlers = GetUserThrottlersOrThrow(userId, guard);
        if (CanRequestBeThrottled(userThrottlers, requestWeight, soft)) {
            return userThrottlers.ThroughputThrottler->Throttle(requestWeight);
        } else {
            userThrottlers.ThroughputThrottler->Acquire(requestWeight);
            return VoidFuture;
        }
    }

    bool TryIncreaseRequestQueueSize(std::string_view userId, int requestCount, bool soft)
    {
        if (!Config_->Enabled) {
            return true;
        }

        auto guard = ReaderGuard(RatesMapLock_);
        auto& userThrottlers = GetUserThrottlersOrThrow(userId, guard);

        // NB: Fast and flaky solution: pending fetch_sub's in fail path might block valid requests.
        int old = userThrottlers.RequestQueueSize.fetch_add(requestCount);

        int sizeLimit = userThrottlers.RequestQueueSizeLimit.load();
        if (sizeLimit == 0) {
            // Unlimited.
            return true;
        }

        auto overdraftSizeLimit = soft
            ? Config_->QueueSizeOverdraftMultiplier * sizeLimit
            : sizeLimit;
        if (Y_UNLIKELY(old >= overdraftSizeLimit)) {
            userThrottlers.RequestQueueSize.fetch_sub(requestCount);
            userThrottlers.ThrottledCounter.Increment();
            return false;
        } else {
            return true;
        }
    }

    void DecreaseRequestQueueSize(std::string_view userId, int requestCount)
    {
        if (!Config_->Enabled) {
            return;
        }

        auto guard = ReaderGuard(RatesMapLock_);
        auto& userThrottlers = GetUserThrottlersOrThrow(userId, guard);
        userThrottlers.RequestQueueSize.fetch_sub(requestCount);
    }

private:
    const TRequestTrackerConfigPtr Config_;

    THashMap<NObjects::TObjectId, TUserThrottlers, THash<TString>, TEqualTo<>> RatesMap_;
    THashMap<NObjects::TObjectId, TUserLimits, THash<TString>, TEqualTo<>> UsersLimits_;
    TReaderWriterSpinLock RatesMapLock_;

    const NProfiling::TProfiler Profiler_ =  Profiler.WithPrefix("/request_tracker");
    const NProfiling::TCounter UserConfigureCount_ = Profiler_.Counter("/configure");
    const NProfiling::TCounter UserReconfigureCount_ = Profiler_.Counter("/reconfigure");

private:
    TUserThrottlers& GetUserThrottlersOrThrow(
        std::string_view userId,
        TReaderGuard<TReaderWriterSpinLock>& readerGuard)
    {
        auto it = RatesMap_.find(userId);
        if (it != RatesMap_.end()) {
            return it->second;
        }

        CreateUserThrottlers(userId, readerGuard);
        return GetOrCrash(RatesMap_, userId);
    }

    void CreateUserThrottlers(
        std::string_view userId,
        TReaderGuard<TReaderWriterSpinLock>& readerGuard)
    {
        auto readerUnguard = Unguard(readerGuard);
        auto writerGuard = WriterGuard(RatesMapLock_);

        if (RatesMap_.contains(userId)) {
            return;
        }

        // TODO(babenko(): migrate to std::string
        auto userProfiler = Profiler_.WithTag("user", std::string(userId)).WithSparse();
        auto limits = GetUserLimitsOrThrow(userId);
        auto throttlerConfig = CreateThroughputThrottlerConfig(limits.RequestWeightRateLimit);
        auto throttler = CreateReconfigurableThroughputThrottler(
            throttlerConfig,
            Logger().WithTag("UserId: %v", ToString(userId)),
            userProfiler.WithPrefix("/weight"));

        YT_LOG_DEBUG("Initialized user request throttlers ("
            "UserId: %v, "
            "RequestQueueSizeLimit: %v, "
            "RequestWeightRateLimit: %v)",
            userId,
            limits.RequestQueueSizeLimit,
            throttlerConfig->Limit);

        YT_VERIFY(RatesMap_.try_emplace(
            userId,
            std::move(throttler),
            std::move(throttlerConfig),
            limits.RequestQueueSizeLimit,
            userProfiler).second);

        UserConfigureCount_.Increment();
    }

    void UpdateUserThrottlers(
        std::string_view userId,
        const TUserLimits& limits)
    {
        auto it = RatesMap_.find(userId);
        if (it == RatesMap_.end()) {
            return;
        }

        auto throttlerConfig = CreateThroughputThrottlerConfig(limits.RequestWeightRateLimit);

        YT_LOG_DEBUG("Updated user request throttlers ("
            "UserId: %v, "
            "RequestQueueSizeLimit: %v, "
            "RequestWeightRateLimit: %v)",
            userId,
            limits.RequestQueueSizeLimit,
            throttlerConfig->Limit);
        it->second.ThroughputThrottler->Reconfigure(throttlerConfig);
        it->second.ThrottlerConfig = std::move(throttlerConfig);
        it->second.RequestQueueSizeLimit.store(limits.RequestQueueSizeLimit);
        UserReconfigureCount_.Increment();
    }

    bool CanRequestBeThrottled(const TUserThrottlers& userThrottlers, i64 requestWeight, bool soft) const
    {
        if (!soft) {
            return true;
        }

        auto maxAvailable = userThrottlers.ThrottlerConfig->GetMaxAvailable();
        if (!maxAvailable.has_value()) {
            return false;
        }

        auto availableAfterAccounting = userThrottlers.ThroughputThrottler->GetAvailable() - requestWeight;
        auto maxDebt = -1 * Config_->MaxThrottlerDebtMultiplier * *maxAvailable;

        return availableAfterAccounting < maxDebt;
    }

    TUserLimits GetUserLimitsOrThrow(std::string_view userId)
    {
        auto it = UsersLimits_.find(userId);
        if (it == UsersLimits_.end()) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Request throttlers for user %Qv are not configured",
                userId);
        }
        return it->second;
    }
};

////////////////////////////////////////////////////////////////////////////////

TRequestTracker::TUserSetup::TUserSetup(
    NObjects::TObjectId userId,
    i64 requestWeightRateLimit,
    int requestQueueSizeLimit)
    : UserId(std::move(userId))
    , RequestWeightRateLimit(requestWeightRateLimit)
    , RequestQueueSizeLimit(requestQueueSizeLimit)
{ }

TRequestTracker::TRequestTracker(TRequestTrackerConfigPtr config)
    : Impl_(New<TImpl>(std::move(config)))
{ }

TRequestTracker::~TRequestTracker() = default;

TFuture<void> TRequestTracker::ThrottleUserRequest(
    std::string_view userId,
    i64 requestWeight,
    bool soft)
{
    return Impl_->ThrottleUserRequest(userId, requestWeight, soft);
}

void TRequestTracker::ReconfigureUsersBatch(const std::vector<TUserSetup>& update)
{
    Impl_->ReconfigureUsersBatch(update);
}

bool TRequestTracker::TryIncreaseRequestQueueSize(
    std::string_view userId,
    int requestCount,
    bool soft)
{
    return Impl_->TryIncreaseRequestQueueSize(userId, requestCount, soft);
}

void TRequestTracker::DecreaseRequestQueueSize(std::string_view userId, int requestCount)
{
    Impl_->DecreaseRequestQueueSize(userId, requestCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl
