#include "request_tracker.h"

#include "config.h"
#include "private.h"

#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/profiler.h>

namespace NYP::NServer::NAccessControl {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TUserThrottlers
{
    IReconfigurableThroughputThrottlerPtr ThroughputThrottler;
    std::atomic<int> RequestQueueSizeLimit = {0};
    std::atomic<int> RequestQueueSize = {0};
    NProfiling::TProfiler UserProfiler;
    NProfiling::TMonotonicCounter ThrottledCounter;

    TUserThrottlers(
        IReconfigurableThroughputThrottlerPtr throttler,
        int queueSizeLimit,
        NProfiling::TProfiler profiler)
        : ThroughputThrottler(std::move(throttler))
        , RequestQueueSizeLimit(queueSizeLimit)
        , UserProfiler(std::move(profiler))
        , ThrottledCounter("/throttled")
    { }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TRequestTracker::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(TRequestTrackerConfigPtr config)
        : Config_(std::move(config))
        , UserConfigureCount_("/configure")
        , UserReConfigureCount_("/reconfigure")
    { }

    ~TImpl() = default;

    void ReconfigureUsersBatch(const std::vector<TRequestTracker::TUserSetup>& update)
    {
        if (update.empty() || !Config_->Enabled) {
            return;
        }

        TWriterGuard guard(RatesMapLock_);
        int processedSubjects = 0;
        for (const auto& [userId, requestWeightRateLimit, requestQueueSizeLimit] : update) {
            // NB: Sanitize limits
            auto saneRequestWeightRateLimit = std::max<i64>(0, requestWeightRateLimit);
            auto saneRequestQueueSizeLimit = std::max(0, requestQueueSizeLimit);

            // NB: Cast 0 -> nullopt to get unlimited throttler
            auto newConfig = New<TThroughputThrottlerConfig>(
                saneRequestWeightRateLimit ? std::optional<i64>(saneRequestWeightRateLimit) : std::nullopt);
            DoReconfigureUser(userId, std::move(newConfig), saneRequestQueueSizeLimit);

            ++processedSubjects;
            if (processedSubjects % Config_->ReconfigureBatchSize == 0) {
                // NB: Let readers process requests
                auto unguard = Unguard(guard);
            }
        }
    }

    TFuture<void> ThrottleUserRequest(const NObjects::TObjectId& userId, i64 requestCount)
    {
        if (!Config_->Enabled) {
            return VoidFuture;
        }

        TReaderGuard guard(RatesMapLock_);
        auto& userThrottlers = GetUserThrottlersOrThrow(userId);
        return userThrottlers.ThroughputThrottler->Throttle(requestCount);
    }

    bool TryIncreaseRequestQueueSize(const NObjects::TObjectId& userId, int requestCount)
    {
        if (!Config_->Enabled) {
            return true;
        }

        TReaderGuard guard(RatesMapLock_);
        auto& userThrottlers = GetUserThrottlersOrThrow(userId);

        // NB: Fast and flaky solution: pending fetch_sub's in fail path might block valid requests
        int old = userThrottlers.RequestQueueSize.fetch_add(requestCount);

        int sizeLimit = userThrottlers.RequestQueueSizeLimit.load();
        if (sizeLimit == 0) {
            // unlimited
            return true;
        }

        if (Y_UNLIKELY(old >= sizeLimit)) {
            userThrottlers.RequestQueueSize.fetch_sub(requestCount);
            userThrottlers.UserProfiler.Increment(userThrottlers.ThrottledCounter);
            return false;
        } else {
            return true;
        }
    }

    void DecreaseRequestQueueSize(const NObjects::TObjectId& userId, int requestCount)
    {
        if (!Config_->Enabled) {
            return;
        }

        TReaderGuard guard(RatesMapLock_);
        auto& userThrottlers = GetUserThrottlersOrThrow(userId);
        userThrottlers.RequestQueueSize.fetch_sub(requestCount);
    }

private:
    const TRequestTrackerConfigPtr Config_;
    const NYT::NProfiling::TProfiler Profiler_ =  Profiler.AppendPath("/request_tracker");

    THashMap<NObjects::TObjectId, TUserThrottlers> RatesMap_;
    TReaderWriterSpinLock RatesMapLock_;
    NProfiling::TMonotonicCounter UserConfigureCount_;
    NProfiling::TMonotonicCounter UserReConfigureCount_;

private:
    TUserThrottlers& GetUserThrottlersOrThrow(const NObjects::TObjectId& userId) {
        auto it = RatesMap_.find(userId);
        if (it == RatesMap_.end()) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Request throttlers for user %Qv are not configured",
                userId);
        }
        return it->second;
    }

    void DoReconfigureUser(
        const NObjects::TObjectId& userId,
        TThroughputThrottlerConfigPtr throttlerConfig,
        int requestQueueSizeLimit)
    {
        auto it = RatesMap_.find(userId);
        if (it == RatesMap_.end()) {
            NProfiling::TTagIdList tagIds{NProfiling::TProfileManager::Get()->RegisterTag("user", userId)};
            auto userProfiler = Profiler_.AddTags(tagIds);

            auto throttler = CreateReconfigurableThroughputThrottler(
                std::move(throttlerConfig),
                {},
                userProfiler.AppendPath("/weight"));

            YT_VERIFY(RatesMap_.try_emplace(
                userId,
                std::move(throttler),
                requestQueueSizeLimit,
                std::move(userProfiler)).second);
            Profiler_.Increment(UserConfigureCount_);
        } else {
            it->second.ThroughputThrottler->Reconfigure(std::move(throttlerConfig));
            it->second.RequestQueueSizeLimit.store(requestQueueSizeLimit);
            Profiler_.Increment(UserReConfigureCount_);
        }
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

TFuture<void> TRequestTracker::ThrottleUserRequest(const NObjects::TObjectId& userId, i64 requestCount)
{
    return Impl_->ThrottleUserRequest(userId, requestCount);
}

void TRequestTracker::ReconfigureUsersBatch(const std::vector<TUserSetup>& update)
{
    Impl_->ReconfigureUsersBatch(update);
}

bool TRequestTracker::TryIncreaseRequestQueueSize(const NObjects::TObjectId& userId, int requestCount)
{
    return Impl_->TryIncreaseRequestQueueSize(userId, requestCount);
}

void TRequestTracker::DecreaseRequestQueueSize(const NObjects::TObjectId& userId, int requestCount)
{
    Impl_->DecreaseRequestQueueSize(userId, requestCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NAccessControl
