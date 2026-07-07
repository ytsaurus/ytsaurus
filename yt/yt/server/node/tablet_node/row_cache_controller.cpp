#include "row_cache_controller.h"
#include "bootstrap.h"
#include "config.h"
#include "row_cache.h"
#include "tablet_slot.h"

#include <yt/yt/server/lib/cellar_agent/cellar.h>
#include <yt/yt/server/lib/cellar_agent/cellar_manager.h>
#include <yt/yt/server/lib/cellar_agent/occupant.h>

#include <yt/yt/server/lib/tablet_node/private.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NTabletNode {

using namespace NCellarAgent;
using namespace NCellarClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(IRowCacheControllerStrategy)

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletNodeLogger;

static constexpr i64 MinReliableRowCount = 100;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TRowCacheBriefStatistics
{
    i64 DataWeight = 0;
    i64 CapacityRowCount = 0;
    i64 AliveBytes = 0;
    i64 AliveRowCount = 0;
};

std::optional<TRowCacheBriefStatistics> EvaluateRowCacheStatistics(const TRowCacheTabletInfo& tabletInfo)
{
    if (tabletInfo.LookupCacheRowsRatio <= 0) {
        return std::nullopt;
    }

    if (!tabletInfo.RowCache) {
        return std::nullopt;
    }

    const auto& rowCache = tabletInfo.RowCache;

    return TRowCacheBriefStatistics{
        .DataWeight = rowCache->GetUsedBytesCount(),
        .CapacityRowCount = static_cast<i64>(rowCache->GetCache()->GetCapacity()),
        .AliveBytes = rowCache->GetAliveByteSize(),
        .AliveRowCount = rowCache->GetAliveItemCount(),
    };
}

struct TTabletRotationInfo
{
    const TRowCacheTabletInfo* TabletInfo;
    i64 TargetDataWeightWithoutScaleFactor = 0;
    i64 AliveBytes = 0;
};

struct TCollectedStatistics
{
    double NewTotalCacheSize = 0;
    i64 TotalGarbageAmount = 0;
    std::vector<TTabletRotationInfo> TabletRotationInfos;
};

TCollectedStatistics CollectStatistics(const TRowCacheControllerContext& context)
{
    TCollectedStatistics result;

    for (const auto& [_, tabletInfo] : context.Tablets) {
        auto statistics = EvaluateRowCacheStatistics(tabletInfo);
        if (!statistics) {
            continue;
        }

        if (tabletInfo.TabletRowCount == 0) {
            result.NewTotalCacheSize += statistics->DataWeight;
            result.TabletRotationInfos.push_back({
                .TabletInfo = &tabletInfo,
                .TargetDataWeightWithoutScaleFactor = statistics->DataWeight,
            });
            continue;
        }

        double fillFactor = 0;
        if (statistics->CapacityRowCount > 0) {
            fillFactor = std::clamp(
                static_cast<double>(statistics->AliveRowCount) / statistics->CapacityRowCount,
                0.0,
                1.0);
        }

        double reliabilityFactor = std::clamp(
            static_cast<double>(statistics->AliveRowCount) / MinReliableRowCount,
            0.0,
            1.0);

        double factor = std::pow(fillFactor, 0.1) * reliabilityFactor;

        double averageCacheRowByteLength = 0;
        if (statistics->AliveRowCount > 0) {
            averageCacheRowByteLength = static_cast<double>(statistics->AliveBytes) / statistics->AliveRowCount;
        }

        double averageTabletRowByteLength = static_cast<double>(tabletInfo.TabletDataWeight) / tabletInfo.TabletRowCount;

        double averageRowByteLength = averageCacheRowByteLength * factor + averageTabletRowByteLength * (1 - factor);

        double perTabletTargetSize = tabletInfo.LookupCacheRowsRatio * tabletInfo.TabletRowCount * averageRowByteLength;

        result.NewTotalCacheSize += perTabletTargetSize;
        result.TotalGarbageAmount += std::max<i64>(0, statistics->DataWeight - statistics->AliveBytes);

        result.TabletRotationInfos.push_back({
            .TabletInfo = &tabletInfo,
            .TargetDataWeightWithoutScaleFactor = static_cast<i64>(perTabletTargetSize),
        });
    }

    return result;
}

i64 RefreshAliveBytes(std::vector<TTabletRotationInfo>& rotationInfos)
{
    i64 totalAliveBytes = 0;
    for (auto& rotationInfo : rotationInfos) {
        rotationInfo.AliveBytes = rotationInfo.TabletInfo->RowCache->GetAliveByteSize();
        totalAliveBytes += rotationInfo.AliveBytes;
    }
    return totalAliveBytes;
}

i64 RotatePass(
    const std::vector<TTabletRotationInfo>& rotationInfos,
    double categoryMemoryLimitScaleFactor)
{
    i64 rotatedCount = 0;
    for (const auto& rotationInfo : rotationInfos) {
        i64 threshold = static_cast<i64>(rotationInfo.TargetDataWeightWithoutScaleFactor * categoryMemoryLimitScaleFactor);
        if (rotationInfo.AliveBytes > threshold) {
            rotationInfo.TabletInfo->RowCache->ForceRotate();
            ++rotatedCount;
        }
    }
    return rotatedCount;
}

i64 ComputeMemoryLimitWithoutGarbage(
    const TRowCacheControllerDynamicConfigPtr& config,
    i64 totalMemoryLimit,
    i64 totalGarbageAmount)
{
    i64 memoryGap = std::max(
        config->MemoryLimitGapInBytes,
        static_cast<i64>(config->MemoryLimitGapFraction * totalMemoryLimit));

    i64 effectiveMemoryLimit = totalMemoryLimit - memoryGap;

    return std::max<i64>(0, effectiveMemoryLimit - totalGarbageAmount);
}

void RunRotations(
    std::vector<TTabletRotationInfo>* rotationInfos,
    double categoryMemoryLimitScaleFactor,
    i64 totalAliveBytesRotationThreshold)
{
    TDuration firstPassElapsed;
    i64 firstPassTotalAliveBytes;
    i64 firstPassRotated = 0;
    {
        NProfiling::TValueIncrementingTimingGuard<NProfiling::TWallTimer> timingGuard(&firstPassElapsed);
        firstPassTotalAliveBytes = RefreshAliveBytes(*rotationInfos);
        if (firstPassTotalAliveBytes > totalAliveBytesRotationThreshold) {
            firstPassRotated = RotatePass(
                *rotationInfos,
                categoryMemoryLimitScaleFactor);
        }
    }

    TDuration secondPassElapsed;
    i64 secondPassTotalAliveBytes;
    i64 secondPassRotated = 0;
    {
        NProfiling::TValueIncrementingTimingGuard<NProfiling::TWallTimer> timingGuard(&secondPassElapsed);
        secondPassTotalAliveBytes = RefreshAliveBytes(*rotationInfos);
        if (secondPassTotalAliveBytes > totalAliveBytesRotationThreshold) {
            secondPassRotated = RotatePass(
                *rotationInfos,
                categoryMemoryLimitScaleFactor);
        }
    }

    YT_LOG_DEBUG("Rotated row caches scaled to memory limit "
        "(CategoryMemoryLimitScaleFactor: %v, TotalAliveBytesRotationThreshold: %v, "
        "FirstPassTotalAliveBytes: %v, FirstPassRotatedTabletCount: %v, FirstPassElapsedTime: %v, "
        "SecondPassTotalAliveBytes: %v, SecondPassRotatedTabletCount: %v, SecondPassElapsedTime: %v)",
        categoryMemoryLimitScaleFactor,
        totalAliveBytesRotationThreshold,
        firstPassTotalAliveBytes,
        firstPassRotated,
        firstPassElapsed,
        secondPassTotalAliveBytes,
        secondPassRotated,
        secondPassElapsed);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TScaleToMemoryLimitStrategy
    : public IRowCacheControllerStrategy
{
public:
    explicit TScaleToMemoryLimitStrategy(TRowCacheControllerDynamicConfigPtr config)
        : Config_(std::move(config))
    { }

    TRowCacheControllerDecision Run(const TRowCacheControllerContext& context, IInvokerPtr invokerToPerformRotation) override
    {
        if (!Config_->Enabled || !context.TotalMemoryLimit || *context.TotalMemoryLimit <= 0) {
            return {};
        }

        TDuration elapsedTime;
        TRowCacheControllerDecision decision;
        {
            NProfiling::TValueIncrementingTimingGuard<NProfiling::TWallTimer> timingGuard(&elapsedTime);
            decision = DoRun(context, invokerToPerformRotation);
        }

        YT_LOG_DEBUG("Finished row controller iteration via TScaleToMemoryLimitStrategy (ElapsedTime: %v)",
            elapsedTime);

        return decision;
    }

private:
    TRowCacheControllerDecision DoRun(const TRowCacheControllerContext& context, const IInvokerPtr& invokerToPerformRotation)
    {
        auto statistics = CollectStatistics(context);
        if (statistics.NewTotalCacheSize <= 0) {
            return {};
        }

        i64 memoryLimitWithoutGarbage = ComputeMemoryLimitWithoutGarbage(
            Config_,
            *context.TotalMemoryLimit,
            statistics.TotalGarbageAmount);

        double rawScaleFactor = static_cast<double>(memoryLimitWithoutGarbage) / statistics.NewTotalCacheSize;
        double categoryMemoryLimitScaleFactor = std::clamp(rawScaleFactor, 0.0, 1.0);

        YT_LOG_DEBUG("Computed row cache memory scale factor "
            "(TotalMemoryLimit: %v, TotalGarbageAmount: %v, MemoryLimitWithoutGarbage: %v, NewTotalCacheSize: %v, "
            "RawScaleFactor: %v, CategoryMemoryLimitScaleFactor: %v)",
            *context.TotalMemoryLimit,
            statistics.TotalGarbageAmount,
            memoryLimitWithoutGarbage,
            statistics.NewTotalCacheSize,
            rawScaleFactor,
            categoryMemoryLimitScaleFactor);

        i64 totalAliveBytesRotationThreshold = static_cast<i64>(
            *context.TotalMemoryLimit * Config_->RotationMemoryThreshold);

        WaitFor(
            BIND(&RunRotations,
                &statistics.TabletRotationInfos,
                categoryMemoryLimitScaleFactor,
                totalAliveBytesRotationThreshold)
            .AsyncVia(invokerToPerformRotation)
            .Run())
            .ThrowOnError();

        return TRowCacheControllerDecision{.CategoryMemoryLimitScaleFactor=categoryMemoryLimitScaleFactor};
    }

    TRowCacheControllerDynamicConfigPtr Config_;
};

DEFINE_REFCOUNTED_TYPE(TScaleToMemoryLimitStrategy)

IRowCacheControllerStrategyPtr CreateDefaultRowCacheControllerStrategy(
    TRowCacheControllerDynamicConfigPtr config)
{
    return New<TScaleToMemoryLimitStrategy>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

TRowCacheController::TRowCacheController(
    TRowCacheControllerDynamicConfigPtr config,
    IBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , Executor_(New<TPeriodicExecutor>(
        bootstrap->GetControlInvoker(),
        BIND(&TRowCacheController::Adjust, MakeWeak(this)),
        config->Period))
    , Strategy_(CreateDefaultRowCacheControllerStrategy(std::move(config)))
{ }

void TRowCacheController::Start()
{
    Executor_->Start();
}

double TRowCacheController::GetCategoryMemoryLimitScaleFactor() const
{
    return LastCategoryMemoryLimitScaleFactor_;
}

void TRowCacheController::Reconfigure(const TRowCacheControllerDynamicConfigPtr& config)
{
    Executor_->SetPeriod(config->Period);
    Strategy_ = CreateDefaultRowCacheControllerStrategy(config);
}

void TRowCacheController::Adjust()
{
    auto dynamicConfig = Bootstrap_->GetTabletNodeDynamicConfig();
    if (!dynamicConfig || !dynamicConfig->RowCacheController->Enabled) {
        LastCategoryMemoryLimitScaleFactor_ = 1.0;
        return;
    }

    TDuration elapsedTime;
    {
        NProfiling::TValueIncrementingTimingGuard<NProfiling::TWallTimer> timingGuard(&elapsedTime);

        std::vector<TFuture<TRowCacheControllerContext>> futures;
        {
            const auto& occupants = Bootstrap_->GetCellarManager()
                ->GetCellar(ECellarType::Tablet)
                ->Occupants();

            for (const auto& occupant : occupants) {
                if (!occupant) {
                    continue;
                }

                if (auto occupier = occupant->GetTypedOccupier<ITabletSlot>()) {
                    futures.push_back(occupier->GetRowCacheControllerContext());
                }
            }
        }

        TRowCacheControllerContext context;
        {
            auto perCellContexts = WaitFor(AllSucceeded(futures));
            if (!perCellContexts.IsOK()) {
                return;
            }

            for (auto& perCellContext : perCellContexts.Value()) {
                context.Tablets.insert(
                    perCellContext.Tablets.begin(),
                    perCellContext.Tablets.end());
            }

            context.TotalMemoryLimit = Bootstrap_->GetNodeMemoryUsageTracker()->GetLimit(EMemoryCategory::LookupRowsCache);
        }

        auto strategy = Strategy_;
        auto queryInvoker = Bootstrap_->GetQueryPoolInvoker("RowCacheController", "RowCacheController");
        auto decision = strategy->Run(context, queryInvoker);
        LastCategoryMemoryLimitScaleFactor_ = decision.CategoryMemoryLimitScaleFactor;
    }

    YT_LOG_DEBUG("Finished RowCacheControllerUpdate (TimeSpent: %v)", elapsedTime);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
