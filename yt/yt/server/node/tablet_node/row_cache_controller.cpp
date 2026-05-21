#include "row_cache_controller.h"
#include "config.h"

#include <yt/yt/server/lib/tablet_node/private.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletNodeLogger;

static constexpr i64 MinReliableRowCount = 100;

////////////////////////////////////////////////////////////////////////////////

class TScaleToMemoryLimitStrategy
    : public IRowCacheControllerStrategy
{
public:
    explicit TScaleToMemoryLimitStrategy(TRowCacheControllerDynamicConfigPtr config)
        : Config_(std::move(config))
    { }

    TRowCacheControllerDecision Run(const TRowCacheControllerContext& context) override
    {
        if (!Config_->Enabled || !context.TotalMemoryLimit || *context.TotalMemoryLimit <= 0) {
            return {};
        }

        double newTotalCacheSize = 0;
        i64 totalGarbageAmount = 0;

        for (const auto& [_, tabletInfo] : context.Tablets) {
            if (tabletInfo.LookupCacheRowsRatio <= 0) {
                continue;
            }

            if (tabletInfo.TabletRowCount == 0) {
                newTotalCacheSize += tabletInfo.RowCacheDataWeight;
                continue;
            }

            double factor = 0;
            {
                double fillFactor = 0;
                if (tabletInfo.RowCacheCapacityRowCount > 0) {
                    fillFactor = static_cast<double>(tabletInfo.RowCachePreciseAliveRowCount) / tabletInfo.RowCacheCapacityRowCount;
                    fillFactor = std::clamp(fillFactor, 0.0, 1.0);
                }

                double reliabilityFactor = std::clamp(static_cast<double>(tabletInfo.RowCachePreciseAliveRowCount) / MinReliableRowCount, 0.0, 1.0);

                factor = std::pow(fillFactor, 0.1) * reliabilityFactor;
            }

            double averageCacheRowByteLength = 0;
            if (tabletInfo.RowCachePreciseAliveRowCount > 0) {
                averageCacheRowByteLength = static_cast<double>(tabletInfo.RowCachePreciseAliveByteSize) / tabletInfo.RowCachePreciseAliveRowCount;
            }

            double averageTabletRowByteLength = 0;
            if (tabletInfo.TabletRowCount > 0) {
                averageTabletRowByteLength = static_cast<double>(tabletInfo.TabletDataWeight) / tabletInfo.TabletRowCount;
            }

            double averageRowByteLength = averageCacheRowByteLength * factor + averageTabletRowByteLength * (1 - factor);

            newTotalCacheSize += tabletInfo.LookupCacheRowsRatio * tabletInfo.TabletRowCount * averageRowByteLength;

            i64 garbageAmount = std::max<i64>(0, tabletInfo.RowCacheDataWeight - tabletInfo.RowCachePreciseAliveByteSize);
            totalGarbageAmount += garbageAmount;
        }

        if (newTotalCacheSize <= 0) {
            return {};
        }

        i64 memoryLimitWithoutGarbage = 0;
        {
            i64 memoryGap = std::max(
                Config_->MemoryLimitGapInBytes,
                static_cast<i64>(Config_->MemoryLimitGapFraction * *context.TotalMemoryLimit));

            i64 effectiveMemoryLimit = *context.TotalMemoryLimit - memoryGap;

            i64 maxGarbagePenalty = static_cast<i64>(Config_->MaxGarbagePenaltyFraction * effectiveMemoryLimit);
            i64 garbagePenalty = std::min(totalGarbageAmount, maxGarbagePenalty);

            memoryLimitWithoutGarbage = std::max<i64>(0, effectiveMemoryLimit - garbagePenalty);
        }

        double rawScaleFactor = static_cast<double>(memoryLimitWithoutGarbage) / newTotalCacheSize;
        double clampedScaleFactor = std::clamp(rawScaleFactor, 0.0, 1.0);

        double alpha = Config_->ScaleFactorSmoothingAlpha;
        if (SmoothedScaleFactor_) {
            SmoothedScaleFactor_ = alpha * clampedScaleFactor + (1 - alpha) * *SmoothedScaleFactor_;
        } else {
            SmoothedScaleFactor_ = clampedScaleFactor;
        }

        double categoryMemoryLimitScaleFactor = std::clamp(*SmoothedScaleFactor_, 0.0, 1.0);

        YT_LOG_DEBUG("Finished row controller iteration via TScaleToMemoryLimitStrategy "
            "(TotalMemoryLimit: %v, TotalGarbageAmount: %v, MemoryLimitWithoutGarbage: %v, NewTotalCacheSize: %v, "
            "RawScaleFactor: %v, ClampedScaleFactor: %v, CategoryMemoryLimitScaleFactor: %v)",
            *context.TotalMemoryLimit,
            totalGarbageAmount,
            memoryLimitWithoutGarbage,
            newTotalCacheSize,
            rawScaleFactor,
            clampedScaleFactor,
            categoryMemoryLimitScaleFactor);

        return TRowCacheControllerDecision{.CategoryMemoryLimitScaleFactor=categoryMemoryLimitScaleFactor};
    }

private:
    TRowCacheControllerDynamicConfigPtr Config_;
    std::optional<double> SmoothedScaleFactor_;
};

std::unique_ptr<IRowCacheControllerStrategy> CreateComputeAvgRowSizeAndScaleLookupCacheRowsRatioToMemoryLimitStrategy(
    TRowCacheControllerDynamicConfigPtr config)
{
    return std::make_unique<TScaleToMemoryLimitStrategy>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

TRowCacheController::TRowCacheController(TRowCacheControllerDynamicConfigPtr config)
    : Strategy_(CreateComputeAvgRowSizeAndScaleLookupCacheRowsRatioToMemoryLimitStrategy(std::move(config)))
{ }

void TRowCacheController::RunIteration(const TRowCacheControllerContext& context)
{
    LastContext_ = context;
    LastDecision_ = Strategy_->Run(LastContext_);
}

double TRowCacheController::GetCategoryMemoryLimitScaleFactor() const
{
    return LastDecision_.CategoryMemoryLimitScaleFactor.value_or(1.0);
}

void TRowCacheController::Reconfigure(const TRowCacheControllerDynamicConfigPtr& config)
{
    Strategy_ = CreateComputeAvgRowSizeAndScaleLookupCacheRowsRatioToMemoryLimitStrategy(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
