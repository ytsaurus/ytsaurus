#pragma once

#include "public.h"

#include <memory>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TRowCacheTabletInfo
{
    i64 TabletDataWeight = 0;
    i64 TabletRowCount = 0;
    i64 RowCacheDataWeight = 0;
    i64 RowCacheCapacityRowCount = 0;
    i64 RowCacheSizeRowCount = 0;
    double LookupCacheRowsRatio = 0;
    i64 RowCachePreciseAliveByteSize = 0;
    i64 RowCachePreciseAliveRowCount = 0;
};

struct TRowCacheControllerContext
{
    THashMap<TTabletId, TRowCacheTabletInfo> Tablets;
    std::optional<i64> TotalMemoryLimit;
};

struct TRowCacheControllerDecision
{
    std::optional<double> CategoryMemoryLimitScaleFactor;
};

struct IRowCacheControllerStrategy
{
    virtual ~IRowCacheControllerStrategy() = default;

    virtual TRowCacheControllerDecision Run(const TRowCacheControllerContext& context) = 0;
};

std::unique_ptr<IRowCacheControllerStrategy> CreateComputeAvgRowSizeAndScaleLookupCacheRowsRatioToMemoryLimitStrategy(
    TRowCacheControllerDynamicConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

class TRowCacheController
    : public TRefCounted
{
public:
    explicit TRowCacheController(TRowCacheControllerDynamicConfigPtr config);

    void RunIteration(const TRowCacheControllerContext& context);

    double GetCategoryMemoryLimitScaleFactor() const;

    void Reconfigure(const TRowCacheControllerDynamicConfigPtr& config);

private:
    std::unique_ptr<IRowCacheControllerStrategy> Strategy_;

    TRowCacheControllerContext LastContext_;
    TRowCacheControllerDecision LastDecision_;
};

DEFINE_REFCOUNTED_TYPE(TRowCacheController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
