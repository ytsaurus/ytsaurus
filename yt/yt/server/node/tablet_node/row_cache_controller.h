#pragma once

#include "public.h"

#include <yt/yt/core/actions/invoker.h>

#include <atomic>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TRowCacheTabletInfo
{
    TRowCachePtr RowCache;
    i64 TabletDataWeight = 0;
    i64 TabletRowCount = 0;
    double LookupCacheRowsRatio = 0;
};

struct TRowCacheControllerContext
{
    THashMap<TTabletId, TRowCacheTabletInfo> Tablets;
    std::optional<i64> TotalMemoryLimit;
};

struct TRowCacheControllerDecision
{
    double CategoryMemoryLimitScaleFactor = 1.0;
};

struct IRowCacheControllerStrategy
    : public TRefCounted
{
    virtual TRowCacheControllerDecision Run(const TRowCacheControllerContext& context, IInvokerPtr invokerToPerformRotation) = 0;
};

DECLARE_REFCOUNTED_STRUCT(IRowCacheControllerStrategy)

IRowCacheControllerStrategyPtr CreateDefaultRowCacheControllerStrategy(
    TRowCacheControllerDynamicConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

class TRowCacheController
    : public TRefCounted
{
public:
    TRowCacheController(
        TRowCacheControllerDynamicConfigPtr config,
        IBootstrap* bootstrap);

    void Start();

    double GetCategoryMemoryLimitScaleFactor() const;

    void Reconfigure(const TRowCacheControllerDynamicConfigPtr& config);

private:
    IBootstrap* const Bootstrap_;

    NConcurrency::TPeriodicExecutorPtr Executor_;

    IRowCacheControllerStrategyPtr Strategy_;

    std::atomic<double> LastCategoryMemoryLimitScaleFactor_{1.0};

    void Adjust();
};

DEFINE_REFCOUNTED_TYPE(TRowCacheController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
