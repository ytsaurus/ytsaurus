#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/sink.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/logging/log.h>

#include <atomic>
#include <deque>
#include <map>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! True if either routing expression is set.
bool IsTabletRoutingEnabled(const TQueueSinkTabletRoutingParameters& parameters);

////////////////////////////////////////////////////////////////////////////////

//! Owns a #TTabletIndexEvaluator plus the tablet count it reduces against. With an explicit
//! |tablet_count| the count is fixed and no queue read happens; otherwise the queue's live
//! @tablet_count is resolved once at #Start and refreshed periodically (error-resilient).
class TTabletRouter
    : public TRefCounted
{
public:
    TTabletRouter(
        TTabletIndexEvaluatorPtr evaluator,
        std::optional<i64> explicitTabletCount,
        TSinkContextPtr context,
        NYPath::TRichYPath queuePath,
        TDuration refreshPeriod,
        NLogging::TLogger logger);

    ~TTabletRouter() override;

    //! With no explicit count, resolves the queue's @tablet_count once (throws on failure) and
    //! starts periodic refresh. No-op when the count is explicit.
    void Start();

    //! Tablet index for |payload| using the current (live or explicit) tablet count.
    i64 GetTabletIndex(const TPayload& payload);

private:
    const TTabletIndexEvaluatorPtr Evaluator_;
    const std::optional<i64> ExplicitTabletCount_;
    const TSinkContextPtr Context_;
    const NYPath::TRichYPath QueuePath_;
    const TDuration RefreshPeriod_;
    const NLogging::TLogger Logger;

    NApi::IClientPtr Client_;
    std::atomic<i64> TabletCount_ = 0;
    NConcurrency::TPeriodicExecutorPtr RefreshExecutor_;

    i64 GetTabletCount() const;
    i64 FetchTabletCount();
    void RefreshTabletCount();
};

DEFINE_REFCOUNTED_TYPE(TTabletRouter);

//! Builds a router from routing |parameters| over |streamSchema|, dispatching on which expression
//! is set. Returns null when routing is off. |refreshPeriod| paces the @tablet_count refresh.
TTabletRouterPtr CreateTabletRouter(
    const TQueueSinkTabletRoutingParameters& parameters,
    const NYPath::TRichYPath& queuePath,
    TDuration refreshPeriod,
    const NTableClient::TTableSchemaPtr& streamSchema,
    const TSinkContextPtr& context,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

//! Groups |messages| by the tablet index computed by |router|, preserving per-tablet arrival
//! order. Groups are ordered by tablet index.
std::map<i64, std::deque<TOutputMessageConstPtr>> GroupMessagesByTabletIndex(
    TTabletRouter& router,
    const std::deque<TOutputMessageConstPtr>& messages);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
