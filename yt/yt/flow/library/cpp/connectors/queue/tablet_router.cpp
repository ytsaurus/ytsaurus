#include "tablet_router.h"

#include "spec.h"
#include "tablet_index_evaluator.h"

#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/client_common.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

bool IsTabletRoutingEnabled(const TQueueSinkTabletRoutingParameters& parameters)
{
    return parameters.TabletIndexExpression.has_value() ||
        parameters.TabletIndexRoutingHashExpression.has_value();
}

////////////////////////////////////////////////////////////////////////////////

TTabletRouter::TTabletRouter(
    TTabletIndexEvaluatorPtr evaluator,
    std::optional<i64> explicitTabletCount,
    TSinkContextPtr context,
    NYPath::TRichYPath queuePath,
    TDuration refreshPeriod,
    NLogging::TLogger logger)
    : Evaluator_(std::move(evaluator))
    , ExplicitTabletCount_(explicitTabletCount)
    , Context_(std::move(context))
    , QueuePath_(std::move(queuePath))
    , RefreshPeriod_(refreshPeriod)
    , Logger(std::move(logger))
{ }

TTabletRouter::~TTabletRouter()
{
    if (RefreshExecutor_) {
        YT_UNUSED_FUTURE(RefreshExecutor_->Stop());
    }
}

void TTabletRouter::Start()
{
    if (ExplicitTabletCount_) {
        return;
    }

    Client_ = QueuePath_.GetCluster()
        ? Context_->ClientsCache->GetClient(*QueuePath_.GetCluster())
        : Context_->GetClient();

    // Routing can't proceed without a count, so fail hard if the first resolution fails.
    TabletCount_.store(FetchTabletCount());
    YT_LOG_INFO("Resolved queue tablet count for routing (TabletCount: %v)", TabletCount_.load());

    RefreshExecutor_ = New<NConcurrency::TPeriodicExecutor>(
        Context_->PoolInvoker,
        BIND(&TTabletRouter::RefreshTabletCount, MakeWeak(this)),
        NConcurrency::TPeriodicExecutorOptions::WithJitter(RefreshPeriod_));
    RefreshExecutor_->Start();
}

i64 TTabletRouter::GetTabletIndex(const TPayload& payload)
{
    return Evaluator_->GetTabletIndex(payload, GetTabletCount());
}

i64 TTabletRouter::GetTabletCount() const
{
    return ExplicitTabletCount_.value_or(TabletCount_.load());
}

i64 TTabletRouter::FetchTabletCount()
{
    NApi::TGetNodeOptions options;
    // Many per-partition sinks poll the same rarely-changing attribute; serve from the master cache.
    options.ReadFrom = NApi::EMasterChannelKind::Cache;
    auto yson = NConcurrency::WaitFor(Client_->GetNode(QueuePath_.GetPath() + "/@tablet_count", options))
        .ValueOrThrow();
    auto tabletCount = NYTree::ConvertTo<i64>(yson);
    THROW_ERROR_EXCEPTION_IF(tabletCount <= 0,
        "Queue %v has non-positive @tablet_count %v",
        QueuePath_.GetPath(),
        tabletCount);
    return tabletCount;
}

void TTabletRouter::RefreshTabletCount()
{
    try {
        auto tabletCount = FetchTabletCount();
        if (tabletCount != TabletCount_.load()) {
            YT_LOG_INFO("Queue tablet count changed (TabletCount: %v)", tabletCount);
        }
        TabletCount_.store(tabletCount);
    } catch (const std::exception& ex) {
        // Keep the last-known-good count and keep routing; retry on the next tick.
        YT_LOG_WARNING(ex, "Failed to refresh queue tablet count, keeping last known value (TabletCount: %v)", TabletCount_.load());
    }
}

////////////////////////////////////////////////////////////////////////////////

TTabletRouterPtr CreateTabletRouter(
    const TQueueSinkTabletRoutingParameters& parameters,
    const NYPath::TRichYPath& queuePath,
    TDuration refreshPeriod,
    const NTableClient::TTableSchemaPtr& streamSchema,
    const TSinkContextPtr& context,
    const NLogging::TLogger& logger)
{
    std::optional<std::string> expression;
    std::optional<EQueueTabletIndexRoutingHashPolicy> policy;
    std::optional<i64> explicitTabletCount;
    if (parameters.TabletIndexExpression) {
        // Verbatim: the count is used only for the range check, resolved live from the queue.
        expression = parameters.TabletIndexExpression;
    } else if (parameters.TabletIndexRoutingHashExpression) {
        expression = parameters.TabletIndexRoutingHashExpression;
        policy = parameters.TabletIndexRoutingHashPolicy;
        explicitTabletCount = parameters.TabletCount;
    } else {
        return nullptr;
    }

    return New<TTabletRouter>(
        New<TTabletIndexEvaluator>(streamSchema, *expression, policy),
        explicitTabletCount,
        context,
        queuePath,
        refreshPeriod,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

std::map<i64, std::deque<TOutputMessageConstPtr>> GroupMessagesByTabletIndex(
    TTabletRouter& router,
    const std::deque<TOutputMessageConstPtr>& messages)
{
    std::map<i64, std::deque<TOutputMessageConstPtr>> groups;
    for (const auto& message : messages) {
        groups[router.GetTabletIndex(message->Payload)].push_back(message);
    }
    return groups;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
