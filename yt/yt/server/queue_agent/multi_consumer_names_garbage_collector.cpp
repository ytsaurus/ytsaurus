#include "multi_consumer_names_garbage_collector.h"

#include "config.h"
#include "pass_profiler.h"

#include <yt/yt/ytlib/queue_client/path.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_service.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NQueueAgent {

using namespace NAlertManager;
using namespace NConcurrency;
using namespace NQueueClient;
using namespace NTracing;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = MultiConsumerNamesGarbageCollectorLogger;

////////////////////////////////////////////////////////////////////////////////

class TMultiConsumerNamesGarbageCollector
    : public IMultiConsumerNamesGarbageCollector
{
public:
    TMultiConsumerNamesGarbageCollector(
        IInvokerPtr invoker,
        TDynamicStatePtr dynamicState,
        TCallback<IAlertCollectorPtr()> createAlertCollectorCallback)
        : Invoker_(std::move(invoker))
        , DynamicState_(std::move(dynamicState))
        , CreateAlertCollectorCallback_(std::move(createAlertCollectorCallback))
        , DynamicConfig_(New<TMultiConsumerNamesGarbageCollectorDynamicConfig>())
        , PassExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TMultiConsumerNamesGarbageCollector::Pass, MakeWeak(this)),
            DynamicConfig_.Acquire()->PassPeriod))
        , OrchidService_(IYPathService::FromProducer(BIND(&TMultiConsumerNamesGarbageCollector::BuildOrchid, MakeWeak(this)))->Via(Invoker_))
        , AlertCollector_(CreateAlertCollectorCallback_())
    { }

    IYPathServicePtr GetOrchidService() const override
    {
        return OrchidService_;
    }

    void Start() override
    {
        Active_ = true;

        // NB: Start and Stop called via Serialized Invoker, so there is no concurrency here.
        if (!PassProfiler_.Acquire()) {
            PassProfiler_.Store(New<TPassProfiler>(QueueAgentProfiler().WithPrefix("/multi_consumer_names_garbage_collector")));
        }
        AlertCollector_.Store(CreateAlertCollectorCallback_());
        PassExecutor_->Start();
    }

    void Stop() override
    {
        YT_UNUSED_FUTURE(PassExecutor_->Stop());

        // NB: Start and Stop called via Serialized Invoker, so there is no concurrency here.
        AlertCollector_.Acquire()->Stop();
        PassProfiler_.Store(nullptr);

        Active_ = false;
    }

    void OnDynamicConfigChanged(
        const TMultiConsumerNamesGarbageCollectorDynamicConfigPtr& oldConfig,
        const TMultiConsumerNamesGarbageCollectorDynamicConfigPtr& newConfig) override
    {
        DynamicConfig_.Store(newConfig);

        PassExecutor_->SetPeriod(newConfig->PassPeriod);

        YT_TLOG_DEBUG("Updated multi consumer names garbage collector dynamic config")
            .With("OldConfig", ConvertToYsonString(oldConfig, NYson::EYsonFormat::Text))
            .With("NewConfig", ConvertToYsonString(newConfig, NYson::EYsonFormat::Text));
    }

private:
    const IInvokerPtr Invoker_;
    const TDynamicStatePtr DynamicState_;
    const TCallback<IAlertCollectorPtr()> CreateAlertCollectorCallback_;

    TAtomicIntrusivePtr<TMultiConsumerNamesGarbageCollectorDynamicConfig> DynamicConfig_;
    const TPeriodicExecutorPtr PassExecutor_;
    const IYPathServicePtr OrchidService_;

    TAtomicIntrusivePtr<TPassProfiler> PassProfiler_;
    TAtomicIntrusivePtr<IAlertCollector> AlertCollector_;

    //! Whether this instance is actively performing passes.
    std::atomic<bool> Active_ = false;
    //! Current pass iteration error.
    TError PassError_;
    //! Current pass iteration instant.
    TInstant PassInstant_ = TInstant::Zero();
    //! Index of the current pass iteration.
    i64 PassIndex_ = -1;

    void Pass()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        if (!DynamicConfig_.Acquire()->Enable) {
            YT_TLOG_DEBUG("Pass skipped");
            return;
        }

        auto traceContextGuard = TTraceContextGuard(TTraceContext::NewRoot("MultiConsumerNamesGarbageCollector"));

        auto alertCollector = AlertCollector_.Acquire();
        auto passProfiler = PassProfiler_.Acquire();

        PassInstant_ = TInstant::Now();
        ++PassIndex_;
        if (passProfiler) {
            passProfiler->OnStart(PassIndex_, PassInstant_);
        }

        YT_TLOG_DEBUG("Pass started")
            .With("PassIndex", PassIndex_);
        try {
            GuardedPass();
            PassError_ = TError();
        } catch (const std::exception& ex) {
            PassError_ = TError(ex);
            YT_TLOG_ERROR("Error performing multi consumer names garbage collector pass")
                .With(ex);
            alertCollector->StageAlert(CreateAlert(
                NAlerts::EErrorCode::QueueAgentMultiConsumerNamesGarbageCollectorPassFailed,
                "Error performing multi consumer names garbage collector pass",
                /*tags*/ {},
                ex));
            if (passProfiler) {
                passProfiler->OnError();
            }
        }
        YT_TLOG_DEBUG("Pass finished")
            .With("PassIndex", PassIndex_);

        alertCollector->PublishAlerts();
        if (passProfiler) {
            passProfiler->OnFinish(TInstant::Now() - PassInstant_);
        }
    }

    void BuildOrchid(NYson::IYsonConsumer* consumer) const
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        BuildYsonFluently(consumer).BeginMap()
            .Item("active").Value(Active_.load())
            .Item("pass_instant").Value(PassInstant_)
            .Item("pass_index").Value(PassIndex_)
            .Item("pass_error").Value(PassError_)
        .EndMap();
    }

    void GuardedPass()
    {
        // NB(panesher): Multi consumer name rows are selected strictly before consumer rows.
        // Name rows are only written for multi consumers already present in the consumers
        // table, so every selected name row whose consumer is missing from the later
        // consumers snapshot indeed refers to a deleted multi consumer.
        auto multiConsumerNameRows = WaitFor(DynamicState_->MultiConsumerNames->Select())
            .ValueOrThrow();
        auto consumerRows = WaitFor(DynamicState_->Consumers->Select())
            .ValueOrThrow();

        THashSet<TTablePath> consumerPaths;
        consumerPaths.reserve(consumerRows.size());
        for (const auto& row : consumerRows) {
            consumerPaths.insert(row.Path);
        }

        std::vector<TMultiConsumerNameTableRow> rowsToDelete;
        for (auto& row : multiConsumerNameRows) {
            if (!consumerPaths.contains(ToTablePath(row.Ref))) {
                YT_TLOG_DEBUG("Multi consumer is not found in consumers table, scheduled to delete its name row")
                    .With("Ref", row.Ref);
                rowsToDelete.push_back(std::move(row));
            }
        }

        if (rowsToDelete.empty()) {
            return;
        }

        YT_TLOG_DEBUG("Deleting multi consumer name rows")
            .With("Count", rowsToDelete.size());
        WaitFor(DynamicState_->MultiConsumerNames->Delete(rowsToDelete))
            .ThrowOnError();
    }
};

IMultiConsumerNamesGarbageCollectorPtr CreateMultiConsumerNamesGarbageCollector(
    IInvokerPtr invoker,
    TDynamicStatePtr dynamicState,
    TCallback<IAlertCollectorPtr()> createAlertCollectorCallback)
{
    return New<TMultiConsumerNamesGarbageCollector>(
        std::move(invoker),
        std::move(dynamicState),
        std::move(createAlertCollectorCallback));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
