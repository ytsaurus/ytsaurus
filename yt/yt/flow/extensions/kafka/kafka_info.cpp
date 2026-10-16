#include "kafka_info.h"

#include "private.h"
#include "spec.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <contrib/libs/cppkafka/include/cppkafka/metadata.h>
#include <contrib/libs/cppkafka/include/cppkafka/topic.h>

namespace NYT::NFlow {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TKafkaInfoControllerState::Register(TRegistrar registrar)
{
    registrar.Parameter("cached_partition_count", &TThis::CachedPartitionCount)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TKafkaInfoController::TKafkaInfoController(
    TKafkaInfoSpecPtr spec,
    TKafkaClientPtr client,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger,
    IStatusProfilerPtr statusProfiler)
    : Logger(logger)
    , Spec_(std::move(spec))
    , Client_(std::move(client))
    , Invoker_(std::move(invoker))
    , UpdatePartitionCountErrorState_(statusProfiler->ErrorState("/update_partition_count"))
    , Producer_([&] {
        auto configuration = Client_->MakeBaseConfiguration();
        // librdkafka defaults this to true for producers, so a metadata request would create the
        // topic it asks about wherever the broker allows it -- a typo would read as an empty topic.
        configuration.set("allow.auto.create.topics", "false");
        return std::make_unique<cppkafka::Producer>(std::move(configuration));
    }())
    , MetadataQueue_(New<TActionQueue>("KafkaMetadata"))
{
    Producer_->set_timeout(std::chrono::milliseconds(Spec_->MetadataTimeout.MilliSeconds()));
}

void TKafkaInfoController::Init(IInitContextPtr initContext)
{
    initContext->InitClient<TKafkaInfoControllerState>(State_, "v0");

    Executor_ = New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TKafkaInfoController::TryUpdatePartitionCount, MakeWeak(this)),
        TPeriodicExecutorOptions::WithJitter(Spec_->UpdatePartitionCountPeriod));
    Executor_->Start();
    Executor_->ScheduleOutOfBand();
}

void TKafkaInfoController::Sync()
{ }

void TKafkaInfoController::Commit()
{ }

void TKafkaInfoController::TryUpdatePartitionCount()
{
    try {
        // The metadata call is a blocking broker round-trip; run it on the dedicated queue and yield
        // this (controller) fiber via WaitFor so the shared invoker's thread is not blocked.
        auto count = WaitFor(
            BIND([this, this_ = MakeStrong(this)] {
                auto topic = Producer_->get_topic(Spec_->Topic);
                auto metadata = Producer_->get_metadata(topic);
                // A topic the principal may not describe comes back with an error and no
                // partitions, indistinguishable from a missing or an empty one unless it is read.
                if (auto error = metadata.get_error()) {
                    THROW_ERROR_EXCEPTION("Kafka metadata request for topic %Qv failed: %v",
                        Spec_->Topic,
                        error.to_string())
                        .With("kafka_error", error.to_string())
                        .With("sasl_username", Client_->GetSaslUsername());
                }
                return static_cast<i64>(metadata.get_partitions().size());
            })
                .AsyncVia(MetadataQueue_->GetInvoker())
                .Run())
            .ValueOrThrow();

        if (count <= 0) {
            // No error and no partitions is not a reply Kafka is expected to produce.
            THROW_ERROR_EXCEPTION("Kafka reported no partitions and no error for topic %Qv",
                Spec_->Topic);
        }

        auto current = State_->CachedPartitionCount;
        if (!current) {
            State_->CachedPartitionCount = count;
            YT_TLOG_INFO("Kafka topic partition count discovered")
                .With("Topic", Spec_->Topic)
                .With("PartitionCount", count);
        } else if (count > *current) {
            // Kafka only ever adds partitions; new partitions become new source jobs.
            State_->CachedPartitionCount = count;
            YT_TLOG_INFO("Kafka topic partition count grew")
                .With("Topic", Spec_->Topic)
                .With("OldCount", *current)
                .With("NewCount", count);
        } else if (count < *current) {
            // A transient metadata glitch can report fewer partitions; never shrink the cached count.
            YT_TLOG_WARNING("Broker reported fewer partitions than cached, ignoring")
                .With("Topic", Spec_->Topic)
                .With("CachedCount", *current)
                .With("ReportedCount", count);
        }

        UpdatePartitionCountErrorState_->ClearError();
    } catch (const std::exception& ex) {
        UpdatePartitionCountErrorState_->SetError(TError("Failed to update partition count").With(ex));
        YT_TLOG_ERROR("Failed to update Kafka partition count")
            .With("Topic", Spec_->Topic)
            .With(ex);
    }
}

std::optional<i64> TKafkaInfoController::GetPartitionCount() const
{
    return State_->CachedPartitionCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
