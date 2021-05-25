#include "tracer.h"

#include <yt/yt/library/tracing/jaeger/model.pb.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/rpc/grpc/channel.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/serialize.h>

#include <util/string/cast.h>
#include <util/string/reverse.h>

#include <util/system/getpid.h>
#include <util/system/env.h>
#include <util/system/byteorder.h>

namespace NYT::NTracing {

using namespace NRpc;
using namespace NConcurrency;
using namespace NProfiling;

static NLogging::TLogger Logger{"Jaeger"};
static NProfiling::TProfiler Profiler{"/tracing"};

////////////////////////////////////////////////////////////////////////////////

TJaegerTracerDynamicConfig::TJaegerTracerDynamicConfig()
{
    RegisterParameter("collector_channel_config", CollectorChannelConfig)
        .Optional();
    RegisterParameter("max_request_size", MaxRequestSize)
        .Default();
    RegisterParameter("max_memory", MaxMemory)
        .Default();
    RegisterParameter("subsampling_rate", SubsamplingRate)
        .Default();
    RegisterParameter("flush_period", FlushPeriod)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TJaegerTracerConfig::TJaegerTracerConfig()
{
    RegisterParameter("collector_channel_config", CollectorChannelConfig)
        .Optional();

    // 10K nodes x 128 KB / 15s == 85mb/s
    RegisterParameter("flush_period", FlushPeriod)
        .Default(TDuration::Seconds(15));
    RegisterParameter("rpc_timeout", RpcTimeout)
        .Default(TDuration::Seconds(15));
    RegisterParameter("queue_stall_timeout", QueueStallTimeout)
        .Default(TDuration::Minutes(15));
    RegisterParameter("max_request_size", MaxRequestSize)
        .Default(128_KB);
    RegisterParameter("max_batch_size", MaxBatchSize)
        .Default(128);
    RegisterParameter("max_memory", MaxMemory)
        .Default(1_GB);
    RegisterParameter("subsampling_rate", SubsamplingRate)
        .Default();

    RegisterParameter("service_name", ServiceName)
        .Default();
    RegisterParameter("process_tags", ProcessTags)
        .Default();
    RegisterParameter("enable_pid_tag", EnablePidTag)
        .Default(false);
}

TJaegerTracerConfigPtr TJaegerTracerConfig::ApplyDynamic(const TJaegerTracerDynamicConfigPtr& dynamicConfig)
{
    auto config = New<TJaegerTracerConfig>();
    config->CollectorChannelConfig = CollectorChannelConfig;
    if (dynamicConfig->CollectorChannelConfig) {
        config->CollectorChannelConfig = dynamicConfig->CollectorChannelConfig;
    }

    config->FlushPeriod = dynamicConfig->FlushPeriod.value_or(FlushPeriod);
    config->QueueStallTimeout = QueueStallTimeout;
    config->MaxRequestSize = dynamicConfig->MaxRequestSize.value_or(MaxRequestSize);
    config->MaxBatchSize = MaxBatchSize;
    config->MaxMemory = dynamicConfig->MaxMemory.value_or(MaxMemory);
    config->SubsamplingRate = SubsamplingRate;
    if (dynamicConfig->SubsamplingRate) {
        config->SubsamplingRate = dynamicConfig->SubsamplingRate;
    }

    config->ServiceName = ServiceName;
    config->ProcessTags = ProcessTags;
    config->EnablePidTag = EnablePidTag;

    config->Postprocess();
    return config;
}

bool TJaegerTracerConfig::IsEnabled() const
{
    return ServiceName && CollectorChannelConfig;
}

////////////////////////////////////////////////////////////////////////////////

class TJaegerCollectorProxy
    : public TProxyBase
{
public:
    DEFINE_RPC_PROXY(TJaegerCollectorProxy, jaeger.api_v2.CollectorService);

    DEFINE_RPC_PROXY_METHOD(NProto, PostSpans);
};

////////////////////////////////////////////////////////////////////////////////

namespace {

void ToProtoGuid(TString* proto, const TGuid& guid)
{
    *proto = TString{reinterpret_cast<const char*>(&guid.Parts32[0]), 16};
    ReverseInPlace(*proto);
}

void ToProtoUInt64(TString* proto, i64 i)
{
    i = SwapBytes64(i);
    *proto = TString{reinterpret_cast<char*>(&i), 8};
}

void ToProto(NProto::Span* proto, const TTraceContextPtr& traceContext)
{
    ToProtoGuid(proto->mutable_trace_id(), traceContext->GetTraceId());
    ToProtoUInt64(proto->mutable_span_id(), traceContext->GetSpanId());

    proto->set_operation_name(traceContext->GetSpanName());

    proto->mutable_start_time()->set_seconds(traceContext->GetStartTime().Seconds());
    proto->mutable_start_time()->set_nanos(traceContext->GetStartTime().NanoSecondsOfSecond());

    proto->mutable_duration()->set_seconds(traceContext->GetDuration().Seconds());
    proto->mutable_duration()->set_nanos(traceContext->GetDuration().NanoSecondsOfSecond());

    for (const auto& [name, value] : traceContext->GetTags()) {
        auto* protoTag = proto->add_tags();

        protoTag->set_key(name);
        protoTag->set_v_str(value);
    }

    if (auto parentSpanId = traceContext->GetParentSpanId(); parentSpanId != InvalidSpanId) {
        auto* ref = proto->add_references();

        ToProtoGuid(ref->mutable_trace_id(), traceContext->GetTraceId());
        ToProtoUInt64(ref->mutable_span_id(), parentSpanId);
        ref->set_ref_type(NProto::CHILD_OF);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TJaegerTracer::TJaegerTracer(
    const TJaegerTracerConfigPtr& config)
    : ActionQueue_(New<TActionQueue>("Jaeger"))
    , Flusher_(New<TPeriodicExecutor>(
        ActionQueue_->GetInvoker(),
        BIND(&TJaegerTracer::Flush, MakeStrong(this)),
        config->FlushPeriod))
    , Config_(config)
    , TracesDequeued_(Profiler.Counter("/traces_dequeued"))
    , TracesDropped_(Profiler.Counter("/traces_dropped"))
    , PushErrors_(Profiler.Counter("/push_errors"))
    , MemoryUsage_(Profiler.Gauge("/memory_usage"))
    , TraceQueueSize_(Profiler.Gauge("/queue_size"))
    , PushDuration_(Profiler.Timer("/push_duration"))
{
    Profiler.AddFuncGauge("/enabled", MakeStrong(this), [this] {
        return Config_.Load()->IsEnabled();
    });

    Flusher_->Start();
}

TFuture<void> TJaegerTracer::WaitFlush()
{
    auto guard = Guard(QueueEmptyLock_);
    return QueueEmpty_.ToFuture();
}

void TJaegerTracer::NotifyEmptyQueue()
{
    if (!TraceQueue_.IsEmpty() || !BatchQueue_.empty()) {
        return;
    }

    TPromise<void> promise;

    {
        auto guard = Guard(QueueEmptyLock_);
        promise = QueueEmpty_;
        QueueEmpty_ = NewPromise<void>();
    }

    promise.Set();
}

void TJaegerTracer::Stop()
{
    Flusher_->Stop();
    Flusher_.Reset();
    ActionQueue_->Shutdown();
}

void TJaegerTracer::Configure(const TJaegerTracerConfigPtr& config)
{
    Config_.Store(config);
    Flusher_->SetPeriod(config->FlushPeriod);
}

void TJaegerTracer::Enqueue(TTraceContextPtr trace)
{
    TraceQueue_.Enqueue(std::move(trace));
}

void TJaegerTracer::DequeueAll(const TJaegerTracerConfigPtr& config)
{
    auto traces = TraceQueue_.DequeueAll();
    if (traces.empty()) {
        return;
    }

    NProto::Batch batch;
    auto flushBatch = [&] {
        if (batch.spans_size() == 0) {
            return;
        }

        if (QueueMemory_ > config->MaxMemory) {
            TracesDropped_.Increment(batch.spans_size());
            batch.Clear();
            return;
        }

        BatchQueue_.emplace_back(batch.spans_size(), SerializeProtoToRef(batch));

        QueueMemory_ += BatchQueue_.back().second.size();
        MemoryUsage_.Update(QueueMemory_);
        QueueSize_ += batch.spans_size();
        TraceQueueSize_.Update(QueueSize_);
        TracesDequeued_.Increment(batch.spans_size());

        batch.Clear();
    };

    for (const auto& trace : traces) {
        if (config->SubsamplingRate && !trace->IsDebug()) {
            auto traceHash = THash<TGuid>()(trace->GetTraceId());

            if (((traceHash % 128) / 128.) > *config->SubsamplingRate) {
                continue;
            }
        }

        ToProto(batch.add_spans(), trace);

        if (batch.spans_size() > config->MaxBatchSize) {
            flushBatch();
        }
    }

    flushBatch();
}

std::pair<std::vector<TSharedRef>, int> TJaegerTracer::PeekQueue(const TJaegerTracerConfigPtr& config)
{
    std::vector<TSharedRef> batches;
    if (config->IsEnabled()) {
        batches.push_back(GetProcessInfo(config));
    }

    int size = 0;

    int i = 0;
    for (; i < static_cast<int>(BatchQueue_.size()); i++) {
        if (size > config->MaxRequestSize) {
            break;
        }

        size += BatchQueue_[i].second.Size();
        batches.push_back(BatchQueue_[i].second);
    }

    return std::make_pair(batches, i);
}

void TJaegerTracer::DropQueue(int i)
{
    for (; i > 0; i--) {
        QueueMemory_ -= BatchQueue_[0].second.Size();
        QueueSize_ -= BatchQueue_[0].first;
        BatchQueue_.pop_front();
    }

    TraceQueueSize_.Update(QueueSize_);
    MemoryUsage_.Update(QueueMemory_);
}

void TJaegerTracer::Flush()
{
    try {
        YT_LOG_DEBUG("Started span flush");

        auto config = Config_.Load();

        DequeueAll(config);

        auto dropFullQueue = [&] {
            while (true) {
                auto [batch, i] = PeekQueue(config);
                TracesDropped_.Increment(i);
                DropQueue(i);

                if (i == 0) {
                    break;
                }
            }
        };

        if (TInstant::Now() - LastSuccessfullFlushTime_ > config->QueueStallTimeout) {
            dropFullQueue();
        }

        if (!config->IsEnabled()) {
            dropFullQueue();
            return;
        }

        if (!CollectorChannel_ || OpenChannelConfig_ != config->CollectorChannelConfig) {
            OpenChannelConfig_.Reset();

            CollectorChannel_ = NGrpc::CreateGrpcChannel(config->CollectorChannelConfig);
            OpenChannelConfig_ = config->CollectorChannelConfig;
        }

        TJaegerCollectorProxy proxy(CollectorChannel_);
        proxy.SetDefaultTimeout(config->RpcTimeout);
        auto req = proxy.PostSpans();

        auto [batch, i] = PeekQueue(config);
        if (i > 0) {
            req->SetEnableLegacyRpcCodecs(false);
            req->set_batch(MergeRefsToString(batch));

            TEventTimerGuard timerGuard(PushDuration_);
            WaitFor(req->Invoke())
                .ThrowOnError();

            DropQueue(i);
        }

        LastSuccessfullFlushTime_ = TInstant::Now();
        NotifyEmptyQueue();

        YT_LOG_DEBUG("Finished span flush");
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to send spans");
        PushErrors_.Increment();
    }
}

TSharedRef TJaegerTracer::GetProcessInfo(const TJaegerTracerConfigPtr& config)
{
    NProto::Batch batch;
    auto* process = batch.mutable_process();
    process->set_service_name(*config->ServiceName);

    for (const auto& [key, value] : config->ProcessTags) {
        auto* tag = process->add_tags();
        tag->set_key(key);
        tag->set_v_str(value);
    }

    if (config->EnablePidTag) {
        auto* tag = process->add_tags();
        tag->set_key("pid");
        tag->set_v_str(::ToString(GetPID()));
    }

    return SerializeProtoToRef(batch);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
