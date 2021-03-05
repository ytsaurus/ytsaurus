#include "tracer.h"

#include <yt/yt/library/tracing/jaeger/model.pb.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/rpc/grpc/channel.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/serialize.h>

#include <util/string/cast.h>

#include <util/system/getpid.h>
#include <util/system/env.h>
#include <util/system/byteorder.h>

namespace NYT::NTracing {

using namespace NRpc;
using namespace NConcurrency;
using namespace NProfiling;

static NLogging::TLogger Logger{"Jaeger"};
static NProfiling::TRegistry Profiler{"/tracing"};

////////////////////////////////////////////////////////////////////////////////

TJaegerTracerConfig::TJaegerTracerConfig()
{
    RegisterParameter("collector_channel_config", CollectorChannelConfig)
        .Optional();

    // 10K nodes x 128 KB / 15s == 85mb/s
    RegisterParameter("flush_period", FlushPeriod)
        .Default(TDuration::Seconds(15));
    RegisterParameter("max_request_size", MaxRequestSize)
        .Default(128_KB);
    RegisterParameter("max_memory", MaxMemory)
        .Default(1_GB);

    RegisterParameter("service_name", ServiceName)
        .Default();
    RegisterParameter("process_tags", ProcessTags)
        .Default();
    RegisterParameter("enable_pid_tag", EnablePidTag)
        .Default(false);

    RegisterPostprocessor([this] {
        if (ServiceName && !CollectorChannelConfig) {
            THROW_ERROR_EXCEPTION("\"collector_channel_config\" is required");
        }
    });
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
    : Config_(config)
    , ActionQueue_(New<TActionQueue>("Jaeger"))
    , Flusher_(New<TPeriodicExecutor>(
        ActionQueue_->GetInvoker(),
        BIND(&TJaegerTracer::Flush, MakeStrong(this)),
        Config_->FlushPeriod))
    , TracesDequeued_(Profiler.Counter("/traces_dequeued"))
    , TracesDropped_(Profiler.Counter("/traces_dropped"))
    , PushErrors_(Profiler.Counter("/push_errors"))
    , MemoryUsage_(Profiler.Gauge("/memory_usage"))
    , TraceQueueSize_(Profiler.Gauge("/queue_size"))
    , PushDuration_(Profiler.Timer("/push_duration"))
{
    if (!Config_->ServiceName) {
        Flusher_.Reset();
        return;
    }

    NProto::Batch batch;
    auto* process = batch.mutable_process();
    process->set_service_name(*Config_->ServiceName);

    for (const auto& [key, value] : Config_->ProcessTags) {
        auto* tag = process->add_tags();
        tag->set_key(key);
        tag->set_v_str(value);
    }

    if (Config_->EnablePidTag) {
        auto* tag = process->add_tags();
        tag->set_key("pid");
        tag->set_v_str(::ToString(GetPID()));
    }

    ProcessInfo_ = SerializeProtoToRef(batch);

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
    if (Flusher_) {
        Flusher_->Stop();
        Flusher_.Reset();
    }
    ActionQueue_->Shutdown();
}

void TJaegerTracer::Enqueue(TTraceContextPtr trace)
{
    TraceQueue_.Enqueue(std::move(trace));
}

void TJaegerTracer::DequeueAll()
{
    auto traces = TraceQueue_.DequeueAll();
    if (traces.empty()) {
        return;
    }

    NProto::Batch batch;
    for (const auto& trace : traces) {
        ToProto(batch.add_spans(), trace);
    }

    BatchQueue_.emplace_back(traces.size(), SerializeProtoToRef(batch));

    QueueMemory_ += BatchQueue_.back().second.size();
    MemoryUsage_.Update(QueueMemory_);
    QueueSize_ += traces.size();
    TraceQueueSize_.Update(QueueSize_);
}

std::pair<std::vector<TSharedRef>, int> TJaegerTracer::PeekQueue()
{
    std::vector<TSharedRef> batches{ProcessInfo_};

    int size = 0;

    int i = 0;
    for (; i < static_cast<int>(BatchQueue_.size()); i++) {
        if (size > Config_->MaxRequestSize) {
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

        DequeueAll();

        if (!CollectorChannel_) {
            CollectorChannel_ = NGrpc::CreateGrpcChannel(Config_->CollectorChannelConfig);
        }

        TJaegerCollectorProxy proxy(CollectorChannel_);
        auto req = proxy.PostSpans();

        auto [batch, i] = PeekQueue();
        if (i > 0) {
            req->SetEnableLegacyRpcCodecs(false);
            req->set_batch(MergeRefsToString(batch));

            TEventTimerGuard timerGuard(PushDuration_);
            WaitFor(req->Invoke())
                .ThrowOnError();

            DropQueue(i);
        }

        NotifyEmptyQueue();

        YT_LOG_DEBUG("Finished span flush");
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to send spans");
        PushErrors_.Increment();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
