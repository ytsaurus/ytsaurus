#pragma once

#include <yt/yt/library/tracing/tracer.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/misc/lock_free.h>

#include <yt/yt/core/rpc/grpc/config.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJaegerTracerConfig)

class TJaegerTracerConfig
    : public NYTree::TYsonSerializable
{
public:
    NRpc::NGrpc::TChannelConfigPtr CollectorChannelConfig;

    TDuration FlushPeriod;

    i64 MaxRequestSize;

    i64 MaxMemory;

    // ServiceName is required by jaeger. When ServiceName is missing, tracer is disabled.
    std::optional<TString> ServiceName;

    THashMap<TString, TString> ProcessTags;

    bool EnablePidTag;

    TJaegerTracerConfig();
};

DEFINE_REFCOUNTED_TYPE(TJaegerTracerConfig)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJaegerTracer)

class TJaegerTracer
    : public ITracer
{
public:
    TJaegerTracer(
        const TJaegerTracerConfigPtr& config);

    TFuture<void> WaitFlush();

    virtual void Stop() override;

    virtual void Enqueue(TTraceContextPtr trace) override;

private:
    const TJaegerTracerConfigPtr Config_;
    const NConcurrency::TActionQueuePtr ActionQueue_;
    NConcurrency::TPeriodicExecutorPtr Flusher_;

    TMultipleProducerSingleConsumerLockFreeStack<TTraceContextPtr> TraceQueue_;

    TSharedRef ProcessInfo_;

    std::deque<std::pair<int, TSharedRef>> BatchQueue_;
    i64 QueueMemory_ = 0;
    i64 QueueSize_ = 0;

    TSpinLock QueueEmptyLock_;
    TPromise<void> QueueEmpty_ = NewPromise<void>();

    NRpc::IChannelPtr CollectorChannel_;

    void Flush();
    void DequeueAll();
    void NotifyEmptyQueue();

    std::pair<std::vector<TSharedRef>, int> PeekQueue();
    void DropQueue(int i);

    NProfiling::TCounter TracesDequeued_;
    NProfiling::TCounter TracesDropped_;
    NProfiling::TCounter PushErrors_;
    NProfiling::TGauge MemoryUsage_;
    NProfiling::TGauge TraceQueueSize_;
    NProfiling::TEventTimer PushDuration_;
};

DEFINE_REFCOUNTED_TYPE(TJaegerTracer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
