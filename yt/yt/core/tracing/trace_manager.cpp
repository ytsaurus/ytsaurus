#include "trace_manager.h"
#include "private.h"
#include "config.h"

#include <yt/core/tracing/proto/span.pb.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/lock_free.h>
#include <yt/core/misc/singleton.h>
#include <yt/core/misc/shutdown.h>

#include <yt/core/profiling/timing.h>

#include <util/folder/path.h>

#include <util/system/execpath.h>
#include <util/system/env.h>

namespace NYT::NTracing {

using namespace NConcurrency;

struct TTraceSpanTag { };

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TracingLogger;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TSpan* proto, const TTraceContextPtr& traceContext)
{
    ToProto(proto->mutable_trace_id(), traceContext->GetTraceId());
    proto->set_span_id(traceContext->GetSpanId());
    proto->set_parent_span_id(traceContext->GetParentSpanId());
    proto->set_follows_from_span_id(traceContext->GetSpanId());
    proto->set_name(traceContext->GetSpanName());
    proto->set_start_time(traceContext->GetStartTime().NanoSeconds());
    proto->set_duration(traceContext->GetDuration().NanoSeconds());

    for (const auto& tag : traceContext->GetTags()) {
        auto* protoTag = proto->add_tags();
        protoTag->set_key(tag.first);
        protoTag->set_value(tag.second);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TTraceManager::TImpl
{
public:
    TImpl()
        : CleanupExecutor_(New<TPeriodicExecutor>(
            ActionQueue_->GetInvoker(),
            BIND(&TImpl::Cleanup, this)))
    {
        CleanupExecutor_->Start();
    }

    void Configure(TTraceManagerConfigPtr config)
    {
        CleanupExecutor_->SetPeriod(config->CleanupPeriod);

        CleanupPeriod_.store(config->CleanupPeriod);
        TraceBufferSize_.store(config->TraceBufferSize);
        Enabled_.store(true);
    }

    void Shutdown()
    {
        ActionQueue_->Shutdown();
    }

    void Enqueue(NTracing::TTraceContextPtr traceContext)
    {
        if (!Enabled_.load()) {
            return;
        }

        EventQueue_.Enqueue(std::move(traceContext));
    }

    std::pair<i64, std::vector<TSharedRef>> ReadTraces(i64 startIndex, i64 limit)
    {
        std::vector<TSharedRef> traces;
        traces.reserve(limit);

        {
            auto guard = Guard(BufferLock_);
            startIndex = std::max(startIndex, StartIndex_);
            auto startOffset = startIndex - StartIndex_;
            for (int i = 0; i < limit && startOffset + i < TracesBuffer_.size(); ++i) {
                traces.push_back(TracesBuffer_[startOffset + i]);
            }
        }

        return std::make_pair(startIndex, std::move(traces));
    }

private:
    const TActionQueuePtr ActionQueue_ = New<TActionQueue>("Tracing");
    const TPeriodicExecutorPtr CleanupExecutor_;

    std::atomic<TDuration> CleanupPeriod_;
    std::atomic<i64> TraceBufferSize_ = 0;
    std::atomic<bool> Enabled_ = false;
    
    TMultipleProducerSingleConsumerLockFreeStack<TTraceContextPtr> EventQueue_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, BufferLock_);
    std::deque<TSharedRef> TracesBuffer_;
    i64 StartIndex_ = 0;
    i64 TracesBufferSize_ = 0;

    void Cleanup()
    {
        YT_LOG_DEBUG("Running trace cleanup iteration");

        NProfiling::TWallTimer timer;
        auto batch = EventQueue_.DequeueAll();

        bool traceDebug = false;
        auto traceDir = GetEnv("YT_TRACE_DUMP_DIR", "");
        if (!traceDir.empty()) {
            traceDebug = true;
            auto binary = TFsPath(GetExecPath()).Basename();
            auto traceFileName = Format("%v/%v.%v", traceDir, binary, getpid());

            NProto::TSpanBatch protoBatch;
            for (const auto& traceContext : batch) {
                ToProto(protoBatch.add_spans(), traceContext);
            }

            YT_LOG_DEBUG("Dumping traces to file (Filename: %v, BatchSize: %v)",
                traceFileName,
                batch.size());
            TString batchDump;
            protoBatch.SerializeToString(&batchDump);

            TFileOutput output(TFile::ForAppend(traceFileName));
            output << batchDump;
            output.Finish();
        }

        for (const auto& trace : batch) {
            PushTrace(trace);
        }

        YT_LOG_DEBUG("Traces collected (Count: %v)",
            batch.size());
        
        auto elapsed = timer.GetElapsedTime();
        if (!traceDebug && elapsed > CleanupPeriod_.load()) {
            YT_LOG_WARNING("Trace cleanup took too much time; disabling trace collection (CleanupTime: %v)",
                elapsed);
            Enabled_.store(false);
        } else if (!Enabled_.exchange(true)) {
            YT_LOG_WARNING("Trace collection re-enabled (CleanupTime: %v)",
                elapsed);
        }
    }

    void PushTrace(const TTraceContextPtr& trace)
    {
        NProto::TSpanBatch singleTrace;
        ToProto(singleTrace.add_spans(), trace);

        auto ref = SerializeProtoToRef(singleTrace);
        auto guard = Guard(BufferLock_);

        TracesBuffer_.push_back(ref);
        TracesBufferSize_ += ref.size();

        while (!TracesBuffer_.empty() && TracesBufferSize_ > TraceBufferSize_.load()) {
            StartIndex_++;
            TracesBufferSize_ -= TracesBuffer_.front().Size();
            TracesBuffer_.pop_front();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TTraceManager::TTraceManager()
    : Impl_(new TImpl())
{ }

TTraceManager::~TTraceManager() = default;

TTraceManager* TTraceManager::Get()
{
    return Singleton<TTraceManager>();
}

void TTraceManager::StaticShutdown()
{
    Get()->Shutdown();
}

void TTraceManager::Configure(TTraceManagerConfigPtr config)
{
    Impl_->Configure(std::move(config));
}

void TTraceManager::Shutdown()
{
    Impl_->Shutdown();
}

void TTraceManager::Enqueue(TTraceContextPtr traceContext)
{
    Impl_->Enqueue(std::move(traceContext));
}

std::pair<i64, std::vector<TSharedRef>> TTraceManager::ReadTraces(i64 startIndex, i64 limit)
{
    return Impl_->ReadTraces(startIndex, limit);
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(8, TTraceManager::StaticShutdown);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

