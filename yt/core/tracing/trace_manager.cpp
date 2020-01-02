#include "trace_manager.h"
#include "private.h"
#include "config.h"

#include <yt/core/tracing/proto/span.pb.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/lock_free.h>
#include <yt/core/misc/singleton.h>
#include <yt/core/misc/shutdown.h>

#include <util/folder/path.h>
#include <util/system/execpath.h>
#include <util/system/env.h>

namespace NYT::NTracing {

using namespace NConcurrency;

struct TTraceSpanTag {};

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
        auto tag_proto = proto->add_tags();
        tag_proto->set_key(tag.first);
        tag_proto->set_value(tag.second);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TTraceManager::TImpl
{
public:
    TImpl()
        : ActionQueue_(New<TActionQueue>("Tracing"))
    { }

    void Configure(TTraceManagerConfigPtr config)
    {
        if (Enabled_.exchange(true)) {
            return;
        }

        Config_ = std::move(config);
        CleanupExecutor_ = New<TPeriodicExecutor>(
            ActionQueue_->GetInvoker(),
            BIND(&TImpl::Cleanup, this),
            Config_->CleanupPeriod);
        CleanupExecutor_->Start();
    }

    void Shutdown()
    {
        ActionQueue_->Shutdown();
    }

    void Enqueue(
        const NTracing::TTraceContextPtr& traceContext)
    {
        if (!Enabled_) {
            return;
        }

        EventQueue_.Enqueue(traceContext);
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
    TTraceManagerConfigPtr Config_;
    TActionQueuePtr ActionQueue_;
    TMultipleProducerSingleConsumerLockFreeStack<TTraceContextPtr> EventQueue_;
    TPeriodicExecutorPtr CleanupExecutor_;

    std::atomic<bool> Enabled_ = {false};

    TSpinLock BufferLock_;
    std::deque<TSharedRef> TracesBuffer_;
    i64 StartIndex_ = 0;
    i64 TracesBufferSize_ = 0;

    void Cleanup()
    {
        auto startTime = TInstant::Now();
        YT_LOG_DEBUG("Running tracing cleanup iteration");
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

            YT_LOG_DEBUG("Dumping traces to file (Filename: %Qv, BatchSize: %v)",
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

        YT_LOG_DEBUG("Collected %v traces", batch.size());
        auto duration = TInstant::Now() - startTime;
        if (!traceDebug && duration > Config_->CleanupPeriod) {
            YT_LOG_WARNING("Trace cleanup iteration took %v; disabling trace collection",
                duration);
            Enabled_ = false;
        } else {
            Enabled_ = true;
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

        while (!TracesBuffer_.empty() && TracesBufferSize_ > Config_->TracesBufferSize) {
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

