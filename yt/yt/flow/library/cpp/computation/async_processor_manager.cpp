#include "async_processor_manager.h"

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/yson/writer.h>

#include <util/stream/length.h>
#include <util/stream/null.h>

namespace NYT::NFlow {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

//! The size of the binary YSON of |value|.
template <class T>
i64 GetYsonByteSize(const T& value)
{
    // Counted without keeping the YSON.
    TCountingOutput output(&Cnull);
    TBufferedYsonWriter writer(&output, EYsonFormat::Binary);
    Serialize(value, &writer);
    writer.Flush();
    return output.Counter();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TAsyncProcessorManager::TAsyncProcessorManager(IInvokerPtr serializedInvoker, NProfiling::TProfiler profiler, NLogging::TLogger logger)
    : SerializedInvoker_(std::move(serializedInvoker))
    , Profiler_(std::move(profiler))
    , Logger(std::move(logger))
    , DefaultSpec_(New<TDynamicAsyncProcessorSpec>())
{ }

void TAsyncProcessorManager::Register(TAsyncProcessorDescriptorPtr descriptor)
{
    const auto& name = descriptor->Name;
    THROW_ERROR_EXCEPTION_IF(name.empty(), "Async processor has an empty name");

    auto profiler = Profiler_.WithTag("processor", name);
    auto [it, inserted] = Processors_.try_emplace(name, TProcessor{
            .Descriptor = descriptor,
            .Spec = DefaultSpec_,
            .StartedCounter = profiler.Counter("/started"),
            .CompletedCounter = profiler.Counter("/completed"),
            .FailedCounter = profiler.Counter("/failed"),
            .RunningGauge = profiler.Gauge("/running"),
            .PendingGauge = profiler.Gauge("/pending"),
            .ReadyCountGauge = profiler.Gauge("/ready_count"),
            .ReadyByteSizeGauge = profiler.Gauge("/ready_byte_size"),
                                                        });
    THROW_ERROR_EXCEPTION_UNLESS(inserted, "Async processor %Qv is registered twice", name);
    YT_TLOG_INFO("Async processor registered")
        .With("Processor", name);
}

void TAsyncProcessorManager::Reconfigure(const THashMap<std::string, TDynamicAsyncProcessorSpecPtr>& specs)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);
    for (const auto& [name, spec] : specs) {
        THROW_ERROR_EXCEPTION_UNLESS(Processors_.contains(name),
            "Dynamic spec has limits of async processor %Qv, which the computation does not register",
            name);
    }
    for (auto& [name, processor] : Processors_) {
        auto it = specs.find(name);
        processor.Spec = it != specs.end() && it->second ? it->second : DefaultSpec_;
        // Raised limits make room for the waiting tasks.
        StartPending(processor);
    }
}

bool TAsyncProcessorManager::CanStart(const TProcessor& processor)
{
    const auto& spec = *processor.Spec;
    return processor.Running < spec.MaxRunningTasks && processor.ReadyByteSize < spec.MaxReadyByteSize;
}

std::vector<std::string> TAsyncProcessorManager::GetSaturatedProcessors() const
{
    std::vector<std::string> saturated;
    for (const auto& [name, processor] : Processors_) {
        if (!CanStart(processor) || !processor.Pending.empty()) {
            saturated.push_back(name);
        }
    }
    std::ranges::sort(saturated);
    return saturated;
}

const THashMap<TStreamId, TBatchStatistics>& TAsyncProcessorManager::GetReadyByRootStream() const
{
    return ReadyByRootStream_;
}

std::vector<TUntypedCompletedAsyncTaskPtr> TAsyncProcessorManager::TakeReady(
    const THashSet<TStreamId>& allowedRootStreams,
    i64 maxRows,
    i64 maxByteSize)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);
    std::vector<TUntypedCompletedAsyncTaskPtr> taken;
    i64 byteSize = 0;
    auto it = Ready_.begin();
    while (it != Ready_.end() && std::ssize(taken) < maxRows) {
        const auto& next = *it;
        if (!allowedRootStreams.contains(next->RootTimer->StreamId)) {
            ++it;
            continue;
        }
        if (!taken.empty() && byteSize + next->ByteSize > maxByteSize) {
            break;
        }
        AccountReady(GetOrCrash(Processors_, next->Processor->Name), *next, -1);
        byteSize += next->ByteSize;
        taken.push_back(std::move(*it));
        it = Ready_.erase(it);
    }
    return taken;
}

void TAsyncProcessorManager::Start(std::vector<TAsyncTaskRequest> requests)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);
    if (Stopped_) {
        return;
    }
    for (auto& request : requests) {
        GetOrCrash(Processors_, request.Processor->Name).Pending.push_back(std::move(request));
    }
    // The epoch may have taken results and so made room for the waiting tasks.
    for (auto& [_, processor] : Processors_) {
        StartPending(processor);
    }
}

void TAsyncProcessorManager::StartTask(TProcessor& processor, TAsyncTaskRequest request)
{
    auto sequence = NextSequence_++;
    TFuture<TYsonMessagePtr> future;
    try {
        future = request.Processor->Start(request.Task);
    } catch (const std::exception& ex) {
        future = MakeFuture<TYsonMessagePtr>(TError(ex));
    }
    if (!future) {
        future = MakeFuture<TYsonMessagePtr>(TError("Async processor %Qv returned a null future", request.Processor->Name));
    }
    YT_TLOG_DEBUG("Task started")
        .With("Processor", request.Processor->Name)
        .With("RootMessageId", request.RootTimer->MessageId)
        .With("Sequence", sequence);
    EmplaceOrCrash(Running_, sequence, TRunningTask{.Request = std::move(request), .Future = future});
    ++processor.Running;
    processor.StartedCounter.Increment();
    // The timeout cancels the future of the task; the subscription keeps nothing but the sequence number.
    future.WithTimeout(processor.Spec->Timeout)
        .Subscribe(BIND_NO_PROPAGATE(&TAsyncProcessorManager::OnCompleted, MakeWeak(this), sequence)
                .Via(SerializedInvoker_));
}

void TAsyncProcessorManager::StartPending(TProcessor& processor)
{
    while (!processor.Pending.empty() && CanStart(processor)) {
        auto request = std::move(processor.Pending.front());
        processor.Pending.pop_front();
        StartTask(processor, std::move(request));
    }
    UpdateGauges(processor);
}

void TAsyncProcessorManager::Stop()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);
    Stopped_ = true;
    // The completions that are on their way find no task and do nothing.
    for (const auto& [_, task] : std::exchange(Running_, {})) {
        task.Future.Cancel(TError("Async processor manager is stopped"));
    }
    Ready_.clear();
    ReadyByRootStream_.clear();
    for (auto& [_, processor] : Processors_) {
        // A callback may hold the computation, which holds the descriptor through its handle.
        processor.Descriptor->Start.Reset();
        processor.Descriptor->ResultHandler.Reset();
        processor.Running = 0;
        processor.ReadyCount = 0;
        processor.ReadyByteSize = 0;
        processor.Pending.clear();
        UpdateGauges(processor);
    }
}

void TAsyncProcessorManager::OnCompleted(i64 sequence, const TErrorOr<TYsonMessagePtr>& result)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);
    auto it = Running_.find(sequence);
    if (it == Running_.end()) {
        return;
    }
    auto request = std::move(it->second.Request);
    Running_.erase(it);

    const auto& name = request.Processor->Name;
    auto task = New<TUntypedCompletedAsyncTask>();
    task->Result = result;
    if (result.IsOK() && !result.Value()) {
        task->Result = TError("Async processor %Qv returned a null result", name);
    }
    try {
        task->ByteSize = task->Result.IsOK() ? GetYsonByteSize(*task->Result.Value()) : GetYsonByteSize(TError(task->Result));
    } catch (const std::exception& ex) {
        task->Result = TError(ex).Wrap("Failed to measure the result of async processor %Qv", name);
        task->ByteSize = GetYsonByteSize(TError(task->Result));
    }

    auto& processor = GetOrCrash(Processors_, name);
    --processor.Running;
    if (task->Result.IsOK()) {
        processor.CompletedCounter.Increment();
        YT_TLOG_DEBUG("Task completed")
            .With("Processor", name)
            .With("RootMessageId", request.RootTimer->MessageId)
            .With("Sequence", sequence);
    } else {
        processor.FailedCounter.Increment();
        YT_TLOG_WARNING("Task failed")
            .With("Processor", name)
            .With("RootMessageId", request.RootTimer->MessageId)
            .With("Sequence", sequence)
            .With(TError(task->Result));
    }

    task->Processor = std::move(request.Processor);
    task->RootTimer = std::move(request.RootTimer);
    task->Task = std::move(request.Task);
    AccountReady(processor, *task, +1);
    Ready_.push_back(std::move(task));
    StartPending(processor);
}

void TAsyncProcessorManager::AccountReady(TProcessor& processor, const TUntypedCompletedAsyncTask& task, int sign)
{
    processor.ReadyCount += sign;
    processor.ReadyByteSize += sign * task.ByteSize;
    UpdateGauges(processor);
    auto& stream = ReadyByRootStream_[task.RootTimer->StreamId];
    stream.Count += sign;
    stream.ByteSize += sign * task.ByteSize;
    if (stream.Count == 0) {
        ReadyByRootStream_.erase(task.RootTimer->StreamId);
    }
}

void TAsyncProcessorManager::UpdateGauges(const TProcessor& processor)
{
    processor.RunningGauge.Update(processor.Running);
    processor.PendingGauge.Update(std::ssize(processor.Pending));
    processor.ReadyCountGauge.Update(processor.ReadyCount);
    processor.ReadyByteSizeGauge.Update(processor.ReadyByteSize);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
