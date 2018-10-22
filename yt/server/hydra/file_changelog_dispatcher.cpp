#include "file_changelog_dispatcher.h"
#include "private.h"
#include "changelog.h"
#include "config.h"
#include "sync_file_changelog.h"

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/collection_helpers.h>
#include <yt/core/misc/finally.h>
#include <yt/core/misc/fs.h>

#include <yt/core/profiling/timing.h>

#include <atomic>

namespace NYT {
namespace NHydra {

using namespace NConcurrency;
using namespace NHydra::NProto;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HydraLogger;

static const auto FlushThreadQuantum = TDuration::MilliSeconds(10);

////////////////////////////////////////////////////////////////////////////////

class TFileChangelogQueue
    : public TRefCounted
{
public:
    explicit TFileChangelogQueue(
        TSyncFileChangelogPtr changelog,
        const TProfiler& profiler,
        const IInvokerPtr& invoker)
        : Changelog_(std::move(changelog))
        , Profiler(profiler)
        , Invoker_(NConcurrency::CreateBoundedConcurrencyInvoker(invoker, 1))
        , ProcessQueueCallback_(BIND(&TFileChangelogQueue::Process, MakeWeak(this)))
        , FlushedRecordCount_(Changelog_->GetRecordCount())
    { }

    ~TFileChangelogQueue()
    {
        LOG_DEBUG("Changelog queue destroyed (Path: %v)",
            Changelog_->GetFileName());
    }

    const TSyncFileChangelogPtr& GetChangelog()
    {
        return Changelog_;
    }

    TFuture<void> AsyncAppend(TSharedRef data)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TFuture<void> result;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            AppendQueue_.push_back(std::move(data));
            ByteSize_ += data.Size();
            YCHECK(FlushPromise_);
            result = FlushPromise_;
        }

        return result;
    }

    TFuture<void> AsyncFlush()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock_);

        if (FlushQueue_.empty() && AppendQueue_.empty()) {
            return VoidFuture;
        }

        FlushForced_.store(true);
        return FlushPromise_;
    }


    bool HasPendingFlushes()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& config = Changelog_->GetConfig();

        if (ByteSize_.load() >= config->FlushBufferSize) {
            return true;
        }

        if (LastFlushed_.load() + NProfiling::DurationToCpuDuration(config->FlushPeriod) <  NProfiling::GetCpuInstant()) {
            return true;
        }

        if (FlushForced_.load()) {
            return true;
        }

        return false;
    }

    bool HasUnflushedRecords()
    {
        VERIFY_THREAD_AFFINITY(SyncThread);

        return !AppendQueue_.empty() || !FlushQueue_.empty();
    }

    void RunPendingFlushes()
    {
        VERIFY_THREAD_AFFINITY(SyncThread);

        SyncFlush();
    }

    std::vector<TSharedRef> Read(int firstRecordId, int maxRecords, i64 maxBytes)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::vector<TSharedRef> records;
        int currentRecordId = firstRecordId;
        int needRecords = maxRecords;
        i64 needBytes = maxBytes;

        auto appendRecord = [&] (const TSharedRef& record) {
            records.push_back(record);
            --needRecords;
            ++currentRecordId;
            needBytes -= record.Size();
        };

        auto needMore = [&] () {
            return needRecords > 0 && needBytes > 0;
        };

        while (needMore()) {
            TGuard<TSpinLock> guard(SpinLock_);
            if (currentRecordId < FlushedRecordCount_) {
                // Read from disk, w/o spinlock.
                guard.Release();

                PROFILE_TIMING ("/changelog_read_io_time") {
                    auto diskRecords = Changelog_->Read(currentRecordId, needRecords, needBytes);
                    for (const auto& record : diskRecords) {
                        appendRecord(record);
                    }
                }
            } else {
                // Read from memory, w/ spinlock.

                auto readFromMemory = [&] (const std::vector<TSharedRef>& memoryRecords, int firstMemoryRecordId) {
                    if (!needMore())
                        return;
                    int memoryIndex = currentRecordId - firstMemoryRecordId;
                    YCHECK(memoryIndex >= 0);
                    while (memoryIndex < static_cast<int>(memoryRecords.size()) && needMore()) {
                        appendRecord(memoryRecords[memoryIndex++]);
                    }
                };

                PROFILE_TIMING ("/changelog_read_copy_time") {
                    readFromMemory(FlushQueue_, FlushedRecordCount_);
                    readFromMemory(AppendQueue_, FlushedRecordCount_ + FlushQueue_.size());
                }

                // Break since we don't except more records beyond this point.
                break;
            }
        }

        return records;
    }

    const IInvokerPtr& GetInvoker()
    {
        return Invoker_;
    }

    void Wakeup()
    {
        if (ProcessQueueCallbackPending_.load(std::memory_order_relaxed)) {
            return;
        }

        bool expected = false;
        if (ProcessQueueCallbackPending_.compare_exchange_strong(expected, true)) {
            GetInvoker()->Invoke(ProcessQueueCallback_);
        }
    }

    void Process() {
        ProcessQueueCallbackPending_ = false;

        if (HasPendingFlushes()) {
            RunPendingFlushes();
        }
    }

private:
    const TSyncFileChangelogPtr Changelog_;
    const TProfiler Profiler;
    const IInvokerPtr Invoker_;
    const TClosure ProcessQueueCallback_;

    TSpinLock SpinLock_;

    //! Number of records flushed to the underlying sync changelog.
    int FlushedRecordCount_ = 0;
    //! These records are currently being flushed to the underlying sync changelog and
    //! immediately follow the flushed part.
    std::vector<TSharedRef> FlushQueue_;
    //! Newly appended records go here. These records immediately follow the flush part.
    std::vector<TSharedRef> AppendQueue_;

    std::atomic<i64> ByteSize_ = {0};

    TPromise<void> FlushPromise_ = NewPromise<void>();
    std::atomic<bool> FlushForced_ = {false};
    std::atomic<NProfiling::TCpuInstant> LastFlushed_ = {0};
    std::atomic<bool> ProcessQueueCallbackPending_ = {false};

    DECLARE_THREAD_AFFINITY_SLOT(SyncThread);


    void SyncFlush()
    {
        TPromise<void> flushPromise;
        {
            TGuard<TSpinLock> guard(SpinLock_);

            YCHECK(FlushQueue_.empty());
            FlushQueue_.swap(AppendQueue_);
            ByteSize_.store(0);

            YCHECK(FlushPromise_);
            flushPromise = FlushPromise_;
            FlushPromise_ = NewPromise<void>();
            FlushForced_.store(false);
        }

        TError error;
        if (!FlushQueue_.empty()) {
            PROFILE_TIMING("/changelog_flush_io_time") {
                try {
                    Changelog_->Append(FlushedRecordCount_, FlushQueue_);
                    Changelog_->Flush();
                    LastFlushed_.store(NProfiling::GetCpuInstant());
                } catch (const std::exception& ex) {
                    error = ex;
                }
            }
        }

        {
            TGuard<TSpinLock> guard(SpinLock_);
            FlushedRecordCount_ += FlushQueue_.size();
            FlushQueue_.clear();
        }

        flushPromise.Set(error);
    }
};

typedef TIntrusivePtr<TFileChangelogQueue> TFileChangelogQueuePtr;

////////////////////////////////////////////////////////////////////////////////

class TFileChangelogDispatcher::TImpl
    : public TRefCounted
{
public:
    TImpl(
        const NChunkClient::IIOEnginePtr& ioEngine,
        const TFileChangelogDispatcherConfigPtr config,
        const TString& threadName,
        const TProfiler& profiler)
        : IOEngine_(ioEngine)
        , Config_(config)
        , ProcessQueuesCallback_(BIND(&TImpl::ProcessQueues, MakeWeak(this)))
        , ActionQueue_(New<TActionQueue>(threadName))
        , PeriodicExecutor_(New<TPeriodicExecutor>(
            ActionQueue_->GetInvoker(),
            ProcessQueuesCallback_,
            FlushThreadQuantum))
        , Profiler(profiler)
        , RecordCounter_("/records")
        , ByteCounter_("/bytes")
    {
        PeriodicExecutor_->Start();
    }

    ~TImpl()
    {
        Shutdown();
    }

    void Shutdown()
    {
        PeriodicExecutor_->Stop();
        ActionQueue_->Shutdown();
    }

    IInvokerPtr GetInvoker()
    {
        return ActionQueue_->GetInvoker();
    }

    TFileChangelogQueuePtr CreateQueue(TSyncFileChangelogPtr syncChangelog)
    {
        return New<TFileChangelogQueue>(std::move(syncChangelog), Profiler, GetInvoker());
    }

    void RegisterQueue(TFileChangelogQueuePtr queue)
    {
        queue->GetInvoker()->Invoke(BIND(&TImpl::DoRegisterQueue, MakeStrong(this), queue));
    }

    void UnregisterQueue(TFileChangelogQueuePtr queue)
    {
        queue->GetInvoker()->Invoke(BIND(&TImpl::DoUnregisterQueue, MakeStrong(this), queue));
    }

    TFuture<void> Append(TFileChangelogQueuePtr queue, const TSharedRef& record)
    {
        auto result = queue->AsyncAppend(record);
        queue->Wakeup();
        Profiler.Increment(RecordCounter_);
        Profiler.Increment(ByteCounter_, record.Size());
        return result;
    }

    TFuture<std::vector<TSharedRef>> Read(
        TFileChangelogQueuePtr queue,
        int firstRecordId,
        int maxRecords,
        i64 maxBytes)
    {
        return BIND(&TImpl::DoRead, MakeStrong(this))
            .AsyncVia(queue->GetInvoker())
            .Run(std::move(queue), firstRecordId, maxRecords, maxBytes);
    }

    TFuture<void> Flush(TFileChangelogQueuePtr queue)
    {
        auto result = queue->AsyncFlush();
        queue->Wakeup();
        return result;
    }

    TFuture<void> ForceFlush(TFileChangelogQueuePtr queue)
    {
        auto result = queue->AsyncFlush();
        queue->GetInvoker()->Invoke(BIND(&TFileChangelogQueue::Process, queue));
        return result;
    }

    TFuture<void> Truncate(TFileChangelogQueuePtr queue, int recordCount)
    {
        return BIND(&TImpl::DoTruncate, MakeStrong(this))
            .AsyncVia(queue->GetInvoker())
            .Run(std::move(queue), recordCount);
    }

    TFuture<void> Close(TFileChangelogQueuePtr queue)
    {
        return BIND(&TImpl::DoClose, MakeStrong(this))
            .AsyncVia(queue->GetInvoker())
            .Run(std::move(queue));
    }

    TFuture<void> Preallocate(TFileChangelogQueuePtr queue, size_t size)
    {
        return BIND(&TImpl::DoPreallocate, MakeStrong(this))
            .AsyncVia(queue->GetInvoker())
            .Run(std::move(queue), size);
    }

    TFuture<void> FlushChangelogs()
    {
        return BIND(&TImpl::DoFlushChangelogs, MakeStrong(this))
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run();
    }

    const NChunkClient::IIOEnginePtr& GetIOEngine() const
    {
        return IOEngine_;
    }

private:
    const NChunkClient::IIOEnginePtr IOEngine_;
    const TFileChangelogDispatcherConfigPtr Config_;
    const TClosure ProcessQueuesCallback_;

    const TActionQueuePtr ActionQueue_;
    const TPeriodicExecutorPtr PeriodicExecutor_;

    const TProfiler Profiler;

    THashSet<TFileChangelogQueuePtr> Queues_;

    TMonotonicCounter RecordCounter_;
    TMonotonicCounter ByteCounter_;

    void ProcessQueues()
    {
        for (const auto& queue : Queues_) {
            queue->Wakeup();
        }
    }


    void DoRegisterQueue(const TFileChangelogQueuePtr& queue)
    {
        YCHECK(Queues_.insert(queue).second);
        ProfileQueues();
        LOG_DEBUG("Changelog queue registered (Path: %v)",
            queue->GetChangelog()->GetFileName());

        // See Wakeup.
        queue->Process();
    }

    void DoUnregisterQueue(const TFileChangelogQueuePtr& queue)
    {
        YCHECK(!queue->HasUnflushedRecords());
        YCHECK(Queues_.erase(queue) == 1);
        ShrinkHashTable(&Queues_);
        ProfileQueues();
        LOG_DEBUG("Changelog queue unregistered (Path: %v)",
            queue->GetChangelog()->GetFileName());
    }

    void ProfileQueues()
    {
        Profiler.Enqueue("/queue_count", Queues_.size(), EMetricType::Gauge);
    }

    std::vector<TSharedRef> DoRead(
        const TFileChangelogQueuePtr& queue,
        int firstRecordId,
        int maxRecords,
        i64 maxBytes)
    {
        auto records = queue->Read(firstRecordId, maxRecords, maxBytes);
        Profiler.Enqueue("/changelog_read_record_count", records.size(), EMetricType::Gauge);
        Profiler.Enqueue("/changelog_read_size", GetByteSize(records), EMetricType::Gauge);
        return records;
    }

    void DoTruncate(
        const TFileChangelogQueuePtr& queue,
        int recordCount)
    {
        YCHECK(!queue->HasUnflushedRecords());
        PROFILE_TIMING("/changelog_truncate_io_time") {
            const auto& changelog = queue->GetChangelog();
            changelog->Truncate(recordCount);
        }
    }

    void DoClose(const TFileChangelogQueuePtr& queue)
    {
        YCHECK(!queue->HasUnflushedRecords());
        PROFILE_TIMING("/changelog_close_io_time") {
            const auto& changelog = queue->GetChangelog();
            changelog->Close();
        }
    }

    void DoPreallocate(const TFileChangelogQueuePtr& queue, size_t size)
    {
        YCHECK(!queue->HasUnflushedRecords());
        PROFILE_TIMING("/changelog_preallocate_io_time") {
            const auto& changelog = queue->GetChangelog();
            changelog->Preallocate(size);
        }
    }

    TFuture<void> DoFlushChangelogs()
    {
        std::vector<TFuture<void>> flushResults;
        for (const auto& queue : Queues_) {
            flushResults.push_back(queue->AsyncFlush());
        }
        return Combine(flushResults);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFileChangelog
    : public IChangelog
{
public:
    TFileChangelog(
        TFileChangelogDispatcher::TImplPtr impl,
        TFileChangelogConfigPtr config,
        TSyncFileChangelogPtr changelog)
        : DispatcherImpl_(std::move(impl))
        , Config_(std::move(config))
        , Queue_(DispatcherImpl_->CreateQueue(changelog))
        , RecordCount_(changelog->GetRecordCount())
        , DataSize_(changelog->GetDataSize())
    {
        DispatcherImpl_->RegisterQueue(Queue_);
    }

    ~TFileChangelog()
    {
        LOG_DEBUG("Destroying changelog queue (Path: %v)",
            Queue_->GetChangelog()->GetFileName());
        Close();
        DispatcherImpl_->UnregisterQueue(Queue_);
    }

    virtual int GetRecordCount() const override
    {
        return RecordCount_;
    }

    virtual i64 GetDataSize() const override
    {
        return DataSize_;
    }

    virtual const TChangelogMeta& GetMeta() const override
    {
        return Queue_->GetChangelog()->GetMeta();
    }

    virtual TFuture<void> Append(const TSharedRef& data) override
    {
        YCHECK(!Closed_ && !Truncated_);
        RecordCount_ += 1;
        DataSize_ += data.Size();
        return DispatcherImpl_->Append(Queue_, data);
    }

    virtual TFuture<void> Flush() override
    {
        return DispatcherImpl_->Flush(Queue_);
    }

    virtual TFuture<std::vector<TSharedRef>> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes) const override
    {
        YCHECK(firstRecordId >= 0);
        YCHECK(maxRecords >= 0);
        YCHECK(maxBytes >= 0);
        return DispatcherImpl_->Read(
            Queue_,
            firstRecordId,
            maxRecords,
            maxBytes);
    }

    virtual TFuture<void> Truncate(int recordCount) override
    {
        YCHECK(recordCount <= RecordCount_);
        RecordCount_ = recordCount;
        Truncated_ = true;
        // NB: Ignoring the result seems fine since TSyncFileChangelog
        // will propagate any possible error as the result of all further calls.
        DispatcherImpl_->ForceFlush(Queue_);
        return DispatcherImpl_->Truncate(Queue_, recordCount);
    }

    virtual TFuture<void> Close() override
    {
        Closed_ = true;
        // NB: See #Truncate above.
        DispatcherImpl_->ForceFlush(Queue_);
        return DispatcherImpl_->Close(Queue_);
    }

    virtual TFuture<void> Preallocate(size_t size) override
    {
        return DispatcherImpl_->Preallocate(Queue_, size);
    }

private:
    const TFileChangelogDispatcher::TImplPtr DispatcherImpl_;
    const TFileChangelogConfigPtr Config_;

    const TFileChangelogQueuePtr Queue_;

    bool Closed_ = false;
    bool Truncated_ = false;

    std::atomic<int> RecordCount_;
    std::atomic<i64> DataSize_;

};

DEFINE_REFCOUNTED_TYPE(TFileChangelog)

////////////////////////////////////////////////////////////////////////////////

TFileChangelogDispatcher::TFileChangelogDispatcher(
    const NChunkClient::IIOEnginePtr& ioEngine,
    const TFileChangelogDispatcherConfigPtr& config,
    const TString& threadName,
    const TProfiler& profiler)
    : Impl_(New<TImpl>(
        ioEngine,
        config,
        threadName,
        profiler))
{ }

TFileChangelogDispatcher::~TFileChangelogDispatcher() = default;

IInvokerPtr TFileChangelogDispatcher::GetInvoker()
{
    return Impl_->GetInvoker();
}

IChangelogPtr TFileChangelogDispatcher::CreateChangelog(
    const TString& path,
    const TChangelogMeta& meta,
    const TFileChangelogConfigPtr& config)
{
    auto syncChangelog = New<TSyncFileChangelog>(Impl_->GetIOEngine(), path, config);
    syncChangelog->Create(meta);

    return New<TFileChangelog>(Impl_, config, syncChangelog);
}

IChangelogPtr TFileChangelogDispatcher::OpenChangelog(
    const TString& path,
    const TFileChangelogConfigPtr& config)
{
    auto syncChangelog = New<TSyncFileChangelog>(Impl_->GetIOEngine(), path, config);
    syncChangelog->Open();

    return New<TFileChangelog>(Impl_, config, syncChangelog);
}

TFuture<void> TFileChangelogDispatcher::FlushChangelogs()
{
    return Impl_->FlushChangelogs();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

