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

#include <atomic>

#ifdef _linux_
    #include <unistd.h>
    // Copied from linux/ioprio.h
    #define IOPRIO_CLASS_SHIFT              (13)
    #define IOPRIO_PRIO_VALUE(class, data)  (((class) << IOPRIO_CLASS_SHIFT) | data)
    #define IOPRIO_WHO_PROCESS (1)
#endif

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
        const TProfiler& profiler)
        : Changelog_(std::move(changelog))
        , Profiler(profiler)
        , FlushedRecordCount_(Changelog_->GetRecordCount())
    { }

    TSyncFileChangelogPtr GetChangelog()
    {
        return Changelog_;
    }


    void Lock()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ++UseCount_;
    }

    void Unlock()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        --UseCount_;
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

        FlushForced_ = true;
        return FlushPromise_;
    }


    bool HasPendingFlushes()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // Unguarded access seems OK.
        auto config = Changelog_->GetConfig();
        if (ByteSize_ >= config->FlushBufferSize) {
            return true;
        }

        if (Changelog_->GetLastFlushed() < TInstant::Now() - config->FlushPeriod) {
            return true;
        }

        if (FlushForced_) {
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

    TPromise<void> TrySweep()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TPromise<void> promise;
        {
            TGuard<TSpinLock> guard(SpinLock_);

            if (!AppendQueue_.empty() || !FlushQueue_.empty()) {
                return TPromise<void>();
            }

            if (UseCount_.load() > 0) {
                return TPromise<void>();
            }

            promise = FlushPromise_;
            FlushPromise_.Reset();
            FlushForced_ = false;
        }

        return promise;
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

private:
    const TSyncFileChangelogPtr Changelog_;
    const TProfiler Profiler;

    std::atomic<int> UseCount_ = {0};

    TSpinLock SpinLock_;

    //! Number of records flushed to the underlying sync changelog.
    int FlushedRecordCount_ = 0;
    //! These records are currently being flushed to the underlying sync changelog and
    //! immediately follow the flushed part.
    std::vector<TSharedRef> FlushQueue_;
    //! Newly appended records go here. These records immediately follow the flush part.
    std::vector<TSharedRef> AppendQueue_;

    i64 ByteSize_ = 0;

    TPromise<void> FlushPromise_ = NewPromise<void>();
    bool FlushForced_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(SyncThread);


    void SyncFlush()
    {
        TPromise<void> flushPromise;
        {
            TGuard<TSpinLock> guard(SpinLock_);

            YCHECK(FlushQueue_.empty());
            FlushQueue_.swap(AppendQueue_);
            ByteSize_ = 0;

            YCHECK(FlushPromise_);
            flushPromise = FlushPromise_;
            FlushPromise_ = NewPromise<void>();
            FlushForced_ = false;
        }

        TError error;
        if (!FlushQueue_.empty()) {
            PROFILE_TIMING("/changelog_flush_io_time") {
                try {
                    Changelog_->Append(FlushedRecordCount_, FlushQueue_);
                    Changelog_->Flush();
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
        const TFileChangelogDispatcherConfigPtr config,
        const Stroka& threadName,
        const TProfiler& profiler)
        : Config_(config)
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
#ifdef _linux_
        GetInvoker()->Invoke(BIND([=, this_ = MakeStrong(this)] () {
            int result = syscall(
                SYS_ioprio_get,
                IOPRIO_WHO_PROCESS,
                0,
                IOPRIO_PRIO_VALUE(Config_->IOClass,  Config_->IOPriority));
            if (result == -1) {
                LOG_ERROR(TError::FromSystem(), "Failed to set IO priority for changelog flush thread");
             }
        }));
#endif
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

    TFuture<void> Append(
        TSyncFileChangelogPtr changelog,
        const TSharedRef& record)
    {
        auto queue = GetAndLockQueue(changelog);
        auto guard = Finally([&] () {
            queue->Unlock();
        });

        auto result = queue->AsyncAppend(record);

        Wakeup();

        Profiler.Increment(RecordCounter_);
        Profiler.Increment(ByteCounter_, record.Size());

        return result;
    }

    TFuture<std::vector<TSharedRef>> Read(
        TSyncFileChangelogPtr changelog,
        int firstRecordId,
        int maxRecords,
        i64 maxBytes)
    {
        return BIND(&TImpl::DoRead, MakeStrong(this))
            .AsyncVia(GetInvoker())
            .Run(changelog, firstRecordId, maxRecords, maxBytes);
    }

    TFuture<void> Flush(TSyncFileChangelogPtr changelog)
    {
        auto queue = FindAndLockQueue(changelog);
        if (!queue) {
            return VoidFuture;
        }

        auto guard = Finally([&] () {
            queue->Unlock();
        });

        auto result = queue->AsyncFlush();

        Wakeup();

        return result;
    }

    TFuture<void> Truncate(TSyncFileChangelogPtr changelog, int recordCount)
    {
        return BIND(&TImpl::DoTruncate, MakeStrong(this))
            .AsyncVia(GetInvoker())
            .Run(changelog, recordCount);
    }

    TFuture<void> Close(TSyncFileChangelogPtr changelog)
    {
        return BIND(&TImpl::DoClose, MakeStrong(this))
            .AsyncVia(GetInvoker())
            .Run(changelog);
    }

    TFuture<void> FlushAll()
    {
        auto queues = ListQueues();
        std::vector<TFuture<void>> flushResults;
        for (const auto& queue : queues) {
            flushResults.push_back(queue->AsyncFlush());
        }
        return Combine(flushResults);
    }

private:
    const TFileChangelogDispatcherConfigPtr Config_;
    const TClosure ProcessQueuesCallback_;

    const TActionQueuePtr ActionQueue_;
    const TPeriodicExecutorPtr PeriodicExecutor_;

    const TProfiler Profiler;

    std::atomic<bool> ProcessQueuesCallbackPending_ = {false};

    TSpinLock SpinLock_;
    yhash_map<TSyncFileChangelogPtr, TFileChangelogQueuePtr> QueueMap_;

    TSimpleCounter RecordCounter_;
    TSimpleCounter ByteCounter_;


    std::vector<TFileChangelogQueuePtr> ListQueues()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        return GetValues(QueueMap_);
    }

    bool HasUnflushedRecords(TSyncFileChangelogPtr changelog)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        auto it = QueueMap_.find(changelog);
        if (it == QueueMap_.end()) {
            return false;
        }
        const auto& queue = it->second;
        return queue->HasUnflushedRecords();
    }

    TFileChangelogQueuePtr FindAndLockQueue(TSyncFileChangelogPtr changelog)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        auto it = QueueMap_.find(changelog);
        if (it == QueueMap_.end()) {
            return nullptr;
        }

        auto queue = it->second;
        queue->Lock();
        return queue;
    }

    TFileChangelogQueuePtr GetAndLockQueue(TSyncFileChangelogPtr changelog)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        TFileChangelogQueuePtr queue;

        auto it = QueueMap_.find(changelog);
        if (it != QueueMap_.end()) {
            queue = it->second;
        } else {
            queue = New<TFileChangelogQueue>(changelog, Profiler);
            YCHECK(QueueMap_.insert(std::make_pair(changelog, queue)).second);
            LOG_DEBUG("Changelog queue created (Path: %v)",
                changelog->GetFileName());
        }

        queue->Lock();
        return queue;
    }


    void RunPendingFlushes()
    {
        // Take a snapshot.
        std::vector<TFileChangelogQueuePtr> queues;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            for (const auto& pair : QueueMap_) {
                const auto& queue = pair.second;
                if (queue->HasPendingFlushes()) {
                    queues.push_back(queue);
                }
            }
        }

        // Run pending flushes for the queues in the snapshot.
        for (const auto& queue : queues) {
            queue->RunPendingFlushes();
        }
    }

    void SweepQueues()
    {
        std::vector<TPromise<void>> promises;

        {
            TGuard<TSpinLock> guard(SpinLock_);
            auto it = QueueMap_.begin();
            while (it != QueueMap_.end()) {
                auto jt = it++;
                auto queue = jt->second;
                auto promise = queue->TrySweep();
                if (promise) {
                    promises.push_back(promise);
                    QueueMap_.erase(jt);
                    LOG_DEBUG("Changelog queue removed (Path: %v)",
                        queue->GetChangelog()->GetFileName());
                }
            }
        }

        for (auto& promise : promises) {
            promise.Set(TError());
        }
    }

    void Wakeup()
    {
        if (ProcessQueuesCallbackPending_.load(std::memory_order_relaxed)) {
            return;
        }

        bool expected = false;
        if (ProcessQueuesCallbackPending_.compare_exchange_strong(expected, true)) {
            ActionQueue_->GetInvoker()->Invoke(ProcessQueuesCallback_);
        }
    }

    void ProcessQueues()
    {
        ProcessQueuesCallbackPending_ = false;
        RunPendingFlushes();
        SweepQueues();
    }


    std::vector<TSharedRef> DoRead(
        TSyncFileChangelogPtr changelog,
        int firstRecordId,
        int maxRecords,
        i64 maxBytes)
    {
        if (maxRecords == 0) {
            return std::vector<TSharedRef>();
        }

        std::vector<TSharedRef> records;
        auto queue = FindAndLockQueue(changelog);
        if (queue) {
            auto guard = Finally([&] () {
                queue->Unlock();
            });
            records = queue->Read(firstRecordId, maxRecords, maxBytes);
        } else {
            if (!changelog->IsOpen()) {
                // NB: Reading from a changelog and closing it is inherently racy.
                // It is safe to report no records at this point since the client will be retrying its
                // requests anyway.
                return std::vector<TSharedRef>();
            }
            PROFILE_TIMING ("/changelog_read_io_time") {
                records = changelog->Read(firstRecordId, maxRecords, maxBytes);
            }
        }

        Profiler.Enqueue("/changelog_read_record_count", records.size(), EMetricType::Gauge);
        Profiler.Enqueue("/changelog_read_size", GetByteSize(records), EMetricType::Gauge);

        return records;
    }

    void DoTruncate(
        TSyncFileChangelogPtr changelog,
        int recordCount)
    {
        YCHECK(!HasUnflushedRecords(changelog));
        PROFILE_TIMING("/changelog_truncate_io_time") {
            changelog->Truncate(recordCount);
        }
    }

    void DoClose(TSyncFileChangelogPtr changelog)
    {
        YCHECK(!HasUnflushedRecords(changelog));
        PROFILE_TIMING("/changelog_close_io_time") {
            changelog->Close();
        }
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
        , Config_(config)
        , SyncChangelog_(changelog)
        , RecordCount_(changelog->GetRecordCount())
        , DataSize_(changelog->GetDataSize())
    { }

    ~TFileChangelog()
    {
        Close();
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
        return SyncChangelog_->GetMeta();
    }

    virtual TFuture<void> Append(const TSharedRef& data) override
    {
        YCHECK(!Closed_ && !Truncated_);
        RecordCount_ += 1;
        DataSize_ += data.Size();
        return DispatcherImpl_->Append(SyncChangelog_, data);
    }

    virtual TFuture<void> Flush() override
    {
        return DispatcherImpl_->Flush(SyncChangelog_);
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
            SyncChangelog_,
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
        Flush();
        return DispatcherImpl_->Truncate(SyncChangelog_, recordCount);
    }

    virtual TFuture<void> Close() override
    {
        Closed_ = true;
        // NB: See #Truncate above.
        Flush();
        return DispatcherImpl_->Close(SyncChangelog_);
    }

private:
    const TFileChangelogDispatcher::TImplPtr DispatcherImpl_;
    const TFileChangelogConfigPtr Config_;
    const TSyncFileChangelogPtr SyncChangelog_;

    bool Closed_ = false;
    bool Truncated_ = false;

    std::atomic<int> RecordCount_;
    std::atomic<i64> DataSize_;

};

DEFINE_REFCOUNTED_TYPE(TFileChangelog)

////////////////////////////////////////////////////////////////////////////////

TFileChangelogDispatcher::TFileChangelogDispatcher(
    TFileChangelogDispatcherConfigPtr config,
    const Stroka& threadName,
    const TProfiler& profiler)
    : Impl_(New<TImpl>(
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
    const Stroka& path,
    const TChangelogMeta& meta,
    TFileChangelogConfigPtr config)
{
    auto syncChangelog = New<TSyncFileChangelog>(path, config);
    syncChangelog->Create(meta);

    return New<TFileChangelog>(Impl_, config, syncChangelog);
}

IChangelogPtr TFileChangelogDispatcher::OpenChangelog(
    const Stroka& path,
    TFileChangelogConfigPtr config)
{
    auto syncChangelog = New<TSyncFileChangelog>(path, config);
    syncChangelog->Open();

    return New<TFileChangelog>(Impl_, config, syncChangelog);
}

TFuture<void> TFileChangelogDispatcher::FlushChangelogs()
{
    return Impl_->FlushAll();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

