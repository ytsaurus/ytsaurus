#include "stdafx.h"
#include "file_changelog_dispatcher.h"
#include "changelog.h"
#include "sync_file_changelog.h"
#include "config.h"
#include "private.h"

#include <core/misc/fs.h>

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/action_queue.h>
#include <core/concurrency/periodic_executor.h>

#include <atomic>

namespace NYT {
namespace NHydra {

using namespace NConcurrency;
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

static auto& Profiler = HydraProfiler;
static const auto& Logger = HydraLogger;

static const auto FlushThreadQuantum = TDuration::MilliSeconds(10);

////////////////////////////////////////////////////////////////////////////////

class TFileChangelogQueue
    : public TRefCounted
{
public:
    explicit TFileChangelogQueue(TSyncFileChangelogPtr changelog)
        : Changelog_(changelog)
        , UseCount_(0)
        , FlushedRecordCount_(changelog->GetRecordCount())
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


    TFuture<void> Append(TSharedRef data)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TFuture<void> result;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            YCHECK(
                !SealRequested_ &&
                !UnsealRequested_ &&
                !Sealed_ &&
                !CloseRequested_ &&
                !Closed_);
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

    TFuture<void> AsyncSeal(int recordCount)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TFuture<void> result;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            YCHECK(!SealRequested_ && !UnsealRequested_);
            SealRequested_ = true;
            SealRecordCount_ = recordCount;
            result = SealPromise_ = NewPromise<void>();
        }

        return result;
    }

    TFuture<void> AsyncUnseal()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TFuture<void> result;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            YCHECK(!SealRequested_ && !UnsealRequested_);
            UnsealRequested_ = true;
            result = UnsealPromise_ = NewPromise<void>();
        }

        return result;
    }

    TFuture<void> AsyncClose()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TFuture<void> result;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            YCHECK(!CloseRequested_);
            CloseRequested_ = true;
            result = ClosePromise_ = NewPromise<void>();
        }

        return result;
    }


    bool HasPendingActions()
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

        if (SealRequested_) {
            return true;
        }

        if (UnsealRequested_) {
            return true;
        }

        if (CloseRequested_) {
            return true;
        }

        return false;
    }

    void RunPendingActions()
    {
        VERIFY_THREAD_AFFINITY(SyncThread);

        SyncFlush();
        MaybeSyncSeal();
        MaybeSyncUnseal();
        MaybeSyncClose();
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

            if (SealRequested_ && !SealPromise_.IsSet()) {
                return TPromise<void>();
            }

            if (UnsealRequested_ && !UnsealPromise_.IsSet()) {
                return TPromise<void>();
            }

            if (CloseRequested_ && !ClosePromise_.IsSet()) {
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
        i64 readBytes = 0;

        auto appendRecord = [&] (const TSharedRef& record) {
            records.push_back(record);
            --needRecords;
            ++currentRecordId;
            needBytes -= record.Size();
            readBytes += record.Size();
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

        Profiler.Enqueue("/changelog_read_record_count", records.size());
        Profiler.Enqueue("/changelog_read_size", readBytes);

        return records;
    }

private:
    TSyncFileChangelogPtr Changelog_;

    std::atomic<int> UseCount_;

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

    TPromise<void> SealPromise_;
    bool SealRequested_ = false;
    int SealRecordCount_ = -1;

    TPromise<void> UnsealPromise_;
    bool UnsealRequested_ = false;

    TPromise<void> ClosePromise_;
    bool CloseRequested_ = false;

    bool Sealed_ = false;
    bool Closed_ = false;


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

        if (!FlushQueue_.empty()) {
            PROFILE_TIMING("/changelog_flush_io_time") {
                Changelog_->Append(FlushedRecordCount_, FlushQueue_);
                Changelog_->Flush();
            }
        }

        {
            TGuard<TSpinLock> guard(SpinLock_);
            FlushedRecordCount_ += FlushQueue_.size();
            FlushQueue_.clear();
        }

        flushPromise.Set(TError());
    }

    void SyncFlushAll()
    {
        while (true) {
            {
                TGuard<TSpinLock> guard(SpinLock_);
                if (AppendQueue_.empty())
                    break;
            }
            SyncFlush();
        }
    }


    void MaybeSyncSeal()
    {
        TPromise<void> promise;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (!SealRequested_)
                return;
            promise = SealPromise_;
            SealPromise_.Reset();
            SealRequested_ = false;
            Sealed_ = true;
        }

        SyncFlushAll();

        PROFILE_TIMING("/changelog_seal_io_time") {
            Changelog_->Seal(SealRecordCount_);
        }

        promise.Set(TError());
    }

    void MaybeSyncUnseal()
    {
        TPromise<void> promise;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (!UnsealRequested_)
                return;
            promise = UnsealPromise_;
            UnsealPromise_.Reset();
            UnsealRequested_ = false;
            Sealed_ = false;
        }

        PROFILE_TIMING("/changelog_unseal_io_time") {
            Changelog_->Unseal();
        }

        promise.Set(TError());
    }

    void MaybeSyncClose()
    {
        TPromise<void> promise;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (!CloseRequested_)
                return;
            promise = ClosePromise_;
            ClosePromise_.Reset();
            CloseRequested_ = false;
            Closed_ = true;
        }

        SyncFlushAll();

        PROFILE_TIMING("/changelog_close_io_time") {
            Changelog_->Close();
        }

        promise.Set(TError());
    }
};

typedef TIntrusivePtr<TFileChangelogQueue> TFileChangelogQueuePtr;

////////////////////////////////////////////////////////////////////////////////

class TFileChangelogDispatcher::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(const Stroka& threadName)
        : ProcessQueuesCallback_(BIND(&TImpl::ProcessQueues, MakeWeak(this)))
        , ActionQueue_(New<TActionQueue>(threadName))
        , PeriodicExecutor_(New<TPeriodicExecutor>(
            ActionQueue_->GetInvoker(),
            ProcessQueuesCallback_,
            FlushThreadQuantum))
        , RecordCounter_("/record_rate")
        , SizeCounter_("/record_throughput")
    {
        ProcessQueuesCallbackPending_ = false;
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
        auto queue = GetQueueAndLock(changelog);
        auto result = queue->Append(record);
        queue->Unlock();
        Wakeup();

        Profiler.Increment(RecordCounter_);
        Profiler.Increment(SizeCounter_, record.Size());

        return result;
    }

    TFuture<std::vector<TSharedRef>> Read(
        TSyncFileChangelogPtr changelog,
        int recordId,
        int maxRecords,
        i64 maxBytes)
    {
        YCHECK(recordId >= 0);
        YCHECK(maxRecords >= 0);

        return BIND(&TImpl::DoRead, MakeStrong(this))
            .AsyncVia(GetInvoker())
            .Run(changelog, recordId, maxRecords, maxBytes);
    }

    TFuture<void> Flush(TSyncFileChangelogPtr changelog)
    {
        auto queue = FindQueue(changelog);
        return queue ? queue->AsyncFlush() : VoidFuture;
    }

    TFuture<void> Seal(TSyncFileChangelogPtr changelog, int recordCount)
    {
        auto queue = GetQueueAndLock(changelog);
        auto result = queue->AsyncSeal(recordCount);
        queue->Unlock();
        Wakeup();
        return result;
    }

    TFuture<void> Unseal(TSyncFileChangelogPtr changelog)
    {
        auto queue = GetQueueAndLock(changelog);
        auto result = queue->AsyncUnseal();
        queue->Unlock();
        Wakeup();
        return result;
    }

    TFuture<void> Close(TSyncFileChangelogPtr changelog)
    {
        auto queue = GetQueueAndLock(changelog);
        auto result = queue->AsyncClose();
        queue->Unlock();
        Wakeup();
        return result;
    }

private:
    TClosure ProcessQueuesCallback_;
    std::atomic<bool> ProcessQueuesCallbackPending_;

    TActionQueuePtr ActionQueue_;
    TPeriodicExecutorPtr PeriodicExecutor_;

    TSpinLock SpinLock_;
    yhash_map<TSyncFileChangelogPtr, TFileChangelogQueuePtr> QueueMap_;

    NProfiling::TRateCounter RecordCounter_;
    NProfiling::TRateCounter SizeCounter_;


    TFileChangelogQueuePtr FindQueue(TSyncFileChangelogPtr changelog) const
    {
        TGuard<TSpinLock> guard(SpinLock_);
        auto it = QueueMap_.find(changelog);
        return it == QueueMap_.end() ? nullptr : it->second;
    }

    TFileChangelogQueuePtr FindQueueAndLock(TSyncFileChangelogPtr changelog) const
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

    TFileChangelogQueuePtr GetQueueAndLock(TSyncFileChangelogPtr changelog)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        TFileChangelogQueuePtr queue;

        auto it = QueueMap_.find(changelog);
        if (it != QueueMap_.end()) {
            queue = it->second;
        } else {
            queue = New<TFileChangelogQueue>(changelog);
            YCHECK(QueueMap_.insert(std::make_pair(changelog, queue)).second);
            LOG_DEBUG("Changelog queue created (Path: %v)",
                changelog->GetFileName());
        }

        queue->Lock();
        return queue;
    }

    void RunPendingActions()
    {
        // Take a snapshot.
        std::vector<TFileChangelogQueuePtr> queues;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            for (const auto& pair : QueueMap_) {
                const auto& queue = pair.second;
                if (queue->HasPendingActions()) {
                    queues.push_back(queue);
                }
            }
        }

        // Run pending actions for the queues in the snapshot.
        for (auto queue : queues) {
            queue->RunPendingActions();
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

        for (auto promise : promises) {
            promise.Set(TError());
        }
    }


    void Wakeup()
    {
        if (!ProcessQueuesCallbackPending_.load(std::memory_order_relaxed)) {
            bool expected = false;
            if (ProcessQueuesCallbackPending_.compare_exchange_strong(expected, true)) {
                ActionQueue_->GetInvoker()->Invoke(ProcessQueuesCallback_);
            }
        }
    }

    void ProcessQueues()
    {
        ProcessQueuesCallbackPending_ = false;
        RunPendingActions();
        SweepQueues();
    }


    std::vector<TSharedRef> DoRead(
        TSyncFileChangelogPtr changelog,
        int recordId,
        int maxRecords,
        i64 maxBytes)
    {
        if (maxRecords == 0) {
            return std::vector<TSharedRef>();
        }

        auto queue = FindQueueAndLock(changelog);
        if (queue) {
            auto records = queue->Read(recordId, maxRecords, maxBytes);
            queue->Unlock();
            return std::move(records);
        } else {
            PROFILE_TIMING ("/changelog_read_io_time") {
                return changelog->Read(recordId, maxRecords, maxBytes);
            }
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

    virtual bool IsSealed() const override
    {
        return SyncChangelog_->IsSealed();
    }

    virtual TFuture<void> Append(const TSharedRef& data) override
    {
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
        return DispatcherImpl_->Read(
            SyncChangelog_,
            firstRecordId,
            maxRecords,
            maxBytes);
    }

    virtual TFuture<void> Seal(int recordCount) override
    {
        YCHECK(recordCount <= RecordCount_);
        RecordCount_.store(recordCount);

        return DispatcherImpl_->Seal(SyncChangelog_, recordCount);
    }

    virtual TFuture<void> Unseal() override
    {
        return DispatcherImpl_->Unseal(SyncChangelog_);
    }

    virtual TFuture<void> Close() override
    {
        return DispatcherImpl_->Close(SyncChangelog_);
    }

private:
    TFileChangelogDispatcher::TImplPtr DispatcherImpl_;
    TFileChangelogConfigPtr Config_;
    TSyncFileChangelogPtr SyncChangelog_;

    std::atomic<int> RecordCount_;
    std::atomic<i64> DataSize_;

};

DEFINE_REFCOUNTED_TYPE(TFileChangelog)

////////////////////////////////////////////////////////////////////////////////

TFileChangelogDispatcher::TFileChangelogDispatcher(const Stroka& threadName)
    : Impl_(New<TImpl>(threadName))
{ }

TFileChangelogDispatcher::~TFileChangelogDispatcher()
{ }

void TFileChangelogDispatcher::Shutdown()
{
    Impl_->Shutdown();
}

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

