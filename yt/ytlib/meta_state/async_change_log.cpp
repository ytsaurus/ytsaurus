#include "stdafx.h"
#include "async_change_log.h"
#include "meta_version.h"
#include "change_log.h"
#include "private.h"

#include <core/misc/singleton.h>

#include <core/concurrency/thread_affinity.h>

#include <core/profiling/profiler.h>

#include <util/system/thread.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = MetaStateLogger;
static auto& Profiler = MetaStateProfiler;

////////////////////////////////////////////////////////////////////////////////

class TAsyncChangeLog::TChangeLogQueue
    : public TRefCounted
{
public:
    // Guarded by TImpl::SpinLock
    TAtomic UseCount;

    explicit TChangeLogQueue(TChangeLogPtr changeLog)
        : UseCount(0)
        , ChangeLog(changeLog)
        , FlushedRecordCount(changeLog->GetRecordCount())
        , Promise(NewPromise())
    { }

    TFuture<void> Append(int recordIndex, const TSharedRef& data)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock);

        if (recordIndex != DoGetRecordCount()) {
            LOG_FATAL("Unexpected record id in changelog %d: expected %d, got %d",
                ChangeLog->GetId(),
                DoGetRecordCount(),
                recordIndex);
        }

        AppendQueue.push_back(data);

        YCHECK(Promise);
        return Promise;
    }

    void Flush()
    {
        VERIFY_THREAD_AFFINITY(Flush);

        TPromise<void> promise;
        {
            TGuard<TSpinLock> guard(SpinLock);

            YCHECK(FlushQueue.empty());
            FlushQueue.swap(AppendQueue);

            YCHECK(Promise);
            promise = Promise;
            Promise = NewPromise();
        }

        // In addition to making this code run a tiny bit faster,
        // this check also prevents us from calling TChangeLog::Append for an already finalized changelog
        // (its queue may still be present in the map).
        if (!FlushQueue.empty()) {
            PROFILE_TIMING ("/changelog_flush_io_time") {
                ChangeLog->Append(FlushedRecordCount, FlushQueue);
                ChangeLog->Flush();
            }
        }

        promise.Set();

        {
            TGuard<TSpinLock> guard(SpinLock);
            FlushedRecordCount += FlushQueue.size();
            FlushQueue.clear();
        }
    }

    void WaitUntilFlushed()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        PROFILE_TIMING ("/changelog_flush_wait_time") {
            TPromise<void> promise;
            {
                TGuard<TSpinLock> guard(SpinLock);
                if (FlushQueue.empty() && AppendQueue.empty()) {
                    return;
                }
                promise = Promise;
            }
            promise.Get();
        }
    }

    int GetRecordCount()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock);
        return DoGetRecordCount();
    }

    bool TrySweep()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        {
            TGuard<TSpinLock> guard(SpinLock);

            if (!AppendQueue.empty() || !FlushQueue.empty()) {
                return false;
            }

            if (UseCount != 0) {
                return false;
            }
        }

        Promise.Set();
        Promise.Reset();

        return true;
    }

    // Can return less records than recordCount
    void Read(int recordIndex, int recordCount, i64 maxSize, std::vector<TSharedRef>* result)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // First in-memory record index.
        int flushedRecordCount;

        // First take in-memory records (tail part).
        PROFILE_TIMING ("/changelog_read_copy_time") {
            TGuard<TSpinLock> guard(SpinLock);
            flushedRecordCount = FlushedRecordCount;

            CopyRecords(
                FlushedRecordCount,
                FlushQueue,
                recordIndex,
                recordCount,
                result);

            CopyRecords(
                FlushedRecordCount + FlushQueue.size(),
                AppendQueue,
                recordIndex,
                recordCount,
                result);
        }

        // Then take on-disk records, if needed (head part).
        PROFILE_TIMING ("/changelog_read_io_time") {
            if (recordIndex < flushedRecordCount) {
                std::vector<TSharedRef> diskResult;
                int neededRecordCount = std::min(recordCount, flushedRecordCount - recordIndex);
                ChangeLog->Read(recordIndex, neededRecordCount, maxSize, &diskResult);
                // Combine head + tail.
                diskResult.insert(diskResult.end(), result->begin(), result->end());
                result->swap(diskResult);
            }
        }

        // Trim to enforce size limit.
        i64 resultSize = 0;
        {
            auto it = result->begin();
            while (it != result->end() && resultSize <= maxSize) {
                resultSize += it->Size();
                ++it;
            }
            result->erase(it, result->end());
        }

        Profiler.Enqueue("/changelog_read_record_count", result->size());
        Profiler.Enqueue("/changelog_read_size", resultSize);
    }

private:
    int DoGetRecordCount() const
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock);
        return FlushedRecordCount + FlushQueue.size() + AppendQueue.size();
    }

    static void CopyRecords(
        int firstRecordIndex,
        const std::vector<TSharedRef>& records,
        int neededFirstRecordIndex,
        int neededRecordCount,
        std::vector<TSharedRef>* result)
    {
        int size = records.size();
        int beginIndex = neededFirstRecordIndex - firstRecordIndex;
        int endIndex = neededFirstRecordIndex + neededRecordCount - firstRecordIndex;
        auto beginIt = records.begin() + std::min(std::max(beginIndex, 0), size);
        auto endIt = records.begin() + std::min(std::max(endIndex, 0), size);
        if (endIt != beginIt) {
            result->insert(
                result->end(),
                beginIt,
                endIt);
        }
    }
    TChangeLogPtr ChangeLog;

    TSpinLock SpinLock;
    int FlushedRecordCount;
    std::vector<TSharedRef> AppendQueue;
    std::vector<TSharedRef> FlushQueue;
    TPromise<void> Promise;

    DECLARE_THREAD_AFFINITY_SLOT(Flush);
};

////////////////////////////////////////////////////////////////////////////////

class TAsyncChangeLog::TImpl
{
public:
    static TImpl* Get()
    {
        return Singleton<TImpl>();
    }

    TImpl()
        : Thread(ThreadFunc, static_cast<void*>(this))
        , WakeupEvent(Event::rManual)
        , Finished(false)
        , RecordCounter("/record_rate")
        , SizeCounter("/record_throughput")
    {
        Thread.Start();
    }

    ~TImpl()
    {
        Shutdown();
    }

    TFuture<void> Append(
        TChangeLogPtr changeLog,
        int recordIndex,
        const TSharedRef& data)
    {
        LOG_TRACE("Async changelog record is enqueued at version %s",
            ~TMetaVersion(changeLog->GetId(), recordIndex).ToString());

        auto queue = GetQueueAndLock(changeLog);
        auto result = queue->Append(recordIndex, data);
        UnlockQueue(queue);
        WakeupEvent.Signal();

        Profiler.Increment(RecordCounter);
        Profiler.Increment(SizeCounter, data.Size());

        return result;
    }

    void Read(
        TChangeLogPtr changeLog,
        int recordIndex,
        int recordCount,
        i64 maxSize,
        std::vector<TSharedRef>* result)
    {
        YCHECK(recordIndex >= 0);
        YCHECK(recordCount >= 0);
        YCHECK(result);

        if (recordCount == 0) {
            return;
        }

        auto queue = FindQueueAndLock(changeLog);
        if (queue) {
            queue->Read(recordIndex, recordCount, maxSize, result);
            UnlockQueue(queue);
        } else {
            PROFILE_TIMING ("/changelog_read_io_time") {
                changeLog->Read(recordIndex, recordCount, maxSize, result);
            }
        }
    }

    void Flush(TChangeLogPtr changeLog)
    {
        auto queue = FindQueue(changeLog);
        if (queue) {
            queue->WaitUntilFlushed();
        }

        PROFILE_TIMING ("/changelog_flush_io_time") {
            changeLog->Flush();
        }
    }

    int GetRecordCount(TChangeLogPtr changeLog)
    {
        auto queue = FindQueueAndLock(changeLog);
        if (queue) {
            auto result = queue->GetRecordCount();
            UnlockQueue(queue);
            return result;
        } else {
            return changeLog->GetRecordCount();
        }
    }

    void Finalize(TChangeLogPtr changeLog)
    {
        Flush(changeLog);

        PROFILE_TIMING ("/changelog_finalize_time") {
            changeLog->Finalize();
        }

        LOG_DEBUG("Async changelog %d is finalized", changeLog->GetId());
    }

    void Truncate(TChangeLogPtr changeLog, int truncatedRecordCount)
    {
        // TODO: Later on this can be improved to asynchronous behavior by
        // getting rid of explicit synchronization.
        Flush(changeLog);

        PROFILE_TIMING ("/changelog_truncate_time") {
            changeLog->Truncate(truncatedRecordCount);
        }
    }

    void Shutdown()
    {
        Finished = true;
        WakeupEvent.Signal();
        Thread.Join();
    }

private:
    TChangeLogQueuePtr FindQueue(TChangeLogPtr changeLog) const
    {
        TGuard<TSpinLock> guard(SpinLock);
        auto it = ChangeLogQueues.find(changeLog);
        return it == ChangeLogQueues.end() ? NULL : it->second;
    }

    TChangeLogQueuePtr FindQueueAndLock(TChangeLogPtr changeLog) const
    {
        TGuard<TSpinLock> guard(SpinLock);
        auto it = ChangeLogQueues.find(changeLog);
        if (it == ChangeLogQueues.end()) {
            return NULL;
        }
        auto& queue = it->second;
        AtomicIncrement(queue->UseCount);
        return queue;
    }

    TChangeLogQueuePtr GetQueueAndLock(TChangeLogPtr changeLog)
    {
        TGuard<TSpinLock> guard(SpinLock);
        TChangeLogQueuePtr queue;

        auto it = ChangeLogQueues.find(changeLog);
        if (it != ChangeLogQueues.end()) {
            queue = it->second;
        } else {
            queue = New<TChangeLogQueue>(changeLog);
            YCHECK(ChangeLogQueues.insert(std::make_pair(changeLog, queue)).second);
        }

        AtomicIncrement(queue->UseCount);
        return queue;
    }

    void UnlockQueue(const TChangeLogQueuePtr& queue)
    {
        AtomicDecrement(queue->UseCount);
    }

    void FlushQueues()
    {
        // Take a snapshot.
        std::vector<TChangeLogQueuePtr> queues;
        {
            TGuard<TSpinLock> guard(SpinLock);
            FOREACH (const auto& it, ChangeLogQueues) {
                queues.push_back(it.second);
            }
        }

        // Flush the queues.
        FOREACH (auto& queue, queues) {
            queue->Flush();
        }
    }

    // Returns True if there is any unswept queue left in the map.
    bool CleanQueues()
    {
        TGuard<TSpinLock> guard(SpinLock);

        auto it = ChangeLogQueues.begin();
        while (it != ChangeLogQueues.end()) {
            auto jt = it++;
            auto queue = jt->second;
            if (queue->TrySweep()) {
                LOG_DEBUG("Async changelog queue %d was swept", jt->first->GetId());
                ChangeLogQueues.erase(jt);
            }
        }

        return !ChangeLogQueues.empty();
    }

    // Returns True if there is any unswept queue left in the map.
    bool FlushAndClean()
    {
        FlushQueues();
        return CleanQueues();
    }

    static void* ThreadFunc(void* param)
    {
        auto* impl = (TImpl*) param;
        impl->ThreadMain();
        return NULL;
    }

    void ThreadMain()
    {
        NThread::SetCurrentThreadName("AsyncChangeLog");

        while (!Finished) {
            if (FlushAndClean())
                continue;

            WakeupEvent.Reset();

            if (FlushAndClean())
                continue;

            if (!Finished) {
                WakeupEvent.Wait();
            }
        }
    }

    TSpinLock SpinLock;
    yhash_map<TChangeLogPtr, TChangeLogQueuePtr> ChangeLogQueues;

    TThread Thread;
    Event WakeupEvent;
    volatile bool Finished;

    NProfiling::TRateCounter RecordCounter;
    NProfiling::TRateCounter SizeCounter;

};

////////////////////////////////////////////////////////////////////////////////

TAsyncChangeLog::TAsyncChangeLog(TChangeLogPtr changeLog)
    : ChangeLog(changeLog)
{
    YCHECK(changeLog);
}

TAsyncChangeLog::~TAsyncChangeLog()
{ }

TFuture<void> TAsyncChangeLog::Append(
    int recordIndex,
    const TSharedRef& data)
{
    return TImpl::Get()->Append(ChangeLog, recordIndex, data);
}

void TAsyncChangeLog::Finalize()
{
    TImpl::Get()->Finalize(ChangeLog);
}

void TAsyncChangeLog::Flush()
{
    TImpl::Get()->Flush(ChangeLog);
}

void TAsyncChangeLog::Read(
    int firstRecordIndex,
    int recordCount,
    i64 maxSize,
    std::vector<TSharedRef>* result) const
{
    TImpl::Get()->Read(ChangeLog, firstRecordIndex, recordCount, maxSize, result);
}

int TAsyncChangeLog::GetId() const
{
    return ChangeLog->GetId();
}

int TAsyncChangeLog::GetRecordCount() const
{
    return TImpl::Get()->GetRecordCount(ChangeLog);
}

const TEpochId& TAsyncChangeLog::GetEpoch() const
{
    return ChangeLog->GetEpoch();
}

int TAsyncChangeLog::GetPrevRecordCount() const
{
    return ChangeLog->GetPrevRecordCount();
}

bool TAsyncChangeLog::IsFinalized() const
{
    return ChangeLog->IsFinalized();
}

void TAsyncChangeLog::Truncate(int recordCount)
{
    return TImpl::Get()->Truncate(ChangeLog, recordCount);
}

void TAsyncChangeLog::Shutdown()
{
    TImpl::Get()->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT

