#include "stdafx.h"
#include "async_change_log.h"
#include "meta_version.h"
#include "change_log.h"

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/singleton.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/profiling/profiler.h>

#include <util/system/thread.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("MetaState");
static NProfiling::TProfiler Profiler("/meta_state");

////////////////////////////////////////////////////////////////////////////////

class TAsyncChangeLog::TChangeLogQueue
    : public TRefCounted
{
public:
    typedef TAsyncChangeLog::TAppendResult TAppendResult;

    // Guarded by TImpl::SpinLock
    TAtomic UseCount;

    TChangeLogQueue(const TChangeLogPtr& changeLog)
        : UseCount(0)
        , ChangeLog(changeLog)
        , FlushedRecordCount(changeLog->GetRecordCount())
        , Result(New<TAppendResult>())
    { }

    TAppendResult::TPtr Append(i32 recordId, const TSharedRef& data)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock);

        if (recordId != DoGetRecordCount()) {
            LOG_FATAL("Unexpected record id in changelog: expected %d, got %d (ChangeLogId: %d)",
                DoGetRecordCount(),
                recordId,
                ChangeLog->GetId());
        }

        AppendQueue.push_back(data);
        Profiler.Enqueue("/changelog_queue_size", AppendQueue.size());

        return Result;
    }

    void Flush()
    {
        VERIFY_THREAD_AFFINITY(Flush);

        TAppendResult::TPtr result;

        PROFILE_TIMING ("/changelog_flush_append_time") {
            TGuard<TSpinLock> guard(SpinLock);

            YASSERT(FlushQueue.empty());

            // In addition to making this code run a tiny bit faster,
            // this check also prevents from calling TChangeLog::Append for an already finalized changelog 
            // (its queue may still be present in the map).
            if (AppendQueue.empty()) {
                return;
            }

            FlushQueue.swap(AppendQueue);

            result = Result;
            Result = New<TAppendResult>();
        }

        PROFILE_TIMING ("/changelog_flush_io_time") {
            ChangeLog->Append(FlushedRecordCount, FlushQueue);
            ChangeLog->Flush();
        }

        result->Set(TVoid());

        {
            TGuard<TSpinLock> guard(SpinLock);
            FlushedRecordCount += FlushQueue.ysize();
            FlushQueue.clear();
        }
    }

    void WaitFlush()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        PROFILE_TIMING ("/changelog_flush_wait_time") {
            TAppendResult::TPtr result;
            {
                TGuard<TSpinLock> guard(SpinLock);
                if (FlushQueue.empty() && AppendQueue.empty()) {
                    return;
                }
                result = Result;
            }
            result->Get();
        }
    }

    int GetRecordCount()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock);
        return DoGetRecordCount();
    }

    bool IsEmpty()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock);
        return AppendQueue.ysize() == 0 && FlushQueue.ysize() == 0;
    }

    // Can return less records than recordCount
    void Read(i32 firstRecordId, i32 recordCount, yvector<TSharedRef>* result)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        i32 flushedRecordCount;

        PROFILE_TIMING ("/changelog_read_copy_time") {
            TGuard<TSpinLock> guard(SpinLock);
            flushedRecordCount = FlushedRecordCount;            
            
            CopyRecords(
                FlushedRecordCount,
                FlushQueue,
                firstRecordId,
                recordCount,
                result);

            CopyRecords(
                FlushedRecordCount + FlushQueue.ysize(),
                AppendQueue,
                firstRecordId,
                recordCount,
                result);
        }

        PROFILE_TIMING ("/changelog_read_io_time") {
            if (firstRecordId < flushedRecordCount) {
                yvector<TSharedRef> buffer;
                i32 neededRecordCount = Min(
                    recordCount,
                    flushedRecordCount - firstRecordId);
                ChangeLog->Read(firstRecordId, neededRecordCount, &buffer);
                YASSERT(buffer.ysize() == neededRecordCount);

                buffer.insert(
                    buffer.end(),
                    result->begin(),
                    result->end());

                result->swap(buffer);
            }
        }

        Profiler.Enqueue("/changelog_read_record_count", result->size());
    }

private:
    int DoGetRecordCount() const
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock);
        return FlushedRecordCount + FlushQueue.ysize() + AppendQueue.ysize();
    }

    static void CopyRecords(
        i32 firstRecordId,
        const yvector<TSharedRef>& records,
        i32 neededFirstRecordId,
        i32 neededRecordCount,
        yvector<TSharedRef>* result)
    {
        i32 size = records.ysize();
        i32 begin = neededFirstRecordId - firstRecordId;
        i32 end = neededFirstRecordId + neededRecordCount - firstRecordId;
        auto beginIt = records.begin() + Min(Max(begin, 0), size);
        auto endIt = records.begin() + Min(Max(end, 0), size);
        if (endIt != beginIt) {
            result->insert(
                result->end(),
                beginIt,
                endIt);
        }
    }
    
    TChangeLogPtr ChangeLog;
    
    TSpinLock SpinLock;
    i32 FlushedRecordCount;
    yvector<TSharedRef> AppendQueue;
    yvector<TSharedRef> FlushQueue;
    TAppendResult::TPtr Result;

    DECLARE_THREAD_AFFINITY_SLOT(Flush);
};

////////////////////////////////////////////////////////////////////////////////

class TAsyncChangeLog::TImpl
{
public:
    typedef TAsyncChangeLog::TAppendResult TAppendResult;

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

    TAppendResult::TPtr Append(
        const TChangeLogPtr& changeLog,
        i32 recordId,
        const TSharedRef& data)
    {
        LOG_TRACE("Async changelog record is enqueued (Version: %s)",
            ~TMetaVersion(changeLog->GetId(), recordId).ToString());

        auto queue = GetQueueAndLock(changeLog);
        auto result = queue->Append(recordId, data);
        UnlockQueue(queue);
        WakeupEvent.Signal();

        Profiler.Increment(RecordCounter);
        Profiler.Increment(SizeCounter, data.Size());

        return result;
    }

    void Read(
        const TChangeLogPtr& changeLog,
        i32 firstRecordId,
        i32 recordCount,
        yvector<TSharedRef>* result)
    {
        YASSERT(firstRecordId >= 0);
        YASSERT(recordCount >= 0);
        YASSERT(result);

        if (recordCount == 0) {
            return;
        }

        auto queue = FindQueueAndLock(changeLog);
        if (queue) {
            queue->Read(firstRecordId, recordCount, result);
            UnlockQueue(queue);
        } else {
            PROFILE_TIMING ("/changelog_read_io_time") {
                changeLog->Read(firstRecordId, recordCount, result);
            }
        }
    }

    void Flush(const TChangeLogPtr& changeLog)
    {
        auto queue = FindQueueAndLock(changeLog);
        if (queue) {
            queue->WaitFlush();
            AtomicDecrement(queue->UseCount);
        }

        PROFILE_TIMING ("/changelog_flush_io_time") {
            changeLog->Flush();
        }
    }

    i32 GetRecordCount(const TChangeLogPtr& changeLog)
    {
        auto queue = FindQueueAndLock(changeLog);
        if (queue) {
            auto result = queue->GetRecordCount();
            AtomicDecrement(queue->UseCount);
            return result;
        } else {
            return changeLog->GetRecordCount();
        }
    }

    void Finalize(const TChangeLogPtr& changeLog)
    {
        Flush(changeLog);

        PROFILE_TIMING ("/changelog_finalize_time") {
            changeLog->Finalize();
        }

        LOG_DEBUG("Async changelog finalized (ChangeLogId: %d)", changeLog->GetId());
    }

    void Truncate(const TChangeLogPtr& changeLog, i32 atRecordId)
    {
        // TODO: Later on this can be improved to asynchronous behavior by
        // getting rid of explicit synchronization.
        Flush(changeLog);

        PROFILE_TIMING ("/changelog_truncate_time") {
            changeLog->Truncate(atRecordId);
        }
    }

    void Shutdown()
    {
        Finished = true;
        WakeupEvent.Signal();
        Thread.Join();
    }

private:
    TChangeLogQueuePtr FindQueueAndLock(const TChangeLogPtr& changeLog) const
    {
        TGuard<TSpinLock> guard(SpinLock);
        auto it = ChangeLogQueues.find(changeLog);
        if (it == ChangeLogQueues.end())
            return NULL;

        auto& queue = it->second;
        ++queue->UseCount;
        return queue;
    }

    TChangeLogQueuePtr GetQueueAndLock(const TChangeLogPtr& changeLog)
    {
        TGuard<TSpinLock> guard(SpinLock);
        TChangeLogQueuePtr queue;

        auto it = ChangeLogQueues.find(changeLog);
        if (it != ChangeLogQueues.end()) {
            queue = it->second;
        } else {
            queue = New<TChangeLogQueue>(changeLog);
            YVERIFY(ChangeLogQueues.insert(MakePair(changeLog, queue)).second);
        }

        ++queue->UseCount;
        return queue;
    }

    void UnlockQueue(const TChangeLogQueuePtr& queue)
    {
        AtomicDecrement(queue->UseCount);
    }

    void FlushQueues()
    {
        // Take a snapshot.
        yvector<TChangeLogQueuePtr> queues;
        {
            TGuard<TSpinLock> guard(SpinLock);
            
            if (ChangeLogQueues.empty()) {
                // Hash map from arcadia/util does not support iteration over
                // the empty map with iterators. It crashes with a dump assertion
                // deeply within implementation details.
                return;
            }

            FOREACH(const auto& it, ChangeLogQueues) {
                queues.push_back(it.second);
            }
        }

        // Flush the queues.
        FOREACH(auto& queue, queues) {
            queue->Flush();
        }
    }

    // Returns if there are any unswept queues left in the map.
    bool CleanQueues()
    {
        TGuard<TSpinLock> guard(SpinLock);
        
        TChangeLogQueueMap::iterator it, jt;
        for (it = ChangeLogQueues.begin(); it != ChangeLogQueues.end(); /**/) {
            jt = it++;
            auto queue = jt->second;
            if (queue->UseCount == 0 && queue->IsEmpty()) {
                LOG_DEBUG("Async changelog queue was swept (ChangeLogId: %d)", jt->first->GetId());
                ChangeLogQueues.erase(jt);
            }
        }

        return !ChangeLogQueues.empty();
    }

    // Returns True if there are any queues in the map.
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

    typedef yhash_map<TChangeLogPtr, TChangeLogQueuePtr> TChangeLogQueueMap;

    TChangeLogQueueMap ChangeLogQueues;
    TSpinLock SpinLock;
    TThread Thread;
    Event WakeupEvent;
    volatile bool Finished;
    NProfiling::TRateCounter RecordCounter;
    NProfiling::TRateCounter SizeCounter;

};

////////////////////////////////////////////////////////////////////////////////

TAsyncChangeLog::TAsyncChangeLog(const TChangeLogPtr& changeLog)
    : ChangeLog(changeLog)
{
    YASSERT(changeLog);
}

TAsyncChangeLog::~TAsyncChangeLog()
{ }

TAsyncChangeLog::TAppendResult::TPtr TAsyncChangeLog::Append(
    i32 recordId,
    const TSharedRef& data)
{
    return TImpl::Get()->Append(ChangeLog, recordId, data);
}

void TAsyncChangeLog::Finalize()
{
    TImpl::Get()->Finalize(ChangeLog);
}

void TAsyncChangeLog::Flush()
{
    TImpl::Get()->Flush(ChangeLog);
}

void TAsyncChangeLog::Read(i32 firstRecordId, i32 recordCount, yvector<TSharedRef>* result)
{
    TImpl::Get()->Read(ChangeLog, firstRecordId, recordCount, result);
}

i32 TAsyncChangeLog::GetId() const
{
    return ChangeLog->GetId();
}

i32 TAsyncChangeLog::GetRecordCount() const
{
    return TImpl::Get()->GetRecordCount(ChangeLog);
}

i32 TAsyncChangeLog::GetPrevRecordCount() const
{
    return ChangeLog->GetPrevRecordCount();
}

bool TAsyncChangeLog::IsFinalized() const
{
    return ChangeLog->IsFinalized();
}

void TAsyncChangeLog::Truncate(i32 atRecordId)
{
    return TImpl::Get()->Truncate(ChangeLog, atRecordId);
}

void TAsyncChangeLog::Shutdown()
{
    TImpl::Get()->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT

