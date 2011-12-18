#include "stdafx.h"
#include "async_change_log.h"

#include "../misc/foreach.h"
#include "../misc/singleton.h"
#include "../actions/action_util.h"

#include <util/system/thread.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

// TODO: Extract these settings to the global configuration.
static i32 UnflushedBytesThreshold = 1 << 20;
static i32 UnflushedRecordsThreshold = 100000;

////////////////////////////////////////////////////////////////////////////////

class TAsyncChangeLog::TImpl
    : public TActionQueue
{
public:
    ////////////////////////////////////////////////////////////////////////////////

    //! Queue for asynchronous appending of the changes to the changelog.
    /*!
     * Internally, this class delegates all the work to the underlying changelog
     * and eventually performs I/O synchronization hence marking changes as flushed.
     */
    class TChangeLogQueue
        : public TRefCountedBase
    {
    public:
        typedef TIntrusivePtr<TChangeLogQueue> TPtr;

        //! Constructs an empty queue around underlying changelog.
        TChangeLogQueue(TChangeLog::TPtr changeLog)
            : ChangeLog(changeLog)
            , UnflushedBytes(0)
            , UnflushedRecords(0)
        { }

        //! Lazily appends the record to the changelog.
        IAction::TPtr Append(
            const TMetaVersion& version,
            const TSharedRef& data,
            TAppendResult::TPtr result)
        {
            THolder<TRecord> recordHolder(new TRecord(version, data, result));
            TRecord* record = ~recordHolder;

            {
                TGuard<TSpinLock> guard(SpinLock);
                Records.PushBack(recordHolder.Release());
            }

            return FromMethod(&TChangeLogQueue::DoAppend, TPtr(this), record);
        }

        //! Flushes the underlying changelog.
        void Flush()
        {
            ChangeLog->Flush();

            UnflushedBytes = 0;
            UnflushedRecords = 0;

            {
                TGuard<TSpinLock> guard(SpinLock);
                // Curse you, arcadia/util!
                Records.ForEach(SweepRecord);
            }

            LOG_DEBUG("Async changelog is flushed (ChangeLogId: %d)", ChangeLog->GetId());
        }

        //! Checks if the queue is empty. Note that despite the fact that this call locks
        //! the list to perform the actual check, its result is only meaningful when
        //! no additional records may be enqueued into it.
        bool IsEmpty() const
        {
            TGuard<TSpinLock> guard(SpinLock);
            return Records.Empty();
        }

        //! An unflushed record (auxiliary structure).
        struct TRecord : public TIntrusiveListItem<TRecord>
        {
            TMetaVersion Version;
            TSharedRef Data;
            TAsyncChangeLog::TAppendResult::TPtr Result;
            bool WaitingForSync;

            TRecord(
                const TMetaVersion& version,
                const TSharedRef& data,
                const TAsyncChangeLog::TAppendResult::TPtr& result)
                : Version(version)
                , Data(data)
                , Result(result)
                , WaitingForSync(false)
            { }
        }; // struct TRecord

        //! A list of unflushed records (auxiliary structure).
        /*!
         * \note
         * Currently, the queue is based on the linked list to prevent accidental
         * memory reallocations. It should be taken into account that this incurs
         * a memory allocation for each enqueued record. Hence in the future
         * more refined memory strategy should be used.
         */
        class TRecords : public TIntrusiveListWithAutoDelete<TRecord, TDelete>
        {
        }; // class TRecords

        //! Underlying changelog.
        TChangeLog::TPtr ChangeLog;

        //! Number of currently unflushed bytes (since the last synchronization).
        i32 UnflushedBytes;

        //! Number of currently unflushed records (since the last synchronization).
        i32 UnflushedRecords;

        //! A list of unflushed records.
        TRecords Records;

        //! Preserves the atomicity of the operations on the list.
        mutable TSpinLock SpinLock;

    private:
        //! Sweep operation performed during #Flush.
        static void SweepRecord(TRecord* record)
        {
            if (record->WaitingForSync) {
                record->Result->Set(TVoid());
                record->Unlink();
                delete record;

                LOG_TRACE("Async changelog record is committed (Version: %s)",
                    ~record->Version.ToString());
            }
        }

        //! Actually appends the record and flushes the queue if required.
        void DoAppend(TRecord* record)
        {
            YASSERT(record != NULL);
            YASSERT(!record->WaitingForSync);

            ChangeLog->Append(record->Version.RecordCount, record->Data);

            {
                TGuard<TSpinLock> guard(SpinLock);
                record->WaitingForSync = true;
            }

            UnflushedBytes += record->Data.Size();
            UnflushedRecords += 1;

            if (UnflushedBytes >= UnflushedBytesThreshold ||
                UnflushedRecords >= UnflushedRecordsThreshold)
            {
                LOG_DEBUG("Async changelog unflushed threshold reached (ChangeLogId: %d)", ChangeLog->GetId());
                Flush();
            }
        }

    }; // class TChangeLogQueue

    ////////////////////////////////////////////////////////////////////////////////

    TImpl()
        : TActionQueue("AsyncChangeLog")
    { }

    void Append(
        TChangeLog::TPtr changeLog,
        i32 recordId,
        const TSharedRef& data,
        TAppendResult::TPtr result)
    {
        TChangeLogQueue::TPtr queue;
        {
            TGuard<TSpinLock> guard(SpinLock);
            auto it = ChangeLogQueues.find(changeLog);
            if (it == ChangeLogQueues.end()) {
                queue = New<TChangeLogQueue>(changeLog);
                it = ChangeLogQueues.insert(MakePair(changeLog, queue)).first;
            } else {
                queue = it->second;
            }
        }

        TMetaVersion version(changeLog->GetId(), recordId);
        GetInvoker()->Invoke(queue->Append(version, data, result));

        LOG_TRACE("Async changelog record is enqueued (Version: %s)", ~version.ToString());
    }

    TVoid Finalize(TChangeLog::TPtr changeLog)
    {
        auto queue = FindQueue(changeLog);
        if (~queue != NULL) {
            queue->Flush();
        }
        changeLog->Finalize();

        LOG_DEBUG("Async changelog finalized (ChangeLogId: %d)", changeLog->GetId());

        return TVoid();
    }

    TVoid Flush(TChangeLog::TPtr changeLog)
    {
        auto queue = FindQueue(changeLog);
        if (~queue != NULL) {
            queue->Flush();
        }
        changeLog->Flush();
        return TVoid();
    }

    TChangeLogQueue::TPtr FindQueue(TChangeLog::TPtr changeLog)
    {
        TGuard<TSpinLock> guard(SpinLock);
        auto it = ChangeLogQueues.find(changeLog);
        return it != ChangeLogQueues.end() ? it->second : NULL;
    }

    virtual void OnIdle()
    {
        LOG_DEBUG("Async changelog thread is idle");

        // Take a snapshot.
        yvector<TChangeLogQueue::TPtr> queues;
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

        // Sweep the empty queues.
        {
            // Taking this lock ensures that if a queue is found empty then it can be safely
            // erased from the map.
            TGuard<TSpinLock> guard(SpinLock);
            TChangeLogQueueMap::iterator it, jt;
            for (it = ChangeLogQueues.begin(); it != ChangeLogQueues.end(); /**/) {
                jt = it++;
                auto queue = jt->second;
                if (queue->IsEmpty()) {
                    ChangeLogQueues.erase(jt);
                    LOG_DEBUG("Async changelog queue is swept (ChangeLogId: %d)", queue->ChangeLog->GetId());
                }
            }
        }
    }

private:
    typedef yhash_map<TChangeLog::TPtr, TChangeLogQueue::TPtr> TChangeLogQueueMap;

    TChangeLogQueueMap ChangeLogQueues;
    TSpinLock SpinLock;
};

////////////////////////////////////////////////////////////////////////////////

TAsyncChangeLog::TAsyncChangeLog(TChangeLog::TPtr changeLog)
    : ChangeLog(changeLog)
    , Impl(RefCountedSingleton<TImpl>())
{
    YASSERT(~changeLog != NULL);
}

TAsyncChangeLog::~TAsyncChangeLog()
{ }

TAsyncChangeLog::TAppendResult::TPtr TAsyncChangeLog::Append(
    i32 recordId,
    const TSharedRef& data)
{
    auto result = New<TAppendResult>();
    Impl->Append(ChangeLog, recordId, data, result);
    return result;
}

void TAsyncChangeLog::Finalize()
{
    FromMethod(&TImpl::Finalize, Impl, ChangeLog)
        ->AsyncVia(Impl->GetInvoker())
        ->Do()
        ->Get();
}

void TAsyncChangeLog::Flush()
{
    FromMethod(&TImpl::Flush, Impl, ChangeLog)
        ->AsyncVia(Impl->GetInvoker())
        ->Do()
        ->Get();
}

void TAsyncChangeLog::Read(i32 firstRecordId, i32 recordCount, yvector<TSharedRef>* result)
{
    YASSERT(firstRecordId >= 0);
    YASSERT(recordCount >= 0);
    YASSERT(result);

    if (recordCount == 0) {
        return;
    }

    auto queue = Impl->FindQueue(ChangeLog);

    if (~queue == NULL) {
        ChangeLog->Read(firstRecordId, recordCount, result);
        return;
    }

    // Determine whether unflushed records intersect with requested records.
    // To achieve this we have to lock the queue in order to iterate
    // through currently flushing record.
    TGuard<TSpinLock> guard(queue->SpinLock);

    i32 lastRecordId = firstRecordId + recordCount; // Non-inclusive.
    i32 firstUnflushedRecordId = lastRecordId;

    yvector<TSharedRef> unflushedRecords;

    FOREACH(const auto& record, queue->Records) {
        i32 currentId = record.Version.RecordCount;

        if (currentId >= lastRecordId)
            break;

        if (currentId  < firstRecordId)
            continue;

        firstUnflushedRecordId = Min(firstUnflushedRecordId, currentId);

        unflushedRecords.push_back(record.Data);
    }

    // At this moment we can release the lock.
    guard.Release();

    if (unflushedRecords.empty()) {
        ChangeLog->Read(firstRecordId, recordCount, result);
        return;
    }

    ChangeLog->Read(firstRecordId, firstUnflushedRecordId - firstRecordId, result);

    i32 firstUnreadRecordId = firstRecordId + result->ysize();

    if (firstUnreadRecordId != firstUnflushedRecordId) {
        LOG_FATAL("Gap found while reading changelog (FirstUnreadRecordId: %d, FirstUnflushedRecordId: %d)",
            firstUnreadRecordId,
            firstUnflushedRecordId);
    } else {
        result->insert(result->end(), unflushedRecords.begin(), unflushedRecords.end());
    }
}

i32 TAsyncChangeLog::GetId() const
{
    return ChangeLog->GetId();
}

i32 TAsyncChangeLog::GetRecordCount() const
{
    auto queue = Impl->FindQueue(ChangeLog);

    if (~queue == NULL) {
        return ChangeLog->GetRecordCount();
    }
    
    TGuard<TSpinLock> guard(queue->SpinLock);
    if (queue->Records.Empty()) {
        return ChangeLog->GetRecordCount();
    } else {
        auto it = queue->Records.End();
        --it;
        return it->Version.RecordCount + 1;
    }
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
    // TODO: Later on this can be improved to asynchronous behaviour by
    // getting rid of explicit synchronization.
    Flush();

    return ChangeLog->Truncate(atRecordId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT

