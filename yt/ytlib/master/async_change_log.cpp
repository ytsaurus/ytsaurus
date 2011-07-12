#include "async_change_log.h"

#include "../actions/action_util.h"

#include <util/system/thread.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MasterLogger;

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

        //! An unflushed record (auxiliary structure).
        class TRecord : public TIntrusiveListItem<TRecord>
        {
        public:
            i32 Id;
            TSharedRef Data;
            TAsyncChangeLog::TAppendResult::TPtr Result;

            bool WaitingForSync;

        public:
            TRecord(
                i32 id,
                const TSharedRef& data,
                const TAsyncChangeLog::TAppendResult::TPtr& result)
                : Id(id)
                , Data(data)
                , WaitingForSync(false)
                , Result(result)
            { }
        };

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
        public:
            //! Sweep operation performed during the Flush().
            class TSweepWaitingForSync
            {
                inline operator()(TRecord* record)
                {
                    if (record->WaitingForSync) {
                        record->Result.Set(TVoid());
                        record->Unlink();
                    }
                }
            };
        };

        //! Underlying changelog.
        TChangeLog::TPtr ChangeLog;
        //! Amount of currently unflushed bytes (since the last synchronization).
        i32 UnflushedBytes;
        //! Amount of currently unflushed records (since the last synchronization).
        i32 UnflushedRecords;

        //! A list of unflushed records.
        TRecords Records;

        //! Preverses the atomicity of the operations on the list.
        TSpinLock SpinLock;

        //! Constructs an empty queue around underlying changelog.
        TChangeLogQueue(TChangeLog::TPtr changeLog)
            : ChangeLog(changeLog)
            , UnflushedBytes(0)
            , UnflushedRecords(0)
        { }

        //! Lazily appends the record to the changelog.
        IAction::TPtr Append(
            i32 recordId,
            const TSharedRef& data,
            const TAppendResult::TPtr& result)
        {
            THolder<TRecord> recordHolder(new TRecord(recordId, data, result));
            TRecord* record = recordHolder.Get();

            {
                TGuard<TSpinLock> guard(SpinLock);
                Records.PushBack(record.Release());
            }

            UnflushedBytes += data.Size();
            UnflushedRecords += 1;

            return FromMethod(&TChangeLogQueue::DoAppend, this, record);
        }

        //! Flushes the underlying changelog.
        void Flush()
        {
            TGuard<TSpinLock> guard(FlushLock);

            ChangeLog->Flush();

            UnflushedBytes = 0;
            UnflushedRecords = 0;

            Records.ForEach(TRecords::TSweepWaitingForSync());
        }

    private:
        //! Actually appends the record and flushes the queue if required.
        void DoAppend(TRecord* record)
        {
            YASSERT(record);
            YASSERT(!record->WaitingForSync);

            ChangeLog->Append(record->Id, record->Data);

            {
                TGuard<TSpinLock> guard(SpinLock);
                record->WaitingForSync = true;
            }

            if (
                UnflushedBytes >= UnflushedBytesThreshold ||
                UnflushedRecords >= UnflushedRecordsThreshold)
            {
                Flush();
            }
        }
    }; // class TChangeLogQueue

    ////////////////////////////////////////////////////////////////////////////////

    void Append(
        TChangeLog::TPtr changeLog,
        i32 recordId,
        const TSharedRef& data,
        const TAppendResult::TPtr& result)
    {
        TGuard<TSpinLock> guard(SpinLock);

        TChangeLogQueueMap::iterator it = ChangeLogQueues.find(changeLog);
        TChangeLogQueue::TPtr queue;

        if (it == ChangeLogQueues.end()) {
            queue = new TChangeLogQueue(changeLog);
            it = ChangeLogQueues.insert(MakePair(changeLog, queue)).first;
        } else {
            queue = it->second;
        }

        queue->Append(recordId, data)->Via(this)->Do();
    }

    TVoid Finalize(TChangeLog::TPtr changeLog)
    {
        TGuard<TSpinLock> guard(SpinLock);

        TChangeLogQueueMap::iterator it = ChangeLogQueues.find(changeLog);
        TChangeLogQueue::TPtr queue;

        if (it != ChangeLogQueues.end()) {
            queue = it->second;
            queue->Flush();

            ChangeLogQueues.erase(it);
        }

        changeLog->Finalize();
        return TVoid();
    }

    TVoid Flush(TChangeLog::TPtr changeLog)
    {
        TGuard<TSpinLock> guard(SpinLock);

        TChangeLogQueueMap::iterator it = ChangeLogQueues.find(changeLog);
        TChangeLogQueue::TPtr queue;

        if (it != ChangeLogQueues.end()) {
            queue = it->second;
            queue->Flush();

            ChangeLogQueues.erase(it);
        }

        changeLog->Flush();
        return TVoid();
    }

    //! Invoked synchronously in the idle time, as the name suggests.
    virtual void OnIdle()
    {
        for (TChangeLogQueueMap::iterator it = ChangeLogQueues.begin();
            it != ChangeLogQueues.end(); ++it)
        {
            it->second->Flush();
        }

        {
            TGuard<TSpinLock> guard(SpinLock);
            ChangeLogQueues.clear();
        }
    }

    TChangeLogQueue::TPtr GetCorrespondingQueue(TChangeLog::TPtr changeLog)
    {
        TGuard<TSpinLock> guard(SpinLock);

        TChangeLogQueueMap::iterator it = ChangeLogQueues.find(changeLog);

        if (it != ChangeLogQueues.end()) {
            return it->second;
        } else {
            return NULL;
        }
    }

    typedef yhash_map<TChangeLog::TPtr,
        TChangeLogQueue::TPtr,
        TIntrusivePtrHash<TChangeLog> > TChangeLogQueueMap;

    TChangeLogQueueMap ChangeLogQueues;
    TSpinLock SpinLock;
};

////////////////////////////////////////////////////////////////////////////////

TAsyncChangeLog::TAsyncChangeLog(TChangeLog::TPtr changeLog)
    : ChangeLog(changeLog)
    , Impl(RefCountedSingleton<TImpl>())
{ }

TAsyncChangeLog::~TAsyncChangeLog()
{ }

TAsyncChangeLog::TAppendResult::TPtr TAsyncChangeLog::Append(
    i32 recordId, const TSharedRef& data)
{
    TAppendResult::TPtr result = new TAppendResult();
    Impl->Append(ChangeLog, recordId, data, result);
    return result;
}

void TAsyncChangeLog::Finalize()
{
    FromMethod(&TImpl::Finalize, Impl, ChangeLog)
        ->AsyncVia(~Impl)
        ->Do()
        ->Get();
    LOG_INFO("Changelog %d is finalized", ChangeLog->GetId());
}

void TAsyncChangeLog::Flush()
{
    FromMethod(&TImpl::Flush, Impl, ChangeLog)
        ->AsyncVia(~Impl)
        ->Do()
        ->Get();
    LOG_INFO("Changelog %d is flushed", ChangeLog->GetId());
}

void TAsyncChangeLog::Read(i32 firstRecordId, i32 recordCount, yvector<TSharedRef>* result)
{
    YASSERT(firstRecordId >= 0);
    YASSERT(recordCount >= 0);
    YASSERT(result);

    // TODO: ?
    if (recordCount == 0) {
        return;
    }

    // TODO: queue access locking

    TImpl::TChangeLogQueue::TPtr flushQueue = Impl->GetCorrespondingQueue(ChangeLog);
    if (~flushQueue == NULL) {
        ChangeLog->Read(firstRecordId, recordCount, result);
        return;
    }

    // Determine whether unflushed records intersect with requested records.
    // To achieve this we have to lock the queue in order to iterate
    // through currently flushing record.
    TGuard<TSpinLock> guard(flushQueue->FlushLock);
    i32 lastRecordId = firstRecordId + recordCount;
    i32 firstUnflushedRecordId = lastRecordId;

    yvector<TSharedRef> unflushedRecords;

    for (TImpl::TChangeLogQueue::TAppendRecords::iterator it = flushQueue->Records.begin();
        it != flushQueue->Records.end();
        ++it)
    {
        if (it->RecordId >= lastRecordId)
            break;

        if (it->RecordId < firstRecordId)
            continue;

        firstUnflushedRecordId = Min(firstUnflushedRecordId, it->RecordId);

        unflushedRecords.push_back(it->Data);
    }

    // At this moment we can release the lock.
    guard.Release();

    if (unflushedRecords.empty()) {
        ChangeLog->Read(firstRecordId, lastRecordId, result);
        return;
    }

    ChangeLog->Read(firstRecordId, firstUnflushedRecordId - firstRecordId, result);

    i32 firstUnreadRecordId = firstRecordId + result->ysize();

    if (firstUnreadRecordId != firstUnflushedRecordId) {
        LOG_FATAL("Gap found while reading changelog: (FirstUnreadRecordId: %d, FirstUnflushedRecordId: %d)",
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
    // TODO: locking?
    TImpl::TChangeLogQueue::TPtr flushQueue = Impl->GetCorrespondingQueue(ChangeLog);
    if (~flushQueue == NULL) {
        return ChangeLog->GetRecordCount();
    } else {
        TGuard<TSpinLock> guard(flushQueue->FlushLock);
        if (flushQueue->Records.empty()) {
            return ChangeLog->GetRecordCount();
        } else {
            return flushQueue->Records[flushQueue->Records.ysize() - 1].RecordId + 1;
        }
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

void TAsyncChangeLog::Truncate( i32 atRecordId )
{
    // TOOD: flush or something?
    return ChangeLog->Truncate(atRecordId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

