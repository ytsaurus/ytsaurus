#include "async_change_log.h"

#include "../actions/action_util.h"
#include "../logging/log.h"

#include <util/system/thread.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ChangeLogWriter");
static i32 UnflushedThreshold = 1 << 20;

////////////////////////////////////////////////////////////////////////////////

//! Implementation of the TAsyncChangeLog.
class TAsyncChangeLog::TImpl
    : public TActionQueue
{
public:
    // This method is invoked synchronously and only via underlying queue,
    // so there's no need for extra synchronization.
    void Append(
        i32 recordId,
        const TSharedRef& data,
        TAppendResult::TPtr result,
        TChangeLog::TPtr changeLog)
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

        if (queue->Append(recordId, data, result)) {
            ChangeLogQueues.erase(it);
        }
    }

    // This method is invoked on changelog finalization.
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

    //! Queue for asynchronous logging of the changes to the changelog.
    /*!
     * Internally, this class delegates all the work to the underlying changelog
     * and eventually performs I/O synchronization hence marking changes as logged.
     *
     * \see #UnflushedThreshold
     */
    class TChangeLogQueue
        : public TRefCountedBase
    {
    public:
        TChangeLog::TPtr ChangeLog;
        i32 UnflushedSize;

        struct TAppendRecord
        {
            i32 RecordId;
            TSharedRef Data;

            TAppendRecord(i32 recordId, const TSharedRef& data)
                : RecordId(recordId)
                , Data(data)
            { }
        };

        typedef yvector<TAppendRecord> TAppendRecords;
        typedef yvector<TAsyncChangeLog::TAppendResult::TPtr> TAppendResults;

        TAppendRecords Records;
        TAppendResults Results;

        //! This mutex should be used to prevent queue flushing while
        //! someone is reading from the queue.
        TSpinLock FlushLock;

        typedef TIntrusivePtr<TChangeLogQueue> TPtr;

        //! Constructs empty queue around underlying changelog.
        TChangeLogQueue(TChangeLog::TPtr changeLog)
            : ChangeLog(changeLog)
            , UnflushedSize(0)
        { }

        //! Appends entry to the underlying changelog.
        /*!
         * \returns True if the queue was flushed.
         */
        bool Append(
            i32 recordId,
            const TSharedRef& data,
            TAppendResult::TPtr result)
        {
            // XXX: think about locking on append, because 
            // random push_back can invalidate all reading iterators
            ChangeLog->Append(recordId, data);

            {
                TGuard<TSpinLock> guard(FlushLock);
                Records.push_back(TAppendRecord(recordId, data));
                Results.push_back(result);
            }

            UnflushedSize += data.Size();
            if (UnflushedSize >= UnflushedThreshold) {
                Flush();
                return true;
            }

            return false;
        }

        //! Flushes the underlying changelog.
        void Flush()
        {
            ChangeLog->Flush();

            UnflushedSize = 0;
            for (int i = 0; i < Results.ysize(); ++i) {
                Results[i]->Set(TVoid());
            }

            {
                TGuard<TSpinLock> guard(FlushLock);
                Records.clear();
                Results.clear();
            }
        }
    }; // class TChangeLogQueue

    //! This method is invoked synchronously in the idle time, as the name suggests.
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

i32 TAsyncChangeLog::GetId() const
{
    return ChangeLog->GetId();
}

bool TAsyncChangeLog::IsFinalized() const
{
    return ChangeLog->IsFinalized();
}

TAsyncChangeLog::TAppendResult::TPtr TAsyncChangeLog::Append(
    i32 recordId, const TSharedRef& data)
{
    TAppendResult::TPtr result = new TAppendResult();
    Impl->Invoke(
        FromMethod(&TImpl::Append, Impl, recordId, data, result, ChangeLog));
    return result;
}

void TAsyncChangeLog::Finalize()
{
    FromMethod(&TImpl::Finalize, Impl, ChangeLog)
        ->AsyncVia(~Impl)
        ->Do()
        ->Get();
    LOG_INFO("Changelog %d was closed", ChangeLog->GetId());
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

    // TODO: rename to unflushedRecords
    yvector<TSharedRef> unflushedChanges;

    for (TImpl::TChangeLogQueue::TAppendRecords::iterator it = flushQueue->Records.begin();
        it != flushQueue->Records.end();
        ++it)
    {
        if (it->RecordId >= lastRecordId)
            break;

        if (it->RecordId < firstRecordId)
            continue;

        firstUnflushedRecordId = Min(firstUnflushedRecordId, it->RecordId);

        unflushedChanges.push_back(it->Data);
    }

    // At this moment we can release the lock.
    guard.Release();

    if (unflushedChanges.empty()) {
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
        result->insert(result->end(), unflushedChanges.begin(), unflushedChanges.end());
    }
}

int TAsyncChangeLog::GetRecordCount()
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

TChangeLog::TPtr TAsyncChangeLog::GetChangeLog() const
{
    return ChangeLog;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

