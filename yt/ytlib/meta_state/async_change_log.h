#pragma once

#include "change_log.h"

#include "../misc/hash.h"
#include "../misc/common.h"
#include "../actions/action_queue.h"
#include "../actions/async_result.h"

#include <util/system/file.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {

//! Asynchronous wrapper around TChangeLog.
/*!
 * This class implements (more-or-less) non-blocking semantics for working with
 * the changelog. Blocking can occur eventually when the internal buffers
 * overflow.
 *
 * \see #UnflushedBytesThreshold
 * \see #UnflushedRecordsThreshold
 */
class TAsyncChangeLog
    : private TNonCopyable
{
public:
    TAsyncChangeLog(TChangeLog::TPtr changeLog);
    ~TAsyncChangeLog();

    typedef TAsyncResult<TVoid> TAppendResult;

    //! Finalizes the changelog.
    //! \see TChangeLog::Finalize
    void Finalize();

    //! Enqueues record to be appended to the changelog.
    /*!
     * Internally, asynchronous append to the changelog goes as follows.
     * Firstly, the record is marked as 'Unflushed' and enqueued to the flush queue.
     * Secondly, as soon as the queue becomes synchronized with the disk state
     * the promise is fulfilled. At this moment caller can determine whether
     * the record was written to the disk.
     *
     * Note that promise is not fulfilled when an error occurs.
     * In this case the promise is never fulfilled.
     *
     * \param recordId Consecutive record id.
     * \param data Actual record content.
     * \returns Promise to fulfill when the record will be flushed.
     *
     * \see TChangeLog::Append
     */
    TAppendResult::TPtr Append(i32 recordId, const TSharedRef& data);

    //! Flushes the changelog.
    //! \see TChangeLog::Flush
    void Flush();

    //! Reads records from the changelog.
    //! \see TChangeLog::Read
    void Read(i32 firstRecordId, i32 recordCount, yvector<TSharedRef>* result);

    //! Truncates the changelog at the specified record.
    //! \see TChangeLog::Truncate
    void Truncate(i32 atRecordId);

    i32 GetId() const;
    i32 GetPrevRecordCount() const;
    i32 GetRecordCount() const;
    bool IsFinalized() const;

private:
    TChangeLog::TPtr ChangeLog;

private:
    //! Actual workhorse behind all the TAsyncChangeLog s.
    /*!
     * This class spawns a separate thread which performs all the actual work with
     * all the changelogs. This is somewhat serialization point.
     *
     * To sustain adequate latency all modifications of the changelogs are
     * asynchronous and buffered with eventual synchronization when the buffers
     * become too large.
     *
     * \see UnflushedBytesThreshold
     * \see UnflushedRecordsThreshold
     */
    //! {
    class TImpl;
    TIntrusivePtr<TImpl> Impl;
    //! }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
