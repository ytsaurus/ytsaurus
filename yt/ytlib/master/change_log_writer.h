#pragma once

#include "change_log.h"

#include "../misc/hash.h"
#include "../misc/common.h"
#include "../actions/action_queue.h"
#include "../actions/async_result.h"

#include <util/system/file.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {

class TChangeLogWriter
    : private TNonCopyable
{
public:
    // TODO: more error codes?
    enum EResult
    {
        OK
    };

    TChangeLogWriter(TChangeLog::TPtr changeLog);
    ~TChangeLogWriter();

    typedef TAsyncResult<EResult> TAppendResult;

    TAppendResult::TPtr Append(i32 recordId, const TSharedRef& changeData);
    void Close(); // TODO: rename to Finalize
    // TODO: Truncate();
    // TODO: GetRecordCount();
    TChangeLog::TPtr GetChangeLog() const;

private:
    class TImpl;

    TChangeLog::TPtr ChangeLog;
    TIntrusivePtr<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
