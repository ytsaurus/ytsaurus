#pragma once

#include "change_log.h"

#include "../misc/hash.h"
#include "../misc/common.h"
#include "../actions/action_queue.h"
#include "../actions/async_result.h"

#include <util/system/file.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {

class TAsyncChangeLog
    : private TNonCopyable
{
public:
    TAsyncChangeLog(TChangeLog::TPtr changeLog);
    ~TAsyncChangeLog();

    typedef TAsyncResult<TVoid> TAppendResult;

    void Finalize();

    TAppendResult::TPtr Append(i32 recordId, const TSharedRef& changeData);
    // TODO: Flush();
    void Read(i32 firstRecordId, i32 recordCount, yvector<TSharedRef>* result);
    // TODO: Truncate();

    i32 GetId() const;
    // TMasterStateId GetPrevStateId() const;
    i32 GetRecordCount() const;
    bool IsFinalized() const;

private:
    class TImpl;

    TChangeLog::TPtr ChangeLog;
    TIntrusivePtr<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
