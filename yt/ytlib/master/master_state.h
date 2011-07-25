#pragma once

#include "common.h"

#include "../actions/async_result.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct IMasterState
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IMasterState> TPtr;

    virtual TAsyncResult<TVoid>::TPtr Save(TOutputStream& output) = 0;
    virtual TAsyncResult<TVoid>::TPtr Load(TInputStream& input) = 0;

    virtual void ApplyChange(const TRef& changeData) = 0;

    virtual void Clear() = 0;

    virtual IInvoker::TPtr GetInvoker() const = 0;

    virtual void OnStartLeading() = 0;
    virtual void OnStopLeading() = 0;
    virtual void OnStartFollowing() = 0;
    virtual void OnStopFollowing() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
