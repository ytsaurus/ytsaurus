#pragma once

#include "common.h"

#include "../actions/async_result.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct IMasterState
    : public TRefCountedBase
{
    typedef TIntrusivePtr<IMasterState> TPtr;

    virtual TAsyncResult<TVoid>::TPtr Save(TOutputStream& output) = 0;
    virtual TAsyncResult<TVoid>::TPtr Load(TInputStream& input) = 0;

    // TODO: -> const TRef&
    virtual void ApplyChange(TRef changeData) = 0;

    virtual void Clear() = 0;

    virtual IInvoker::TPtr GetInvoker() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
