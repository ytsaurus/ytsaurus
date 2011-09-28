#pragma once

#include "common.h"

#include "../actions/future.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

struct IMetaState
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IMetaState> TPtr;

    virtual TFuture<TVoid>::TPtr Save(TOutputStream* output) = 0;
    virtual TFuture<TVoid>::TPtr Load(TInputStream* input) = 0;

    virtual void ApplyChange(const TRef& changeData) = 0;

    virtual void Clear() = 0;

    virtual IInvoker::TPtr GetInvoker() const = 0;

    virtual void OnStartLeading() = 0;
    virtual void OnStopLeading() = 0;
    virtual void OnStartFollowing() = 0;
    virtual void OnStopFollowing() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
