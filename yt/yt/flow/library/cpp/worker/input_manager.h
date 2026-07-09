#pragma once

#include "public.h"

#include <yt/yt/core/actions/invoker.h>

#include <library/cpp/yt/misc/property.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

struct IInputManager
    : public TRefCounted
{
    virtual void AddJob(TJobId jobId, IInputBufferPtr inputBuffer) = 0;
    virtual void RemoveJob(TJobId jobId) = 0;

    virtual IInputBufferPtr GetInputBuffer(TJobId jobId) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IInputManager);

////////////////////////////////////////////////////////////////////////////////

IInputManagerPtr CreateInputManager();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
