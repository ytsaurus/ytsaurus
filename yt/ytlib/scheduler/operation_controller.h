#pragma once

#include "public.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IOperationController
    : public TRefCounted
{
    void OnOperationStarted(TOperationPtr operation);
    void OnOperationFinished(TOperationPtr operation);

    void Schedule(
        )
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
