#pragma once

#include "public.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IOperationController
{
    virtual ~IOperationController()
    { }

    virtual void Initialize() = 0;

    virtual void Abort() = 0;

    virtual void Complete() = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
