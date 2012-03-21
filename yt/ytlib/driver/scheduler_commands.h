#pragma once

#include "command.h"

#include <ytlib/scheduler/map_controller.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TMapRequest
    : public TTransactedRequest
{
    NScheduler::TMapOperationSpecPtr Spec;

    TMapRequest()
    {
        Register("spec", Spec);
    }
};

class TMapCommand
    : public TCommandBase<TMapRequest>
{
public:
    TMapCommand(IDriverImpl* driverImpl)
        : TCommandBase(driverImpl)
    { }

private:
    virtual void DoExecute(TMapRequest* request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

