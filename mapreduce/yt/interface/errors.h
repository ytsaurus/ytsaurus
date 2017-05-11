#pragma once

#include "fwd.h"
#include "common.h"

#include <util/generic/yexception.h>

namespace NYT {

////////////////////////////////////////////////////////////////////

class TApiUsageError
    : public yexception
{ };

////////////////////////////////////////////////////////////////////

class TOperationFailedError
    : public yexception
{
public:
    enum EState {
        Failed,
        Aborted,
    };

public:
    TOperationFailedError(
        EState state,
        TOperationId id,
        TString error = TString())
        : State(state)
        , Id(id)
        , Error(error)
    { }

    const char* what() const throw() override
    {
        return ~Error;
    }

public:
    EState State;
    TOperationId Id;
    TString Error;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
