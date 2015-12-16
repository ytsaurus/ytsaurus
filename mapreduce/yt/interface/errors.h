#pragma once

#include "common.h"

#include <util/generic/yexception.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

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
        Stroka error = Stroka())
        : State(state)
        , Id(id)
        , Error(error)
    { }

    virtual const char* what() const throw()
    {
        return ~Error;
    }

public:
    EState State;
    TOperationId Id;
    Stroka Error;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
