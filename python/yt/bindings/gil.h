#pragma once

#include <util/generic/noncopyable.h>

#include <contrib/libs/pycxx/Objects.hxx>

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

class TGilGuard
    : private TNonCopyable
{
public:
    TGilGuard()
        : State_(PyGILState_Ensure())
    { }

    ~TGilGuard()
    {
        PyGILState_Release(State_);
    }

private:
    PyGILState_STATE State_;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
