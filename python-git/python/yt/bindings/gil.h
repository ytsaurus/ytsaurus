#pragma once

#include <util/generic/noncopyable.h>

#include <contrib/libs/pycxx/Objects.hxx>

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

class TGILLock
    : TNonCopyable
{
public:
    TGILLock()
        : State_(PyGILState_Ensure())
    { }

    ~TGILLock()
    {
        PyGILState_Release(State_);
    }

private:
    PyGILState_STATE State_;
};


} // namespace NPython
} // namespace NYT
