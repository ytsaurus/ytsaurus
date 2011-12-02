#pragma once

#include "common.h"

#include "../actions/future.h"
#include "enum.h"
#include "property.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TError
{
    DEFINE_BYVAL_RO_PROPERTY(int, Code);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Message);

public:
    TError()
        : Code_(OK)
    { }

    explicit TError(const Stroka& message)
        : Code_(Fail)
        , Message_(message)
    { }

    TError(int code, const Stroka& message)
        : Code_(code)
        , Message_(message)
    { }

    bool IsOK() const
    {
        return Code_ == OK;
    }

    Stroka ToString() const
    {
        if (Code_ == OK) {
            return "OK";
        } else {
            return Sprintf("(%d): %s", Code_, ~Message_);
        }
    }

    static const int OK;
    static const int Fail;
};

////////////////////////////////////////////////////////////////////////////////

typedef TFuture<TError> TAsyncError;

////////////////////////////////////////////////////////////////////////////////

// TODO: get rid of this
#ifdef _win_
#undef GetMessage
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
