#include "stdafx.h"
#include "error.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TError::TError()
    : Code_(OK)
{ }

TError::TError(const Stroka& message)
    : Code_(Fail)
    , Message_(message)
{ }

TError::TError(const TError& other)
    : Code_(other.Code_)
    , Message_(other.Message_)
{ }

TError::TError(int code, const Stroka& message)
    : Code_(code)
    , Message_(message)
{ }

Stroka TError::ToString() const
{
    switch (Code_) {
        case OK:
            return "OK";
        case Fail:
            return Message_;
        default:
            return Sprintf("(%d): %s", Code_, ~Message_);
    }
}

NProto::TError TError::ToProto() const
{
    NProto::TError protoError;
    protoError.set_code(Code_);
    if (!Message_.empty()) {
        protoError.set_message(Message_);
    }
    return protoError;

}

TError TError::FromProto(const NProto::TError& protoError)
{
    TError error;
    error.Code_ = protoError.code();
    error.Message_ = protoError.has_message() ? protoError.message() : "";
    return error;
}

bool TError::IsOK() const
{
    return Code_ == OK;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
