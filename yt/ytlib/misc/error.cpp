#include "stdafx.h"
#include "error.h"

#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/attributes.h>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TError::TError()
    : Code_(OK)
{ }

TError::TError(const Stroka& message)
    : Code_(Fail)
    , Message_(message)
{ }

TError::TError(const char* format, ...)
    : Code_(Fail)
{
    va_list params;
    va_start(params, format);
    vsprintf(Message_, format, params);
    va_end(params);
}

TError::TError(const TError& other)
    : Code_(other.Code_)
    , Message_(other.Message_)
{ }

TError::TError(int code, const Stroka& message)
    : Code_(code)
    , Message_(message)
{ }

TError::TError(int code, const char* format, ...)
    : Code_(code)
{
    va_list params;
    va_start(params, format);
    vsprintf(Message_, format, params);
    va_end(params);
}

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

NYT::NProto::TError TError::ToProto() const
{
    NYT::NProto::TError protoError;
    protoError.set_code(Code_);
    if (!Message_.empty()) {
        protoError.set_message(Message_);
    }
    return protoError;

}

TError TError::FromProto(const NYT::NProto::TError& protoError)
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

void TError::ToYson(IYsonConsumer* consumer) const
{
    // TODO(babenko): refactor
    BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf(Code_ != TError::Fail, [=] (TFluentMap fluent) {
                fluent.Item("code").Scalar(Code_);
            })
            .Item("message").Scalar(Message_)
        .EndMap();
}

TError TError::FromYson(INodePtr node)
{
    // TODO(babenko): refactor
    auto attributes = ConvertToAttributes(node->AsMap());
    return TError(
        attributes->Get<int>("code", Fail),
        attributes->Get<Stroka>("message"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
