#include "stdafx.h"
#include "error.h"

#include <ytlib/misc/address.h>
#include <ytlib/misc/thread.h>

#include <ytlib/ytree/convert.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/tokenizer.h>

#include <util/system/error.h>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TError::TError()
    : Code_(OK)
{ }

TError::TError(const Stroka& message)
    : Code_(Fail)
    , Message_(message)
{
    CaptureOriginAttributes();
}

TError::TError(const char* format, ...)
    : Code_(Fail)
{
    va_list params;
    va_start(params, format);
    vsprintf(Message_, format, params);
    va_end(params);

    CaptureOriginAttributes();
}

TError::TError(const TError& other)
    : Code_(other.Code_)
    , Message_(other.Message_)
    , Attributes_(~other.Attributes_ ? other.Attributes_->Clone() : NULL)
    , InnerErrors_(other.InnerErrors_)
{ }

TError::TError(TError&& other)
    : Code_(other.Code_)
    , Message_(other.Message_)
    , Attributes_(other.Attributes_)
    , InnerErrors_(MoveRV(other.InnerErrors_))
{ }

TError::TError(const std::exception& ex)
{
    const auto* errorEx = dynamic_cast<const TErrorException*>(&ex);
    if (errorEx) {
        *this = errorEx->Error();
    } else {
        Code_ = Fail;
        Message_ = ex.what();
    }
}

TError::TError(int code, const Stroka& message)
    : Code_(code)
    , Message_(message)
{
    if (!IsOK()) {
        CaptureOriginAttributes();
    }
}

TError::TError(int code, const char* format, ...)
    : Code_(code)
{
    va_list params;
    va_start(params, format);
    vsprintf(Message_, format, params);
    va_end(params);

    if (!IsOK()) {
        CaptureOriginAttributes();
    }
}

TError TError::FromSystem()
{
    return FromSystem(LastSystemError());
}

TError TError::FromSystem(int error)
{
    return TError("%s", LastSystemErrorText(error)) <<
        TErrorAttribute("errno", error);
}

TError& TError::operator= (const TError& other)
{
    if (this != &other) {
        Code_ = other.Code_;
        Message_ = other.Message_;
        Attributes_ = ~other.Attributes_ ? other.Attributes_->Clone() : NULL;
        InnerErrors_ = other.InnerErrors_;
    }
    return *this;
}

TError& TError::operator= (TError&& other)
{
    if (this != &other) {
        Code_ = other.Code_;
        Message_ = MoveRV(other.Message_);
        Attributes_ = other.Attributes_;
        InnerErrors_ = MoveRV(other.InnerErrors_);
    }
    return *this;
}

int TError::GetCode() const
{
    return Code_;
}

TError& TError::SetCode(int code)
{
    Code_ = code;
    return *this;
}

const Stroka& TError::GetMessage() const
{
    return Message_;
}

TError& TError::SetMessage(const Stroka& message)
{
    Message_ = message;
    return *this;
}

const IAttributeDictionary& TError::Attributes() const
{
    return ~Attributes_ ? *Attributes_ : EmptyAttributes();
}

IAttributeDictionary& TError::Attributes()
{
    if (!Attributes_) {
        Attributes_ = CreateEphemeralAttributes();
    }
    return *Attributes_;
}

const std::vector<TError>& TError::InnerErrors() const
{
    return InnerErrors_;
}

std::vector<TError>& TError::InnerErrors()
{
    return InnerErrors_;
}

bool TError::IsOK() const
{
    return Code_ == OK;
}

void TError::CaptureOriginAttributes()
{
    // Use ad-hoc YSON conversions for performance reasons.
    Attributes().SetYson("host", ConvertToYsonString(TRawString(GetLocalHostName())));
    Attributes().SetYson("datetime", ConvertToYsonString(TRawString(ToString(TInstant::Now()))));
    Attributes().SetYson("pid", ConvertToYsonString(getpid()));
    Attributes().SetYson("tid", ConvertToYsonString(NThread::GetCurrentThreadId()));
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void AppendIndent(int indent, Stroka* out)
{
    out->append(Stroka(indent, ' '));
}

void AppendAttribute(const Stroka& key, const Stroka& value, int indent, Stroka* out)
{
    AppendIndent(indent + 4, out);
    out->append(Sprintf("%-15s %s", ~key, ~value));
    out->append('\n');
}

void AppendError(const TError& error, int indent, Stroka* out)
{
    if (error.IsOK()) {
        out->append("OK");
        return;
    }

    AppendIndent(indent, out);
    out->append(error.GetMessage());
    out->append('\n');

    if (error.GetCode() != TError::Fail) {
        AppendAttribute("code", ToString(error.GetCode()), indent, out);
    }

    // Pretty-print origin.
    auto host = error.Attributes().Find<Stroka>("host");
    auto datetime = error.Attributes().Find<Stroka>("datetime");
    auto pid = error.Attributes().Find<i64>("pid");
    auto tid = error.Attributes().Find<i64>("tid");
    if (host && datetime && pid && tid) {
        AppendAttribute(
            "origin",
            Sprintf("%s on %s (pid %d, tid %d)",
                ~host.Get(),
                ~datetime.Get(),
                static_cast<int>(pid.Get()),
                static_cast<int>(tid.Get())),
            indent,
            out);
    }

    // Pretty-print location.
    auto file = error.Attributes().Find<Stroka>("file");
    auto line = error.Attributes().Find<i64>("line");
    if (file && line) {
        AppendAttribute(
            "location",
            Sprintf("%s:%d",
            ~file.Get(),
            static_cast<int>(line.Get())),
            indent,
            out);
    }

    auto keys = error.Attributes().List();
    FOREACH (const auto& key, keys) {
        if (key == "host" ||
            key == "datetime" ||
            key == "pid" ||
            key == "tid" ||
            key == "file" ||
            key == "line")
            continue;

        auto value = error.Attributes().GetYson(key);
        TTokenizer tokenizer(value.Data());
        YCHECK(tokenizer.ParseNext());
        switch (tokenizer.GetCurrentType()) {
            case ETokenType::String:
                AppendAttribute(key, Stroka(tokenizer.CurrentToken().GetStringValue()), indent, out);
                break;
            case ETokenType::Integer:
                AppendAttribute(key, ToString(tokenizer.CurrentToken().GetIntegerValue()), indent, out);
                break;
            case ETokenType::Double:
                AppendAttribute(key, ToString(tokenizer.CurrentToken().GetDoubleValue()), indent, out);
                break;
            default:
                AppendAttribute(key, ConvertToYsonString(value, EYsonFormat::Text).Data(), indent, out);
                break;
        }
    }

    FOREACH (const auto& innerError, error.InnerErrors()) {
        out->append('\n');
        AppendError(innerError, indent + 2, out);
    }
}

} // namespace

Stroka ToString(const TError& error)
{
    Stroka result;
    AppendError(error, 0, &result);
    return result;
}

void ToProto(NYT::NProto::TError* protoError, const TError& error)
{
    protoError->set_code(error.GetCode());
    
    if (!error.GetMessage().empty()) {
        protoError->set_message(error.GetMessage());
    } else {
        protoError->clear_message();
    }

    if (!error.Attributes().List().empty()) {
        ToProto(protoError->mutable_attributes(), error.Attributes());
    } else {
        protoError->clear_attributes();
    }

    protoError->clear_inner_errors();
    FOREACH (const auto& innerError, error.InnerErrors()) {
        ToProto(protoError->add_inner_errors(), innerError);
    }
}

TError FromProto(const NYT::NProto::TError& protoError)
{
    TError error(
        protoError.code(),
        protoError.has_message() ? protoError.message() : "");

    if (protoError.has_attributes()) {
        error.Attributes().MergeFrom(*FromProto(protoError.attributes()));
    }

    FOREACH (const auto& innerProtoError, protoError.inner_errors()) {
        error.InnerErrors().push_back(FromProto(innerProtoError));
    }

    return error;
}

void Serialize(const TError& error, NYTree::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("code").Scalar(error.GetCode())
            .Item("message").Scalar(error.GetMessage())
            .Item("attributes").DoMapFor(error.Attributes().List(), [&] (TFluentMap fluent, const Stroka& key) {
                fluent
                    .Item(key).Scalar(error.Attributes().GetYson(key));
            })
            .DoIf(!error.InnerErrors().empty(), [&] (TFluentMap fluent) {
                fluent
                    .Item("inner_errors").DoListFor(error.InnerErrors(), [=] (TFluentList fluent, const TError& innerError) {
                        fluent
                            .Item().Scalar(innerError);
                    });
            })
        .EndMap();
}

void Deserialize(TError& error, NYTree::INodePtr node)
{
    auto mapNode = node->AsMap();
    
    error = TError(
        mapNode->GetChild("code")->GetValue<i64>(),
        mapNode->GetChild("message")->GetValue<Stroka>());

    error.Attributes().Clear();
    auto attributesNode = mapNode->FindChild("attributes");
    if (attributesNode) {
        error.Attributes().MergeFrom(attributesNode->AsMap());
    }

    error.InnerErrors().clear();
    auto innerErrorsNode = mapNode->FindChild("inner_errors");
    if (innerErrorsNode) {
        FOREACH (auto innerErrorNode, innerErrorsNode->AsList()->GetChildren()) {
            TError innerError;
            Deserialize(innerError, innerErrorNode);
            error.InnerErrors().push_back(innerError);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TError operator << (TError error, const TErrorAttribute& attribute)
{
    error.Attributes().SetYson(attribute.Key, attribute.Value);
    return MoveRV(error);
}

TError operator << (TError error, const TError& innerError)
{
    if (error.IsOK()) {
        return innerError;
    } else {
        error.InnerErrors().push_back(innerError);
        return MoveRV(error);
    }
}

TError operator >>= (const TErrorAttribute& attribute, TError error)
{
    return error << attribute;
}

////////////////////////////////////////////////////////////////////////////////

TErrorException::TErrorException()
{ }

TErrorException::TErrorException(const TErrorException& other)
    : Error_(other.Error_)
{ }

TErrorException::~TErrorException() throw()
{ }

const char* TErrorException::what() const throw()
{
    if (CachedWhat.empty()) {
        CachedWhat = ToString(Error_);
    }
    return ~CachedWhat;
}

TErrorException operator <<= (TErrorException ex, const TError& error)
{
    ex.Error() = error;
    return ex;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
