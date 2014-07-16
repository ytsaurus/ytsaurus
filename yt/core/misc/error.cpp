#include "stdafx.h"
#include "error.h"

#include <core/misc/address.h>

#include <core/ytree/convert.h>
#include <core/ytree/fluent.h>

#include <core/yson/tokenizer.h>

#include <util/system/error.h>

namespace NYT {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TError::TErrorOr()
    : Code_(OK)
{ }

TError::TErrorOr(const TError& other)
    : Code_(other.Code_)
    , Message_(other.Message_)
    , Attributes_(other.Attributes_ ? other.Attributes_->Clone() : nullptr)
    , InnerErrors_(other.InnerErrors_)
{ }

TError::TErrorOr(TError&& other) noexcept
    : Code_(other.Code_)
    , Message_(std::move(other.Message_))
    , Attributes_(std::move(other.Attributes_))
    , InnerErrors_(std::move(other.InnerErrors_))
{ }

TError::TErrorOr(const std::exception& ex)
{
    const auto* errorEx = dynamic_cast<const TErrorException*>(&ex);
    if (errorEx) {
        *this = errorEx->Error();
    } else {
        Code_ = GenericFailure;
        Message_ = ex.what();
    }
}

TError::TErrorOr(const Stroka& message)
    : Code_(GenericFailure)
    , Message_(message)
{
    CaptureOriginAttributes();
}

TError::TErrorOr(int code, const Stroka& message)
    : Code_(code)
    , Message_(message)
{
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
    return TError("%v", LastSystemErrorText(error)) <<
        TErrorAttribute("errno", error);
}

TError& TError::operator= (const TError& other)
{
    if (this != &other) {
        Code_ = other.Code_;
        Message_ = other.Message_;
        Attributes_ = other.Attributes_ ? other.Attributes_->Clone() : nullptr;
        InnerErrors_ = other.InnerErrors_;
    }
    return *this;
}

TError& TError::operator= (TError&& other) noexcept
{
    if (this != &other) {
        Code_ = other.Code_;
        Message_ = std::move(other.Message_);
        Attributes_ = std::move(other.Attributes_);
        InnerErrors_ = std::move(other.InnerErrors_);
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
    return Attributes_ ? *Attributes_ : EmptyAttributes();
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
    Attributes().SetYson("host", ConvertToYsonString(TAddressResolver::Get()->GetLocalHostName()));
    Attributes().SetYson("datetime", ConvertToYsonString(ToString(TInstant::Now())));
    Attributes().SetYson("pid", ConvertToYsonString(getpid()));
    Attributes().SetYson("tid", ConvertToYsonString(NConcurrency::GetCurrentThreadId()));
}

TNullable<TError> TError::FindMatching(int code) const
{
    if (Code_ == code) {
        return *this;
    }

    for (const auto& innerError : InnerErrors_) {
        auto innerResult = innerError.FindMatching(code);
        if (innerResult) {
            return std::move(innerResult);
        }
    }

    return Null;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void AppendIndent(TStringBuilder* builer, int indent)
{
    builer->AppendChar(' ', indent);
}

void AppendAttribute(TStringBuilder* builder, const Stroka& key, const Stroka& value, int indent)
{
    AppendIndent(builder, indent + 4);
    builder->AppendFormat("%-15s %s", key, value);
    builder->AppendChar('\n');
}

void AppendError(TStringBuilder* builder, const TError& error, int indent)
{
    if (error.IsOK()) {
        builder->AppendString("OK");
        return;
    }

    AppendIndent(builder, indent);
    builder->AppendString(error.GetMessage());
    builder->AppendChar('\n');

    if (error.GetCode() != TError::GenericFailure) {
        AppendAttribute(builder, "code", ToString(error.GetCode()), indent);
    }

    // Pretty-print origin.
    auto host = error.Attributes().Find<Stroka>("host");
    auto datetime = error.Attributes().Find<Stroka>("datetime");
    auto pid = error.Attributes().Find<i64>("pid");
    auto tid = error.Attributes().Find<i64>("tid");
    if (host && datetime && pid && tid) {
        AppendAttribute(
            builder,
            "origin",
            Format("%v on %v (pid %v, tid %v)",
                *host,
                *datetime,
                *pid,
                *tid),
            indent);
    }

    // Pretty-print location.
    auto file = error.Attributes().Find<Stroka>("file");
    auto line = error.Attributes().Find<i64>("line");
    if (file && line) {
        AppendAttribute(
            builder,
            "location",
            Format("%v:%v",
                *file,
                *line),
            indent);
    }

    auto keys = error.Attributes().List();
    for (const auto& key : keys) {
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
                AppendAttribute(builder, key, Stroka(tokenizer.CurrentToken().GetStringValue()), indent);
                break;
            case ETokenType::Int64:
                AppendAttribute(builder, key, ToString(tokenizer.CurrentToken().GetInt64Value()), indent);
                break;
            case ETokenType::Double:
                AppendAttribute(builder, key, ToString(tokenizer.CurrentToken().GetDoubleValue()), indent);
                break;
            case ETokenType::Boolean:
                AppendAttribute(builder, key, Stroka(FormatBool(tokenizer.CurrentToken().GetBooleanValue())), indent);
                break;
            default:
                AppendAttribute(builder, key, ConvertToYsonString(value, EYsonFormat::Text).Data(), indent);
                break;
        }
    }

    for (const auto& innerError : error.InnerErrors()) {
        builder->AppendChar('\n');
        AppendError(builder, innerError, indent + 2);
    }
}

} // namespace

Stroka ToString(const TError& error)
{
    TStringBuilder builder;
    AppendError(&builder, error, 0);
    return builder.Flush();
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
    for (const auto& innerError : error.InnerErrors()) {
        ToProto(protoError->add_inner_errors(), innerError);
    }
}

void FromProto(TError* error, const NYT::NProto::TError& protoError)
{
    *error = TError(
        protoError.code(),
        protoError.has_message() ? protoError.message() : "");

    if (protoError.has_attributes()) {
        error->Attributes().MergeFrom(*FromProto(protoError.attributes()));
    }

    error->InnerErrors() = FromProto<TError>(protoError.inner_errors());
}

void Serialize(const TError& error, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("code").Value(error.GetCode())
            .Item("message").Value(error.GetMessage())
            .Item("attributes").DoMapFor(error.Attributes().List(), [&] (TFluentMap fluent, const Stroka& key) {
                fluent
                    .Item(key).Value(error.Attributes().GetYson(key));
            })
            .DoIf(!error.InnerErrors().empty(), [&] (TFluentMap fluent) {
                fluent
                    .Item("inner_errors").DoListFor(error.InnerErrors(), [=] (TFluentList fluent, const TError& innerError) {
                        fluent
                            .Item().Value(innerError);
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
        for (auto innerErrorNode : innerErrorsNode->AsList()->GetChildren()) {
            auto innerError = ConvertTo<TError>(innerErrorNode);
            error.InnerErrors().push_back(innerError);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TError operator << (TError error, const TErrorAttribute& attribute)
{
    error.Attributes().SetYson(attribute.Key, attribute.Value);
    return error;
}

TError operator << (TError error, const TError& innerError)
{
    error.InnerErrors().push_back(innerError);
    return error;
}

TError operator << (TError error, const std::vector<TError>& innerErrors)
{
    error.InnerErrors().insert(
        error.InnerErrors().end(),
        innerErrors.begin(),
        innerErrors.end());
    return error;
}

TError operator >>= (const TErrorAttribute& attribute, TError error)
{
    return error << attribute;
}

////////////////////////////////////////////////////////////////////////////////

const char* TErrorException::what() const throw()
{
    if (CachedWhat.empty()) {
        CachedWhat = ToString(Error_);
    }
    return ~CachedWhat;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TError> OKFuture = MakeFuture(TError());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
