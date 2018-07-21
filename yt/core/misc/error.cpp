#include "error.h"
#include "serialize.h"

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/proto/error.pb.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/proc.h>

#include <yt/core/net/local_address.h>

#include <yt/core/tracing/trace_context.h>

#include <yt/core/yson/tokenizer.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/net/address.h>

#include <util/system/error.h>

namespace NYT {

using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TErrorCode::Save(TStreamSaveContext& context) const
{
    NYT::Save(context, Value_);
}

void TErrorCode::Load(TStreamLoadContext& context)
{
    NYT::Load(context, Value_);
}

////////////////////////////////////////////////////////////////////////////////

TError::TErrorOr()
    : Code_(NYT::EErrorCode::OK)
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
        Code_ = NYT::EErrorCode::Generic;
        Message_ = ex.what();
        CaptureOriginAttributes();
    }
}

TError::TErrorOr(const TString& message)
    : Code_(NYT::EErrorCode::Generic)
    , Message_(message)
{
    CaptureOriginAttributes();
}

TError::TErrorOr(TErrorCode code, const TString& message)
    : Code_(code)
    , Message_(message)
{
    if (!IsOK()) {
        CaptureOriginAttributes();
    }
}

TError& TError::operator = (const TError& other)
{
    if (this != &other) {
        Code_ = other.Code_;
        Message_ = other.Message_;
        Attributes_ = other.Attributes_ ? other.Attributes_->Clone() : nullptr;
        InnerErrors_ = other.InnerErrors_;
    }
    return *this;
}

TError& TError::operator = (TError&& other) noexcept
{
    Code_ = other.Code_;
    Message_ = std::move(other.Message_);
    Attributes_ = std::move(other.Attributes_);
    InnerErrors_ = std::move(other.InnerErrors_);
    return *this;
}

TError TError::FromSystem()
{
    return FromSystem(LastSystemError());
}

TError TError::FromSystem(int error)
{
    return TError(LinuxErrorCodeBase + error, "%v", LastSystemErrorText(error)) <<
        TErrorAttribute("errno", error);
}

TErrorCode TError::GetCode() const
{
    return Code_;
}

TError& TError::SetCode(TErrorCode code)
{
    Code_ = code;
    return *this;
}

const TString& TError::GetMessage() const
{
    return Message_;
}

TError& TError::SetMessage(const TString& message)
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

TError TError::Sanitize() const
{
    TError result;
    result.Code_ = Code_;
    result.Message_ = Message_;
    if (Attributes_) {
        // Cf. CaptureOriginAttributes.
        auto attributes = Attributes_->Clone();
        attributes->Remove("host");
        attributes->Remove("datetime");
        attributes->Remove("pid");
        attributes->Remove("tid");
        attributes->Remove("fid");
        attributes->Remove("trace_id");
        attributes->Remove("span_id");
        result.Attributes_ = std::move(attributes);
    }

    for (auto& innerError : InnerErrors_) {
        result.InnerErrors_.push_back(innerError.Sanitize());
    }

    return result;
}

TError TError::Truncate() const
{
    TError result;
    result.Code_ = Code_;
    result.Message_ = Message_;
    result.Attributes_ = Attributes_->Clone();

    if (InnerErrors_.size() <= 2) {
        for (auto& innerError : InnerErrors_) {
            result.InnerErrors_.push_back(innerError.Truncate());
        }
    } else {
        result.Attributes_->Set("inner_errors_truncated", true);
        result.InnerErrors_.push_back(InnerErrors_.front().Truncate());
        result.InnerErrors_.push_back(InnerErrors_.back().Truncate());
    }

    return result;
}

bool TError::IsOK() const
{
    return Code_ == NYT::EErrorCode::OK;
}

void TError::ThrowOnError() const
{
    if (!IsOK()) {
        THROW_ERROR *this;
    }
}

TError TError::Wrap() const
{
    return *this;
}

void TErrorOr<void>::Save(TStreamSaveContext& context) const
{
    using NYT::Save;
    Save(context, Code_);
    Save(context, Message_);
    Save(context, Attributes_);
    Save(context, InnerErrors_);
}

void TErrorOr<void>::Load(TStreamLoadContext& context)
{
    using NYT::Load;
    Load(context, Code_);
    Load(context, Message_);
    Load(context, Attributes_);
    Load(context, InnerErrors_);
}

void TError::CaptureOriginAttributes()
{
    Attributes().Set("host", NNet::ReadLocalHostName());
    Attributes().Set("datetime", TInstant::Now());
    Attributes().Set("pid", ::getpid());
    Attributes().Set("tid", TThread::CurrentThreadId());
    Attributes().Set("fid", NConcurrency::GetCurrentFiberId());
    auto traceContext = NTracing::GetCurrentTraceContext();
    if (traceContext.IsEnabled()) {
        Attributes().SetYson("trace_id", ConvertToYsonString(traceContext.GetTraceId()));
        Attributes().SetYson("span_id", ConvertToYsonString(traceContext.GetSpanId()));
    }
}

TNullable<TError> TError::FindMatching(TErrorCode code) const
{
    if (Code_ == code) {
        return *this;
    }

    for (const auto& innerError : InnerErrors_) {
        auto innerResult = innerError.FindMatching(code);
        if (innerResult) {
            return innerResult;
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

void AppendAttribute(TStringBuilder* builder, const TString& key, const TString& value, int indent)
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

    if (error.GetCode() != NYT::EErrorCode::Generic) {
        AppendAttribute(builder, "code", ToString(static_cast<int>(error.GetCode())), indent);
    }

    // Pretty-print origin.
    auto host = error.Attributes().Find<TString>("host");
    auto datetime = error.Attributes().Find<TString>("datetime");
    auto pid = error.Attributes().Find<pid_t>("pid");
    auto tid = error.Attributes().Find<NConcurrency::TThreadId>("tid");
    auto fid = error.Attributes().Find<NConcurrency::TFiberId>("fid");
    if (host && datetime && pid && tid && fid) {
        AppendAttribute(
            builder,
            "origin",
            Format("%v on %v (pid %v, tid %llx, fid %llx)",
                *host,
                *datetime,
                *pid,
                *tid,
                *fid),
            indent);
    }

    auto keys = error.Attributes().List();
    for (const auto& key : keys) {
        if (key == "host" ||
            key == "datetime" ||
            key == "pid" ||
            key == "tid" ||
            key == "fid")
            continue;

        auto value = error.Attributes().GetYson(key);
        TTokenizer tokenizer(value.GetData());
        YCHECK(tokenizer.ParseNext());
        switch (tokenizer.GetCurrentType()) {
            case ETokenType::String:
                AppendAttribute(builder, key, TString(tokenizer.CurrentToken().GetStringValue()), indent);
                break;
            case ETokenType::Int64:
                AppendAttribute(builder, key, ToString(tokenizer.CurrentToken().GetInt64Value()), indent);
                break;
            case ETokenType::Uint64:
                AppendAttribute(builder, key, ToString(tokenizer.CurrentToken().GetUint64Value()), indent);
                break;
            case ETokenType::Double:
                AppendAttribute(builder, key, ToString(tokenizer.CurrentToken().GetDoubleValue()), indent);
                break;
            case ETokenType::Boolean:
                AppendAttribute(builder, key, TString(FormatBool(tokenizer.CurrentToken().GetBooleanValue())), indent);
                break;
            default:
                AppendAttribute(builder, key, ConvertToYsonString(value, EYsonFormat::Text).GetData(), indent);
                break;
        }
    }

    for (const auto& innerError : error.InnerErrors()) {
        builder->AppendChar('\n');
        AppendError(builder, innerError, indent + 2);
    }
}

} // namespace

bool operator == (const TErrorOr<void>& lhs, const TErrorOr<void>& rhs)
{
    return lhs.GetCode() == rhs.GetCode() &&
        lhs.GetMessage() == rhs.GetMessage() &&
        lhs.Attributes() == rhs.Attributes() &&
        lhs.InnerErrors() == rhs.InnerErrors();
}

bool operator != (const TErrorOr<void>& lhs, const TErrorOr<void>& rhs)
{
    return !(lhs == rhs);
}

TString ToString(const TError& error)
{
    TStringBuilder builder;
    AppendError(&builder, error, 0);
    return builder.Flush();
}

void ToProto(NYT::NProto::TError* protoError, const TError& error)
{
    protoError->set_code(error.Code_);

    if (!error.Message_.empty()) {
        protoError->set_message(error.GetMessage());
    } else {
        protoError->clear_message();
    }

    if (error.Attributes_) {
        ToProto(protoError->mutable_attributes(), *error.Attributes_);
    } else {
        protoError->clear_attributes();
    }

    protoError->clear_inner_errors();
    for (const auto& innerError : error.InnerErrors_) {
        ToProto(protoError->add_inner_errors(), innerError);
    }
}

void FromProto(TError* error, const NYT::NProto::TError& protoError)
{
    error->Code_ = protoError.code();
    error->Message_ = protoError.has_message() ? protoError.message() : TString();
    if (protoError.has_attributes()) {
        error->Attributes_ = FromProto(protoError.attributes());
    } else {
        error->Attributes_.reset();
    }
    error->InnerErrors_ = FromProto<std::vector<TError>>(protoError.inner_errors());
}

void Serialize(
    const TError& error,
    IYsonConsumer* consumer,
    const std::function<void(IYsonConsumer*)>* valueProducer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("code").Value(error.GetCode())
            .Item("message").Value(error.GetMessage())
            .Item("attributes").DoMapFor(error.Attributes().List(), [&] (TFluentMap fluent, const TString& key) {
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
            .DoIf(valueProducer != nullptr, [=] (TFluentMap fluent) {
                fluent
                    .Item("value");
                // NB: we are obligated to deal with a bare consumer here because
                // we can't use void(TFluentMap) in a function signature as it
                // will lead to the inclusion of fluent.h in error.h and a cyclic
                // inclusion error.h -> fluent.h -> callback.h -> error.h
                auto* consumer = fluent.GetConsumer();
                (*valueProducer)(consumer);
            })
        .EndMap();
}

void Deserialize(TError& error, NYTree::INodePtr node)
{
    auto mapNode = node->AsMap();

    error.Code_ = mapNode->GetChild("code")->GetValue<i64>();
    error.Message_ = mapNode->GetChild("message")->GetValue<TString>();
    error.Attributes_ = IAttributeDictionary::FromMap(mapNode->GetChild("attributes")->AsMap());

    error.InnerErrors_.clear();
    auto innerErrorsNode = mapNode->FindChild("inner_errors");
    if (innerErrorsNode) {
        for (const auto& innerErrorNode : innerErrorsNode->AsList()->GetChildren()) {
            auto innerError = ConvertTo<TError>(innerErrorNode);
            error.InnerErrors_.push_back(innerError);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TError operator << (TError error, const TErrorAttribute& attribute)
{
    error.Attributes().SetYson(attribute.Key, attribute.Value);
    return error;
}

TError operator << (TError error, const std::vector<TErrorAttribute>& attributes)
{
    for (const auto& attribute : attributes) {
        error.Attributes().SetYson(attribute.Key, attribute.Value);
    }
    return error;
}

TError operator << (TError error, const TError& innerError)
{
    error.InnerErrors().push_back(innerError);
    return error;
}

TError operator << (TError error, TError&& innerError)
{
    error.InnerErrors().push_back(std::move(innerError));
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

TError operator << (TError error, std::vector<TError>&& innerErrors)
{
    error.InnerErrors().insert(
        error.InnerErrors().end(),
        std::make_move_iterator(innerErrors.begin()),
        std::make_move_iterator(innerErrors.end()));
    return error;
}

TError operator << (TError error, const NYTree::IAttributeDictionary& attributes)
{
    for (const auto& key : attributes.List()) {
        error.Attributes().SetYson(key, attributes.GetYson(key));
    }
    return error;
}

////////////////////////////////////////////////////////////////////////////////

const char* TErrorException::what() const noexcept
{
    if (CachedWhat_.empty()) {
        CachedWhat_ = ToString(Error_);
    }
    return ~CachedWhat_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
