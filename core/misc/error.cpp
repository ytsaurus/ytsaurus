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
    , Host_(other.Host_)
    , HostHolder_(other.HostHolder_)
    , Datetime_(other.Datetime_)
    , Pid_(other.Pid_)
    , Tid_(other.Tid_)
    , Fid_(other.Fid_)
    , TraceId_(other.TraceId_)
    , SpanId_(other.SpanId_)
    , Attributes_(other.Attributes_ ? other.Attributes_->Clone() : nullptr)
    , InnerErrors_(other.InnerErrors_)
{ }

TError::TErrorOr(TError&& other) noexcept
    : Code_(other.Code_)
    , Message_(std::move(other.Message_))
    , Host_(other.Host_)
    , HostHolder_(std::move(other.HostHolder_))
    , Datetime_(other.Datetime_)
    , Pid_(other.Pid_)
    , Tid_(other.Tid_)
    , Fid_(other.Fid_)
    , TraceId_(other.TraceId_)
    , SpanId_(other.SpanId_)
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

TError::TErrorOr(TString message)
    : Code_(NYT::EErrorCode::Generic)
    , Message_(std::move(message))
{
    CaptureOriginAttributes();
}

TError::TErrorOr(TErrorCode code, TString message)
    : Code_(code)
    , Message_(std::move(message))
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
        Host_ = other.Host_;
        HostHolder_ = other.HostHolder_;
        Datetime_ = other.Datetime_;
        Pid_ = other.Pid_;
        Tid_ = other.Tid_;
        Fid_ = other.Fid_;
        TraceId_ = other.TraceId_;
        SpanId_ = other.SpanId_;
        Attributes_ = other.Attributes_ ? other.Attributes_->Clone() : nullptr;
        InnerErrors_ = other.InnerErrors_;
    }
    return *this;
}

TError& TError::operator = (TError&& other) noexcept
{
    Code_ = other.Code_;
    Message_ = std::move(other.Message_);
    Host_ = other.Host_;
    HostHolder_ = std::move(other.HostHolder_);
    Datetime_ = other.Datetime_;
    Pid_ = other.Pid_;
    Tid_ = other.Tid_;
    Fid_ = other.Fid_;
    TraceId_ = other.TraceId_;
    SpanId_ = other.SpanId_;
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
    return TError(LinuxErrorCodeBase + error, LastSystemErrorText(error)) <<
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

TErrorCode TError::GetNonTrivialCode() const
{
    if (Code_ != NYT::EErrorCode::Generic) {
        return Code_;
    }

    for (const auto& innerError : InnerErrors_) {
        auto innerCode = innerError.GetNonTrivialCode();
        if (innerCode != NYT::EErrorCode::Generic) {
            return innerCode;
        }
    }

    return Code_;
}

const TString& TError::GetMessage() const
{
    return Message_;
}

TError& TError::SetMessage(TString message)
{
    Message_ = std::move(message);
    return *this;
}

bool TError::HasOriginAttributes() const
{
    return Host_.operator bool();
}

TStringBuf TError::GetHost() const
{
    return Host_;
}

TInstant TError::GetDatetime() const
{
    return Datetime_;
}

TProcessId TError::GetPid() const
{
    return Pid_;
}

NConcurrency::TThreadId TError::GetTid() const
{
    return Tid_;
}

NConcurrency::TFiberId TError::GetFid() const
{
    return Fid_;
}

bool TError::HasTracingAttributes() const
{
    return TraceId_ != NTracing::InvalidTraceId;
}

NTracing::TTraceId TError::GetTraceId() const
{
    return TraceId_;
}

NTracing::TSpanId TError::GetSpanId() const
{
    return SpanId_;
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
        result.Attributes_ = Attributes_->Clone();
    }
    for (const auto& innerError : InnerErrors_) {
        result.InnerErrors_.push_back(innerError.Sanitize());
    }
    return result;
}

TError TError::Truncate() const
{
    TError result;
    result.Code_ = Code_;
    result.Message_ = Message_;
    if (Attributes_) {
        result.Attributes_ = Attributes_->Clone();
    }
    if (InnerErrors_.size() <= 2) {
        for (const auto& innerError : InnerErrors_) {
            result.InnerErrors_.push_back(innerError.Truncate());
        }
    } else {
        static const TString InnerErrorsTruncatedKey("inner_errors_truncated");
        result.Attributes().Set(InnerErrorsTruncatedKey, true);
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

    // Cf. TAttributeDictionaryValueSerializer.
    auto attributePairs = Attributes_ ? Attributes_->ListPairs() : std::vector<IAttributeDictionary::TKeyValuePair>();
    size_t attributeCount = attributePairs.size();
    if (HasOriginAttributes()) {
        attributeCount += 5;
    }
    if (HasTracingAttributes()) {
        attributeCount += 2;
    }

    if (attributeCount > 0) {
        // Cf. TAttributeDictionaryRefSerializer.
        Save(context, true);

        TSizeSerializer::Save(context, attributeCount);

        auto saveAttribute = [&] (const TString& key, const auto& value) {
            Save(context, key);
            Save(context, ConvertToYsonString(value));
        };

        if (HasOriginAttributes()) {
            static const TString HostKey("host");
            saveAttribute(HostKey, Host_);

            static const TString DatetimeKey("datetime");
            saveAttribute(DatetimeKey, Datetime_);

            static const TString PidKey("pid");
            saveAttribute(PidKey, Pid_);

            static const TString TidKey("tid");
            saveAttribute(TidKey, Tid_);

            static const TString FidKey("fid");
            saveAttribute(FidKey, Fid_);
        }

        if (HasTracingAttributes()) {
            static const TString TraceIdKey("trace_id");
            saveAttribute(TraceIdKey, TraceId_);

            static const TString SpanIdKey("span_id");
            saveAttribute(SpanIdKey, SpanId_);
        }

        std::sort(attributePairs.begin(), attributePairs.end(), [] (const auto& lhs, const auto& rhs) {
            return lhs.first < rhs.first;
        });
        for (const auto& [key, value] : attributePairs) {
            Save(context, key);
            Save(context, value);
        }
    } else {
        Save(context, false);
    }

    Save(context, InnerErrors_);
}

void TErrorOr<void>::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    Load(context, Code_);
    Load(context, Message_);

    Load(context, Attributes_);
    ExtractOriginAttributes();

    Load(context, InnerErrors_);
}

void TError::CaptureOriginAttributes()
{
    Host_ = NNet::ReadLocalHostName();
    Datetime_ = TInstant::Now();
    Pid_ = GetPID();
    Tid_ = TThread::CurrentThreadId();
    Fid_ = NConcurrency::GetCurrentFiberId();
    if (const auto* traceContext = NTracing::GetCurrentTraceContext()) {
        TraceId_ = traceContext->GetTraceId();
        SpanId_ = traceContext->GetSpanId();
    }
}

void TError::ExtractOriginAttributes()
{
    if (!Attributes_) {
        return;
    }

    static const TString HostKey("host");
    HostHolder_ = Attributes_->GetAndRemove<TString>(HostKey, TString());
    Host_ = HostHolder_.empty() ? TStringBuf() : HostHolder_;

    static const TString DatetimeKey("datetime");
    Datetime_ = Attributes_->GetAndRemove<TInstant>(DatetimeKey, TInstant());

    static const TString PidKey("pid");
    Pid_ = Attributes_->GetAndRemove<TProcessId>(PidKey, 0);

    static const TString TidKey("tid");
    Tid_ = Attributes_->GetAndRemove<NConcurrency::TThreadId>(TidKey, NConcurrency::InvalidThreadId);

    static const TString FidKey("fid");
    Fid_ = Attributes_->GetAndRemove<NConcurrency::TFiberId>(FidKey, NConcurrency::InvalidFiberId);

    static const TString TraceIdKey("trace_id");
    // COMPAT(babenko): some older versions use uint64 for trace id.
    try {
        TraceId_ = Attributes_->GetAndRemove<NTracing::TTraceId>(TraceIdKey, NTracing::InvalidTraceId);
    } catch (const std::exception&) {
        TraceId_ = NTracing::TTraceId(Attributes_->GetAndRemove<ui64>(TraceIdKey, 0), 0);
    }

    static const TString SpanIdKey("span_id");
    SpanId_ = Attributes_->GetAndRemove<NTracing::TSpanId>(SpanIdKey, NTracing::InvalidSpanId);
}

std::optional<TError> TError::FindMatching(TErrorCode code) const
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

    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void AppendIndent(TStringBuilderBase* builer, int indent)
{
    builer->AppendChar(' ', indent);
}

void AppendAttribute(TStringBuilderBase* builder, const TString& key, const TString& value, int indent)
{
    AppendIndent(builder, indent + 4);
    builder->AppendFormat("%-15s %s", key, value);
    builder->AppendChar('\n');
}

void AppendError(TStringBuilderBase* builder, const TError& error, int indent)
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
    if (error.HasOriginAttributes()) {
        AppendAttribute(
            builder,
            "origin",
            Format("%v on %v (pid %v, tid %llx, fid %llx)",
                error.GetHost(),
                error.GetDatetime(),
                error.GetPid(),
                error.GetTid(),
                error.GetFid()),
            indent);
    }

    for (const auto& [key, value] : error.Attributes().ListPairs()) {
        TTokenizer tokenizer(value.GetData());
        YT_VERIFY(tokenizer.ParseNext());
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
    return
        lhs.GetCode() == rhs.GetCode() &&
        lhs.GetMessage() == rhs.GetMessage() &&
        lhs.GetHost() == rhs.GetHost() &&
        lhs.GetDatetime() == rhs.GetDatetime() &&
        lhs.GetPid() == rhs.GetPid() &&
        lhs.GetTid() == rhs.GetTid() &&
        lhs.GetFid() == rhs.GetFid() &&
        lhs.GetTraceId() == rhs.GetTraceId() &&
        lhs.GetSpanId() == rhs.GetSpanId() &&
        lhs.Attributes() == rhs.Attributes() &&
        lhs.InnerErrors() == rhs.InnerErrors();
}

bool operator != (const TErrorOr<void>& lhs, const TErrorOr<void>& rhs)
{
    return !(lhs == rhs);
}

void FormatValue(TStringBuilderBase* builder, const TError& error, TStringBuf /*spec*/)
{
    AppendError(builder, error, 0);
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

    if (error.Message_.empty()) {
        protoError->clear_message();
    } else {
        protoError->set_message(error.GetMessage());
    }

    protoError->clear_attributes();
    if (error.Attributes_) {
        ToProto(protoError->mutable_attributes(), *error.Attributes_);
    }

    auto addAttribute = [&] (const TString& key, const auto& value) {
        auto* protoItem = protoError->mutable_attributes()->add_attributes();
        protoItem->set_key(key);
        protoItem->set_value(ConvertToYsonString(value).GetData());
    };

    if (error.HasOriginAttributes()) {
        static const TString HostKey("host");
        addAttribute(HostKey, error.Host_);

        static const TString DatetimeKey("datetime");
        addAttribute(DatetimeKey, error.Datetime_);

        static const TString PidKey("pid");
        addAttribute(PidKey, error.Pid_);

        static const TString TidKey("tid");
        addAttribute(TidKey, error.Tid_);

        static const TString FidKey("fid");
        addAttribute(FidKey, error.Fid_);
    }

    if (error.HasTracingAttributes()) {
        static const TString TraceIdKey("trace_id");
        addAttribute(TraceIdKey, error.TraceId_);

        static const TString SpanIdKey("span_id");
        addAttribute(SpanIdKey, error.SpanId_);
    }

    protoError->clear_inner_errors();
    for (const auto& innerError : error.InnerErrors_) {
        ToProto(protoError->add_inner_errors(), innerError);
    }
}

void FromProto(TError* error, const NYT::NProto::TError& protoError)
{
    error->Code_ = protoError.code();
    error->Message_ = protoError.message();
    if (protoError.has_attributes()) {
        error->Attributes_ = FromProto(protoError.attributes());
        error->ExtractOriginAttributes();
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
            .Item("attributes").DoMap([&] (auto fluent) {
                if (error.HasOriginAttributes()) {
                    fluent
                        .Item("host").Value(error.GetHost())
                        .Item("datetime").Value(error.GetDatetime())
                        .Item("pid").Value(error.GetPid())
                        .Item("tid").Value(error.GetTid())
                        .Item("fid").Value(error.GetFid());
                }
                if (error.HasTracingAttributes()) {
                    fluent
                        .Item("trace_id").Value(error.GetTraceId())
                        .Item("span_id").Value(error.GetSpanId());
                }
                for (const auto& [key, value] : error.Attributes().ListPairs()) {
                    fluent
                        .Item(key).Value(value);
                }
            })
            .DoIf(!error.InnerErrors().empty(), [&] (auto fluent) {
                fluent
                    .Item("inner_errors").DoListFor(error.InnerErrors(), [=] (auto fluent, const auto& innerError) {
                        fluent
                            .Item().Value(innerError);
                    });
            })
            .DoIf(valueProducer != nullptr, [&] (auto fluent) {
                auto* consumer = fluent.GetConsumer();
                // NB: we are forced to deal with a bare consumer here because
                // we can't use void(TFluentMap) in a function signature as it
                // will lead to the inclusion of fluent.h in error.h and a cyclic
                // inclusion error.h -> fluent.h -> callback.h -> error.h
                consumer->OnKeyedItem(AsStringBuf("value"));
                (*valueProducer)(consumer);
            })
        .EndMap();
}

void Deserialize(TError& error, const NYTree::INodePtr& node)
{
    auto mapNode = node->AsMap();

    static const TString CodeKey("code");
    error.Code_ = mapNode->GetChild(CodeKey)->GetValue<i64>();

    static const TString MessageKey("message");
    error.Message_ = mapNode->GetChild(MessageKey)->GetValue<TString>();

    static const TString AttributesKey("attributes");
    error.Attributes_ = IAttributeDictionary::FromMap(mapNode->GetChild(AttributesKey)->AsMap());
    error.ExtractOriginAttributes();

    error.InnerErrors_.clear();
    static const TString InnerErrorsKey("inner_errors");
    auto innerErrorsNode = mapNode->FindChild(InnerErrorsKey);
    if (innerErrorsNode) {
        for (const auto& innerErrorNode : innerErrorsNode->AsList()->GetChildren()) {
            error.InnerErrors_.push_back(ConvertTo<TError>(innerErrorNode));
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
    error.Attributes().MergeFrom(attributes);
    return error;
}

////////////////////////////////////////////////////////////////////////////////

const char* TErrorException::what() const noexcept
{
    if (CachedWhat_.empty()) {
        CachedWhat_ = ToString(Error_);
    }
    return CachedWhat_.data();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
