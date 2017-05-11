#include "error.h"
#include "error_codes.h"

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/node_visitor.h>

#include <library/json/json_reader.h>
#include <library/yson/writer.h>

#include <util/string/builder.h>
#include <util/stream/str.h>

namespace NYT {

using namespace NJson;

////////////////////////////////////////////////////////////////////

static void WriteErrorDescription(const TError& error, TOutputStream* out)
{
    (*out) << '`' << error.GetMessage() << '\'';
    const auto& innerErrorList = error.InnerErrors();
    if (!innerErrorList.empty()) {
        (*out) << " { ";
        bool first = true;
        for (const auto& innerError : innerErrorList) {
            if (first) {
                first = false;
            } else {
                (*out) << " ; ";
            }
            WriteErrorDescription(innerError, out);
        }
        (*out) << " }";
    }
}

static void SerializeError(const TError& error, IYsonConsumer* consumer)
{
    consumer->OnBeginMap();
    {
        consumer->OnKeyedItem("code");
        consumer->OnInt64Scalar(error.GetCode());

        consumer->OnKeyedItem("message");
        consumer->OnStringScalar(error.GetMessage());

        if (!error.GetAttributes().empty()) {
            consumer->OnKeyedItem("attributes");
            consumer->OnBeginMap();
            {
                for (const auto& item : error.GetAttributes()) {
                    consumer->OnKeyedItem(item.first);
                    TNodeVisitor(consumer).Visit(item.second);
                }
            }
            consumer->OnEndMap();
        }

        if (!error.InnerErrors().empty()) {
            consumer->OnKeyedItem("inner_errors");
            {
                consumer->OnBeginList();
                for (const auto& innerError : error.InnerErrors()) {
                    SerializeError(innerError, consumer);
                }
                consumer->OnEndList();
            }
        }
    }
    consumer->OnEndMap();
}

////////////////////////////////////////////////////////////////////

TError::TError()
    : Code_(0)
{ }

TError::TError(const TString& message)
    : Code_(NYT::NClusterErrorCodes::Generic)
    , Message_(message)
{ }

TError::TError(int code, const TString& message)
    : Code_(code)
    , Message_(message)
{ }

TError::TError(const TJsonValue& value)
{
    const TJsonValue::TMap& map = value.GetMap();
    TJsonValue::TMap::const_iterator it = map.find("message");
    if (it != map.end()) {
        Message_ = it->second.GetString();
    }

    it = map.find("code");
    if (it != map.end()) {
        Code_ = static_cast<int>(it->second.GetInteger());
    } else {
        Code_ = NYT::NClusterErrorCodes::Generic;
    }

    it = map.find("inner_errors");
    if (it != map.end()) {
        const TJsonValue::TArray& innerErrors = it->second.GetArray();
        for (const auto& innerError : innerErrors) {
            InnerErrors_.push_back(TError(innerError));
        }
    }

    it = map.find("attributes");
    if (it != map.end()) {
        auto attributes = NYT::NodeFromJsonValue(it->second);
        if (attributes.IsMap()) {
            Attributes_ = std::move(attributes.AsMap());
        }
    }
}

TError::TError(const TNode& node)
{
    const auto& map = node.AsMap();
    auto it = map.find("message");
    if (it != map.end()) {
        Message_ = it->second.AsString();
    }

    it = map.find("code");
    if (it != map.end()) {
        Code_ = static_cast<int>(it->second.AsInt64());
    } else {
        Code_ = NYT::NClusterErrorCodes::Generic;
    }

    it = map.find("inner_errors");
    if (it != map.end()) {
        const auto& innerErrors = it->second.AsList();
        for (const auto& innerError : innerErrors) {
            InnerErrors_.push_back(TError(innerError));
        }
    }

    it = map.find("attributes");
    if (it != map.end()) {
        auto& attributes = it->second;
        if (attributes.IsMap()) {
            Attributes_ = std::move(attributes.AsMap());
        }
    }
}

int TError::GetCode() const
{
    return Code_;
}

const TString& TError::GetMessage() const
{
    return Message_;
}

const yvector<TError>& TError::InnerErrors() const
{
    return InnerErrors_;
}

void TError::ParseFrom(const TString& jsonError)
{
    TJsonValue value;
    TStringInput input(jsonError);
    ReadJsonTree(&input, &value);
    *this = TError(value);
}

int TError::GetInnerCode() const
{
    if (Code_ >= 100) {
        return Code_;
    }
    for (const auto& error : InnerErrors_) {
        int code = error.GetInnerCode();
        if (code) {
            return code;
        }
    }
    return 0;
}

bool TError::ContainsErrorCode(int code) const
{
    if (Code_ == code) {
        return true;
    }
    for (const auto& error : InnerErrors_) {
        if (error.ContainsErrorCode(code)) {
            return true;
        }
    }
    return false;
}


bool TError::ContainsText(const TStringBuf& text) const
{
    if (Message_.Contains(text)) {
        return true;
    }
    for (const auto& error : InnerErrors_) {
        if (error.ContainsText(text)) {
            return true;
        }
    }
    return false;
}

bool TError::HasAttributes() const
{
    return !Attributes_.empty();
}

const TNode::TMap& TError::GetAttributes() const
{
    return Attributes_;
}

TString TError::GetYsonText() const
{
    TStringStream out;
    TYsonWriter writer(&out, YF_TEXT);
    SerializeError(*this, &writer);
    return std::move(out.Str());
}

////////////////////////////////////////////////////////////////////////////////

TErrorResponse::TErrorResponse(int httpCode, const TString& requestId)
    : HttpCode_(httpCode)
    , RequestId_(requestId)
{ }

bool TErrorResponse::IsOk() const
{
    return Error_.GetCode() == 0;
}

void TErrorResponse::SetRawError(const TString& message)
{
    Error_ = TError(message);
    Setup();
}

void TErrorResponse::SetError(TError error)
{
    Error_ = std::move(error);
    Setup();
}

void TErrorResponse::ParseFromJsonError(const TString& jsonError)
{
    Error_.ParseFrom(jsonError);
    Setup();
}

int TErrorResponse::GetHttpCode() const
{
    return HttpCode_;
}

TString TErrorResponse::GetRequestId() const
{
    return RequestId_;
}

bool TErrorResponse::IsRetriable() const
{
    return Retriable_;
}

TDuration TErrorResponse::GetRetryInterval() const
{
    return RetryInterval_;
}

const TError& TErrorResponse::GetError() const
{
    return Error_;
}

bool TErrorResponse::IsResolveError() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NYTree::ResolveError);
}

bool TErrorResponse::IsAccessDenied() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NSecurityClient::AuthorizationError);
}

bool TErrorResponse::IsConcurrentTransactionLockConflict() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NCypressClient::ConcurrentTransactionLockConflict);
}

bool TErrorResponse::IsRequestRateLimitExceeded() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NSecurityClient::RequestQueueSizeLimitExceeded);
}

bool TErrorResponse::IsRequestQueueSizeLimitExceeded() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NRpc::RequestQueueSizeLimitExceeded);
}

bool TErrorResponse::IsChunkUnavailable() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NChunkClient::ChunkUnavailable);
}

bool TErrorResponse::IsRequestTimedOut() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::Timeout);
}

bool TErrorResponse::IsNoSuchTransaction() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NTransactionClient::NoSuchTransaction);
}

bool TErrorResponse::IsConcurrentOperationsLimitReached() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NScheduler::TooManyOperations);
}

void TErrorResponse::Setup()
{
    TStringStream s;
    WriteErrorDescription(Error_, &s);
    *this << s.Str() << "; full error: " << Error_.GetYsonText();

    Retriable_ = true;
    RetryInterval_ = TConfig::Get()->RetryInterval;

    int code = Error_.GetInnerCode();
    if (HttpCode_ / 100 == 4) {
        if (HttpCode_ == 429 || code == 904 || code == 108) {
            // request rate limit exceeded
            RetryInterval_ = TConfig::Get()->RateLimitExceededRetryInterval;
            return;
        }
        if (IsConcurrentOperationsLimitReached()) {
            // limit for the number of concurrent operations exceeded
            RetryInterval_ = TConfig::Get()->StartOperationRetryInterval;
            return;
        }
        if (code / 100 == 7) {
            // chunk client errors
            RetryInterval_ = TConfig::Get()->ChunkErrorsRetryInterval;
            return;
        }
        Retriable_ = false;
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NYT
