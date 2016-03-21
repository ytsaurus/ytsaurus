#include "error.h"

#include <mapreduce/yt/common/config.h>

#include <util/stream/str.h>

namespace NYT {

using namespace NJson;

////////////////////////////////////////////////////////////////////////////////

TError::TError()
    : Code_(0)
{ }

TError::TError(int code, const Stroka& message)
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
    }

    it = map.find("inner_errors");
    if (it != map.end()) {
        const TJsonValue::TArray& innerErrors = it->second.GetArray();
        for (const auto& innerError : innerErrors) {
            InnerErrors_.push_back(TError(innerError));
        }
    }
}

int TError::GetCode() const
{
    return Code_;
}

const Stroka& TError::GetMessage() const
{
    return Message_;
}

const yvector<TError>& TError::InnerErrors() const
{
    return InnerErrors_;
}

void TError::ParseFrom(const Stroka& jsonError)
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

////////////////////////////////////////////////////////////////////////////////

TErrorResponse::TErrorResponse(int httpCode, const Stroka& requestId)
    : HttpCode_(httpCode)
    , RequestId_(requestId)
{ }

const char* TErrorResponse::what() const throw ()
{
    return ~RawError_;
}

void TErrorResponse::SetRawError(const Stroka& rawError)
{
    RawError_ = rawError;
    Setup();
}

void TErrorResponse::ParseFromJsonError(const Stroka& jsonError)
{
    RawError_ = jsonError;
    Error_.ParseFrom(jsonError);
    Setup();
}

int TErrorResponse::GetHttpCode() const
{
    return HttpCode_;
}

Stroka TErrorResponse::GetRequestId() const
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

void TErrorResponse::Setup()
{
    Retriable_ = true;
    RetryInterval_ = TConfig::Get()->RetryInterval;

    int code = Error_.GetInnerCode();
    if (HttpCode_ / 100 == 4) {
        if (HttpCode_ == 429 || code == 904) {
            // request rate limit exceeded
            RetryInterval_ = TConfig::Get()->RateLimitExceededRetryInterval;
            return;
        }
        if (code == 202 || Error_.ContainsText("Limit for the number of concurrent operations")) {
            // limit for the number of concurrent operations exceeded
            RetryInterval_ = TConfig::Get()->StartOperationRetryInterval;
            return;
        }
        if (code / 100 == 7) {
            // chunk client errors
            return;
        }
        Retriable_ = false;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
