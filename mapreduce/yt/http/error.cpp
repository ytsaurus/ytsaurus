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
        Message_ = it->Second().GetString();
    }

    it = map.find("code");
    if (it != map.end()) {
        Code_ = static_cast<int>(it->Second().GetInteger());
    }

    it = map.find("inner_errors");
    if (it != map.end()) {
        const TJsonValue::TArray& innerErrors = it->Second().GetArray();
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

////////////////////////////////////////////////////////////////////////////////

TErrorResponse::TErrorResponse(int httpCode, const Stroka& requestId)
    : HttpCode_(httpCode)
    , RequestId_(requestId)
{ }

void TErrorResponse::ParseFromJsonError(const Stroka& jsonError)
{
    Error_.ParseFrom(jsonError);
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
    int code = Error_.GetInnerCode();
    if (HttpCode_ / 100 == 4) {
        if (HttpCode_ == 429 || code == 904) { // request rate limit exceeded
            return true;
        }
        if (code / 100 == 7) { // chunk client errors
            return true;
        }
        return false;
    }
    return true;
}

TDuration TErrorResponse::GetRetryInterval() const
{
    int code = Error_.GetInnerCode();
    if (HttpCode_ == 429 || code == 904) { // request rate limit exceeded
        return TConfig::Get()->RateLimitExceededRetryInterval;
    }
    return TConfig::Get()->RetryInterval;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
