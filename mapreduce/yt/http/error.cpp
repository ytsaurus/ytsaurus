#include "error.h"

#include <mapreduce/yt/common/config.h>

#include <util/string/builder.h>
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

////////////////////////////////////////////////////////////////////////////////

TErrorResponse::TErrorResponse(int httpCode, const Stroka& requestId)
    : HttpCode_(httpCode)
    , RequestId_(requestId)
{ }

bool TErrorResponse::IsOk() const
{
    return Error_.GetCode() == 0;
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

bool TErrorResponse::IsResolveError() const
{
    return Error_.ContainsErrorCode(500);
}

bool TErrorResponse::IsAccessDenied() const
{
    return Error_.ContainsErrorCode(901);
}

bool TErrorResponse::IsConcurrentTransactionLockConflict() const
{
    return Error_.ContainsErrorCode(402);
}

bool TErrorResponse::IsRequestRateLimitExceeded() const
{
    return Error_.ContainsErrorCode(904);
}

bool TErrorResponse::IsRequestQueueSizeLimitExceeded() const
{
    return Error_.ContainsErrorCode(108);
}

bool TErrorResponse::IsChunkUnavailable() const
{
    return Error_.ContainsErrorCode(716);
}

bool TErrorResponse::IsRequestTimedOut() const
{
    return Error_.ContainsErrorCode(3);
}

bool TErrorResponse::IsNoSuchTransaction() const
{
    return Error_.ContainsErrorCode(11000);
}

bool TErrorResponse::IsConcurrentOperationsLimitReached() const
{
    return Error_.ContainsErrorCode(202);
}

void TErrorResponse::Setup()
{
    *this << Error_.GetMessage() << ". Raw error: " << RawError_;

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
