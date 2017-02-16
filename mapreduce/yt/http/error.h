#pragma once

#include <library/json/json_reader.h>

#include <util/generic/stroka.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TError
{
public:
    TError();
    TError(int code, const Stroka& message);
    TError(const NJson::TJsonValue& value);

    int GetCode() const;
    const Stroka& GetMessage() const;
    const yvector<TError>& InnerErrors() const;

    void ParseFrom(const Stroka& jsonError);

    int GetInnerCode() const;
    bool ContainsErrorCode(int code) const;

    bool ContainsText(const TStringBuf& text) const;

private:
    int Code_;
    Stroka Message_;
    yvector<TError> InnerErrors_;
};

////////////////////////////////////////////////////////////////////////////////

class TErrorResponse
    : public yexception
{
public:
    TErrorResponse(int httpCode, const Stroka& requestId);

    // Check if response is actually not a error.
    bool IsOk() const;

    void SetRawError(const Stroka& rawError);
    void ParseFromJsonError(const Stroka& jsonError);

    int GetHttpCode() const;
    Stroka GetRequestId() const;

    bool IsRetriable() const;
    TDuration GetRetryInterval() const;

    const TError& GetError() const;

    bool IsResolveError() const;
    bool IsAccessDenied() const;
    bool IsConcurrentTransactionLockConflict() const;
    bool IsRequestRateLimitExceeded() const;
    bool IsRequestQueueSizeLimitExceeded() const;
    bool IsChunkUnavailable() const;
    bool IsRequestTimedOut() const;
    bool IsNoSuchTransaction() const;
    bool IsConcurrentOperationsLimitReached() const;

private:
    int HttpCode_;
    Stroka RequestId_;
    Stroka RawError_;
    TError Error_;

    bool Retriable_;
    TDuration RetryInterval_;

    void Setup();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
