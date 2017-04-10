#pragma once

#include <mapreduce/yt/interface/node.h>

#include <util/datetime/base.h>

#include <util/generic/stroka.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>

namespace NJson {
    class TJsonValue;
} // namespace NJson

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TError
{
public:
    TError();
    TError(const Stroka& message);
    TError(int code, const Stroka& message);
    TError(const NJson::TJsonValue& value);
    TError(const TNode& value);

    int GetCode() const;
    const Stroka& GetMessage() const;
    const yvector<TError>& InnerErrors() const;

    void ParseFrom(const Stroka& jsonError);

    int GetInnerCode() const;
    bool ContainsErrorCode(int code) const;

    bool ContainsText(const TStringBuf& text) const;

    bool HasAttributes() const;
    const TNode::TMap& GetAttributes() const;

    Stroka GetYsonText() const;

private:
    int Code_;
    Stroka Message_;
    yvector<TError> InnerErrors_;
    TNode::TMap Attributes_;
};

////////////////////////////////////////////////////////////////////////////////

class TErrorResponse
    : public yexception
{
public:
    TErrorResponse(int httpCode, const Stroka& requestId);
    TErrorResponse(int httpCode, TError error);

    // Check if response is actually not a error.
    bool IsOk() const;

    void SetRawError(const Stroka& message);
    void SetError(TError error);
    void ParseFromJsonError(const Stroka& jsonError);

    int GetHttpCode() const;
    Stroka GetRequestId() const;

    bool IsRetriable() const;
    TDuration GetRetryInterval() const;

    const TError& GetError() const;

    // Path is cypress can't be resolved.
    bool IsResolveError() const;

    // User don't have enough permissions to execute request.
    bool IsAccessDenied() const;

    // Can't take lock since object is already locked by another transaction.
    bool IsConcurrentTransactionLockConflict() const;

    // User sends requests too often.
    bool IsRequestRateLimitExceeded() const;

    // YT can't serve request because it is overloaded.
    bool IsRequestQueueSizeLimitExceeded() const;

    // Some chunk is (hopefully temporary) lost.
    bool IsChunkUnavailable() const;

    // YT experienced some sort of internal timeout.
    bool IsRequestTimedOut() const;

    // User tries to use transaction that was finished or never existed.
    bool IsNoSuchTransaction() const;

    // User reached their limit of concurrently running operations.
    bool IsConcurrentOperationsLimitReached() const;

private:
    int HttpCode_;
    Stroka RequestId_;
    TError Error_;

    bool Retriable_;
    TDuration RetryInterval_;

    void Setup();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
