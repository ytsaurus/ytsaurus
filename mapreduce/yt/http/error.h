#pragma once

#include <mapreduce/yt/interface/node.h>

#include <util/generic/string.h>
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
    TError(const TString& message);
    TError(int code, const TString& message);
    TError(const NJson::TJsonValue& value);
    TError(const TNode& value);

    int GetCode() const;
    const TString& GetMessage() const;
    const yvector<TError>& InnerErrors() const;

    void ParseFrom(const TString& jsonError);

    int GetInnerCode() const;
    bool ContainsErrorCode(int code) const;

    bool ContainsText(const TStringBuf& text) const;

    bool HasAttributes() const;
    const TNode::TMap& GetAttributes() const;

    TString GetYsonText() const;

private:
    int Code_;
    TString Message_;
    yvector<TError> InnerErrors_;
    TNode::TMap Attributes_;
};

////////////////////////////////////////////////////////////////////////////////

class TErrorResponse
    : public yexception
{
public:
    TErrorResponse(int httpCode, const TString& requestId);
    TErrorResponse(int httpCode, TError error);

    // Check if response is actually not a error.
    bool IsOk() const;

    void SetRawError(const TString& message);
    void SetError(TError error);
    void ParseFromJsonError(const TString& jsonError);

    int GetHttpCode() const;
    TString GetRequestId() const;

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
    void Setup();

private:
    int HttpCode_;
    TString RequestId_;
    TError Error_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
