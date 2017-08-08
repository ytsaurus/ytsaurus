#pragma once

#include "fwd.h"
#include "common.h"

#include <util/generic/yexception.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <mapreduce/yt/node/node.h>

namespace NJson {
    class TJsonValue;
} // namespace NJson

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TApiUsageError
    : public yexception
{ };

////////////////////////////////////////////////////////////////////////////////

class TYtError
{
public:
    TYtError();
    TYtError(const TString& message);
    TYtError(int code, const TString& message);
    TYtError(const NJson::TJsonValue& value);
    TYtError(const TNode& value);

    int GetCode() const;
    const TString& GetMessage() const;
    const yvector<TYtError>& InnerErrors() const;

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
    yvector<TYtError> InnerErrors_;
    TNode::TMap Attributes_;
};

////////////////////////////////////////////////////////////////////////////////

class TErrorResponse
    : public yexception
{
public:
    TErrorResponse(int httpCode, const TString& requestId);
    TErrorResponse(int httpCode, TYtError error);

    // Check if response is actually not a error.
    bool IsOk() const;

    void SetRawError(const TString& message);
    void SetError(TYtError error);
    void ParseFromJsonError(const TString& jsonError);

    int GetHttpCode() const;
    TString GetRequestId() const;

    const TYtError& GetError() const;

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
    TYtError Error_;
};

////////////////////////////////////////////////////////////////////////////////

class TOperationFailedError
    : public yexception
{
public:
    enum EState {
        Failed,
        Aborted,
    };

public:
    TOperationFailedError(
        EState state,
        TOperationId id,
        TString error = TString())
        : State(state)
        , Id(id)
        , Error(error)
    { }

    const char* what() const throw() override
    {
        return ~Error;
    }

public:
    EState State;
    TOperationId Id;
    TString Error;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
