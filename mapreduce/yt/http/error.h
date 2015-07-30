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

    void ParseFromJsonError(const Stroka& jsonError);
    int GetHttpCode() const;
    Stroka GetRequestId() const;

    bool IsRetriable() const;
    TDuration GetRetryInterval() const;

private:
    int HttpCode_;
    Stroka RequestId_;
    TError Error_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
