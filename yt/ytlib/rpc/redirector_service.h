#pragma once

#include "service.h"

#include <ytlib/misc/error.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TRedirectorService
    : public IService
{
protected:
    TRedirectorService(
        const Stroka& serviceName,
        const Stroka& loggingCategory);

    struct TRedirectParams
    {
        Stroka Address;
        TNullable<TDuration> Timeout;
    };

    typedef TValueOrError<TRedirectParams> TRedirectResult;
    typedef TFuture<TRedirectResult>::TPtr TAsyncRedirectResult;

    virtual TAsyncRedirectResult HandleRedirect(IServiceContext* context) = 0;

private:
    class TRequest;
    class TResponseHandler;

    Stroka ServiceName;
    Stroka LoggingCategory;

    virtual void OnBeginRequest(IServiceContext* context);
    virtual void OnEndRequest(IServiceContext* context);

    virtual Stroka GetLoggingCategory() const;
    virtual Stroka GetServiceName() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
