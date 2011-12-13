#pragma once

#include "common.h"

#include <yt/ytlib/actions/action.h>

namespace NYT {
namespace NHTTP {

////////////////////////////////////////////////////////////////////////////////

//! Formats a canonical "Internal Server Error" (500) response.
Stroka FormatInternalServerErrorResponse(const Stroka& body = "");
//! Formats a canonical "Not Implemented" (501) response.
Stroka FormatNotImplementedResponse(const Stroka& body = "");
//! Formats a canonical "Bad Gateway" (502) response.
Stroka FormatBadGatewayResponse(const Stroka& body = "");
//! Formats a canonical "Service Unavailable" (503) response.
Stroka FormatServiceUnavailableResponse(const Stroka& body = "");
//! Formats a canonical "Gateway Timeout" (504) response.
Stroka FormatGatewayTimeoutResponse(const Stroka& body = "");
//! Formats a canonical "Bad Request" (400) response.
Stroka FormatBadRequestResponse(const Stroka& body = "");
//! Formats a canonical "Not Found" (404) response.
Stroka FormatNotFoundResponse(const Stroka& body = "");
//! Formats a canonical "See Other" (303) response.
Stroka FormatRedirectResponse(const Stroka& location);
//! Formats a canonical "OK" (200) response.
Stroka FormatOkResponse(const Stroka& body = "");

////////////////////////////////////////////////////////////////////////////////

//! A simple JSON-HTTP server.
/*
 * This class provides a simple HTTP server which invokes custom handlers 
 * for certain requests. It is assumed that every generated response is a JSON.
 * 
 * You can specify either synchronous or asynchronous
 * handler for a given path prefix.
 *
 * Synchronous handlers have precedence of asynchronous and they are invoked
 * within main server loop. Asynchronous handlers implemented using deferred
 * responses. Asynchronous responses are sent to the client when main loop idles.
 */
class TServer
    : public TNonCopyable
{
public:
    typedef IParamFunc<Stroka, Stroka> TSyncHandler;
    typedef IParamFunc<Stroka, TFuture<Stroka>::TPtr> TAsyncHandler;

public:
    TServer(int port);

    void Register(const Stroka& prefix, TSyncHandler::TPtr handler);
    void Register(const Stroka& prefix, TAsyncHandler::TPtr handler);

    void Start();
    void Stop();

private:
    class TImpl;
    THolder<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHTTP
} // namespace NYT
