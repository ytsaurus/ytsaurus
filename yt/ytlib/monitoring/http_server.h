#pragma once

#include "public.h"

#include <yt/core/actions/public.h>

namespace NYT {
namespace NHttp {

////////////////////////////////////////////////////////////////////////////////

const char* const DefaultContentType = "application/json";

//! Formats a canonical "Internal Server Error" (500) response.
TString FormatInternalServerErrorResponse(const TString& body = "",
    const TString& type = DefaultContentType);
//! Formats a canonical "Not Implemented" (501) response.
TString FormatNotImplementedResponse(const TString& body = "",
    const TString& type = DefaultContentType);
///! Formats a canonical "Bad Gateway" (502) response.
TString FormatBadGatewayResponse(const TString& body = "",
    const TString& type = DefaultContentType);
//! Formats a canonical "Service Unavailable" (503) response.
TString FormatServiceUnavailableResponse(const TString& body = "",
    const TString& type = DefaultContentType);
//! Formats a canonical "Gateway Timeout" (504) response.
TString FormatGatewayTimeoutResponse(const TString& body = "",
    const TString& type = DefaultContentType);
//! Formats a canonical "Bad Request" (400) response.
TString FormatBadRequestResponse(const TString& body = "",
    const TString& type = DefaultContentType);
//! Formats a canonical "Not Found" (404) response.
TString FormatNotFoundResponse(const TString& body = "",
    const TString& type = DefaultContentType);
//! Formats a canonical "See Other" (303) response.
TString FormatRedirectResponse(const TString& location);
//! Formats a canonical "OK" (200) response.
TString FormatOKResponse(const TString& body = "",
    const TString& type = DefaultContentType);

////////////////////////////////////////////////////////////////////////////////

//! A simple JSON-HTTP server.
/*
 * This class provides a simple HTTP server that invokes custom handlers
 * for certain requests. It is assumed that every generated response is a JSON.
 *
 * You can specify either a synchronous or an asynchronous
 * handler for a given path prefix.
 *
 * Synchronous handlers have precedence over asynchronous ones and they are invoked
 * within the main server loop. Asynchronous handlers are implemented using deferred
 * responses. Asynchronous responses are sent to the client when the main loop becomes idle.
 */
class TServer
    : public TNonCopyable
{
public:
    typedef TCallback<TString(const TString&)> TSyncHandler;
    typedef TCallback<TFuture<TString>(const TString&)> TAsyncHandler;

public:
    explicit TServer(int port);
    explicit TServer(int port, int bindRetryCount, TDuration bindRetryBackoff);
    ~TServer();

    void Register(const TString& prefix, TSyncHandler handler);
    void Register(const TString& prefix, TAsyncHandler handler);

    void Start();
    void Stop();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NYT
