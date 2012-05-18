#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/logging/log.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger RpcLogger;

////////////////////////////////////////////////////////////////////////////////

struct IClientRequest;
typedef TIntrusivePtr<IClientRequest> IClientRequestPtr;

class TClientRequest;

struct IClientResponseHandler;
typedef TIntrusivePtr<IClientResponseHandler> IClientResponseHandlerPtr;

template <class TRequestMessage, class TResponse>
class TTypedClientRequest;

class TClientResponse;

template <class TResponseMessage>
class TTypedClientResponse;

class TOneWayClientResponse;
typedef TIntrusivePtr<TOneWayClientResponse> TOneWayClientResponsePtr;

struct TRetryConfig;
typedef TIntrusivePtr<TRetryConfig> TRetryConfigPtr;

class TRetriableChannel;
typedef TIntrusivePtr<TRetriableChannel> TRetriableChannelPtr;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NRpc
} // namespace NYT
