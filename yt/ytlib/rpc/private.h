#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IClientRequest;
class TClientRequest;

struct IClientResponseHandler;

template <class TRequestMessage, class TResponse>
class TTypedClientRequest;

class TClientResponse;

template <class TResponseMessage>
class TTypedClientResponse;

class TOneWayClientResponse;

struct TRetryConfig;
typedef TIntrusivePtr<TRetryConfig> TRetryConfigPtr;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NRpc
} // namespace NYT
