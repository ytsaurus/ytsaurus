#pragma once

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

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NRpc
} // namespace NYT
