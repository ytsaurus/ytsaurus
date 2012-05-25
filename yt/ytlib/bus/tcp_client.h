#pragma once

#include "public.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

//! Initializes a new client for communicating with a given address.
/*!
 *  DNS resolution is performed upon construction, the resulting
 *  IP address is cached.
 */
IBusClientPtr CreateTcpBusClient(TTcpBusClientConfigPtr config);

//////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
