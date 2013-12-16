#pragma once

#include "public.h"

#include <core/rpc/public.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Attaches transaction id to the given request.
/*!
*  #transaction may be null.
*/
void SetTransactionId(NRpc::IClientRequestPtr request, TTransactionPtr transaction);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
