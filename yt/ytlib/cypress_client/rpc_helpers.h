#pragma once

#include "public.h"

#include <core/rpc/public.h>
#include <core/rpc/rpc.pb.h>

namespace NYT {
namespace NCypressClient {

////////////////////////////////////////////////////////////////////////////////

//! Extracts transaction id associated with the given request.
TTransactionId GetTransactionId(NRpc::IServiceContextPtr context);

//! Extracts transaction id associated with the given request.
TTransactionId GetTransactionId(const NRpc::NProto::TRequestHeader& header);

//! Attaches transaction id to the request.
void SetTransactionId(NRpc::IClientRequestPtr request, const TTransactionId& transactionId);

//! Attaches transaction id to the request.
void SetTransactionId(NRpc::NProto::TRequestHeader* header, const TTransactionId& transactionId);

//! Sets access tracking suppression flag.
void SetSuppressAccessTracking(NRpc::IClientRequestPtr request, bool value);

//! Sets access tracking suppression flag.
void SetSuppressAccessTracking(NRpc::NProto::TRequestHeader* header, bool value);

//! Gets access tracking suppression flag.
bool GetSuppressAccessTracking(const NRpc::NProto::TRequestHeader& header);

//! Sets modification tracking suppression flag.
void SetSuppressModificationTracking(NRpc::IClientRequestPtr request, bool value);

//! Sets modification tracking suppression flag.
void SetSuppressModificationTracking(NRpc::NProto::TRequestHeader* header, bool value);

//! Gets modification tracking suppression flag.
bool GetSuppressModificationTracking(const NRpc::NProto::TRequestHeader& header);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressClient
} // namespace NYT
