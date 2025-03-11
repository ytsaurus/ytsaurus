#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

namespace NYT::NCypressClient {

////////////////////////////////////////////////////////////////////////////////

//! Extracts transaction id associated with the given request.
TTransactionId GetTransactionId(const NRpc::IServiceContextPtr& context);

//! Extracts transaction id associated with the given request.
TTransactionId GetTransactionId(const NRpc::NProto::TRequestHeader& header);

//! Attaches transaction id to the request.
void SetTransactionId(const NRpc::IClientRequestPtr& request, TTransactionId transactionId);

//! Attaches transaction id to the request.
void SetTransactionId(NRpc::NProto::TRequestHeader* header, TTransactionId transactionId);

//! Sets access tracking suppression flag.
void SetSuppressAccessTracking(const NRpc::IClientRequestPtr& request, bool value);

//! Sets access tracking suppression flag.
void SetSuppressAccessTracking(NRpc::NProto::TRequestHeader* header, bool value);

//! Gets access tracking suppression flag.
bool GetSuppressAccessTracking(const NRpc::NProto::TRequestHeader& header);

//! Sets modification tracking suppression flag.
void SetSuppressModificationTracking(const NRpc::IClientRequestPtr& request, bool value);

//! Sets modification tracking suppression flag.
void SetSuppressModificationTracking(NRpc::NProto::TRequestHeader* header, bool value);

//! Gets modification tracking suppression flag.
bool GetSuppressModificationTracking(const NRpc::NProto::TRequestHeader& header);

//! Sets expiration timeout renewal suppression flag.
void SetSuppressExpirationTimeoutRenewal(const NRpc::IClientRequestPtr& request, bool value);

//! Sets expiration timeout renewal suppression flag.
void SetSuppressExpirationTimeoutRenewal(NRpc::NProto::TRequestHeader* header, bool value);

//! Sets expiration timeout renewal suppression flag.
bool GetSuppressExpirationTimeoutRenewal(const NRpc::NProto::TRequestHeader& header);

//! Sets allow resolve from Sequoia object flag.
void SetAllowResolveFromSequoiaObject(NRpc::NProto::TRequestHeader* header, bool value);

//! Sets allow resolve from Sequoia object flag.
void SetAllowResolveFromSequoiaObject(const NRpc::IClientRequestPtr& request, bool value);

//! Gets allow resolve from Sequoia object flag.
bool GetAllowResolveFromSequoiaObject(const NRpc::NProto::TRequestHeader& header);

//! Sets resolve depth value.
void SetResolveDepth(NRpc::NProto::TRequestHeader* header, int value);

//! Sets resolve depth value.
void SetResolveDepth(const NRpc::IClientRequestPtr& request, int value);

//! Gets resolve depth value.
int GetResolveDepth(const NRpc::NProto::TRequestHeader& header);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressClient
