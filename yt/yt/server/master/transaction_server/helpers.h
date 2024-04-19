#pragma once

#include "public.h"

// TODO(kvk1920): consider using forward declaration.
#include <yt/yt/server/master/transaction_server/proto/transaction_manager.pb.h>

#include <yt/yt/server/master/object_server/public.h>

// TODO(kvk1920): consider using forward declaration.
#include <yt/yt/ytlib/cypress_transaction_client/proto/cypress_transaction_service.pb.h>

#include <yt/yt/core/rpc/authentication_identity.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

NProto::TReqStartCypressTransaction BuildStartCypressTransactionRequest(
    NCypressTransactionClient::NProto::TReqStartTransaction rpcRequest,
    const NRpc::TAuthenticationIdentity& authenticationIdentity);

NProto::TReqCommitCypressTransaction BuildCommitCypressTransactionRequest(
    TTransactionId transactionId,
    TTimestamp commitTimestamp,
    TRange<TTransactionId> prerequisiteTransactionIds,
    const NRpc::TAuthenticationIdentity& authenticationIdentity);

NProto::TReqAbortCypressTransaction BuildAbortCypressTransactionRequest(
    TTransactionId transactionId,
    bool force,
    bool replicateViaHive,
    const NRpc::TAuthenticationIdentity& authenticationIdentity);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
