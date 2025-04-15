#pragma once

#include <yt/yt/server/lib/sequoia/proto/transaction_manager.pb.h>

#include <yt/yt/ytlib/cypress_transaction_client/proto/cypress_transaction_service.pb.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/rpc/authentication_identity.h>

#include <library/cpp/yt/memory/range.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

NProto::TReqStartCypressTransaction BuildStartCypressTransactionRequest(
    NCypressTransactionClient::NProto::TReqStartTransaction rpcRequest,
    const NRpc::TAuthenticationIdentity& authenticationIdentity);

NProto::TReqCommitCypressTransaction BuildCommitCypressTransactionRequest(
    NTransactionClient::TTransactionId transactionId,
    NTransactionClient::TTimestamp commitTimestamp,
    TRange<NTransactionClient::TTransactionId> prerequisiteTransactionIds,
    const NRpc::TAuthenticationIdentity& authenticationIdentity);

NProto::TReqAbortCypressTransaction BuildAbortCypressTransactionRequest(
    NTransactionClient::TTransactionId transactionId,
    bool force,
    const NRpc::TAuthenticationIdentity& authenticationIdentity);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////



////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
