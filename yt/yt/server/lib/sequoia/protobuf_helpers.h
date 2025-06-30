#pragma once

#include <yt/yt/server/lib/sequoia/proto/transaction_manager.pb.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/ytlib/cypress_transaction_client/proto/cypress_transaction_service.pb.h>

#include <yt/yt/ytlib/object_client/proto/master_ypath.pb.h>

#include <yt/yt/client/cypress_client/public.h>
#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/rpc/authentication_identity.h>

#include <library/cpp/yt/memory/range.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////
NCypressServer::NProto::TReqMaterializeNode BuildMaterializeNodeRequest(
    NCypressClient::TVersionedNodeId nodeId,
    NObjectClient::NProto::TReqMaterializeNode* originalRequest);

NTransactionServer::NProto::TReqStartCypressTransaction BuildStartCypressTransactionRequest(
    NCypressTransactionClient::NProto::TReqStartTransaction rpcRequest,
    const NRpc::TAuthenticationIdentity& authenticationIdentity);

NTransactionServer::NProto::TReqCommitCypressTransaction BuildCommitCypressTransactionRequest(
    NTransactionClient::TTransactionId transactionId,
    NTransactionClient::TTimestamp commitTimestamp,
    TRange<NTransactionClient::TTransactionId> prerequisiteTransactionIds,
    const NRpc::TAuthenticationIdentity& authenticationIdentity);

NTransactionServer::NProto::TReqAbortCypressTransaction BuildAbortCypressTransactionRequest(
    NTransactionClient::TTransactionId transactionId,
    bool force,
    const NRpc::TAuthenticationIdentity& authenticationIdentity);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
