#include "protobuf_helpers.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/helpers.h>

namespace NYT::NSequoiaServer {

using namespace NCypressClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

#define COPY_PRIMITIVE_FIELD(dst, src, field) (dst).set_##field((src).field())

#define COPY_FIELD(dst, src, field) (dst).mutable_##field()->CopyFrom((src).field())

#define MAYBE_ASSIGN_FIELD_IMPL(dst, src, field, do_assign) \
    do { \
        if ((src).has_##field()) {\
            do_assign(dst, src, field); \
        } \
    } while (false)

#define MAYBE_COPY_PRIMITIVE_FIELD(dst, src, field) \
    MAYBE_ASSIGN_FIELD_IMPL(dst, src, field, COPY_PRIMITIVE_FIELD)

#define MAYBE_COPY_FIELD(dst, src, field) MAYBE_ASSIGN_FIELD_IMPL(dst, src, field, COPY_FIELD)

#define MOVE_FIELD(dst, src, field) \
    (dst).mutable_##field()->Swap((src).mutable_##field()); \
    (src).clear_##field()

#define MAYBE_MOVE_FIELD(dst, src, field) MAYBE_ASSIGN_FIELD_IMPL(dst, src, field, MOVE_FIELD)


NCypressServer::NProto::TReqMaterializeNode BuildMaterializeNodeRequest(
    TVersionedNodeId nodeId,
    NObjectClient::NProto::TReqMaterializeNode* originalRequest)
{
    NCypressServer::NProto::TReqMaterializeNode request;

    COPY_FIELD(request, *originalRequest, serialized_node);
    COPY_PRIMITIVE_FIELD(request, *originalRequest, mode);
    COPY_PRIMITIVE_FIELD(request, *originalRequest, version);
    MAYBE_COPY_FIELD(request, *originalRequest, existing_node_id);
    MAYBE_COPY_FIELD(request, *originalRequest, inherited_attributes_override);
    MAYBE_COPY_FIELD(request, *originalRequest, new_account_id);
    COPY_PRIMITIVE_FIELD(request, *originalRequest, preserve_creation_time);
    COPY_PRIMITIVE_FIELD(request, *originalRequest, preserve_expiration_time);
    COPY_PRIMITIVE_FIELD(request, *originalRequest, preserve_expiration_timeout);
    COPY_PRIMITIVE_FIELD(request, *originalRequest, preserve_owner);

    ToProto(request.mutable_node_id(), nodeId.ObjectId);
    ToProto(request.mutable_transaction_id(), nodeId.TransactionId);

    return request;
}

NTransactionServer::NProto::TReqStartCypressTransaction BuildStartCypressTransactionRequest(
    NCypressTransactionClient::NProto::TReqStartTransaction rpcRequest,
    const NRpc::TAuthenticationIdentity& authenticationIdentity)
{
    NTransactionServer::NProto::TReqStartCypressTransaction request;
    COPY_PRIMITIVE_FIELD(request, rpcRequest, timeout);
    MAYBE_COPY_PRIMITIVE_FIELD(request, rpcRequest, deadline);
    MAYBE_MOVE_FIELD(request, rpcRequest, attributes);
    MAYBE_COPY_PRIMITIVE_FIELD(request, rpcRequest, title);
    MAYBE_MOVE_FIELD(request, rpcRequest, parent_id);
    MOVE_FIELD(request, rpcRequest, prerequisite_transaction_ids);
    MOVE_FIELD(request, rpcRequest, replicate_to_cell_tags);
    WriteAuthenticationIdentityToProto(&request, authenticationIdentity);
    return request;
}

#undef COPY_PRIMITIVE_FIELD
#undef COPY_FIELD
#undef MAYBE_COPY_FIELD_IMPL
#undef MAYBE_COPY_PRIMITIVE_FIELD
#undef MAYBE_COPY_FIELD

NTransactionServer::NProto::TReqCommitCypressTransaction BuildCommitCypressTransactionRequest(
    TTransactionId transactionId,
    TTimestamp commitTimestamp,
    TRange<TTransactionId> prerequisiteTransactionIds,
    const NRpc::TAuthenticationIdentity& authenticationIdentity)
{
    NTransactionServer::NProto::TReqCommitCypressTransaction request;
    ToProto(request.mutable_transaction_id(), transactionId);
    request.set_commit_timestamp(commitTimestamp);
    ToProto(request.mutable_prerequisite_transaction_ids(), prerequisiteTransactionIds);
    WriteAuthenticationIdentityToProto(&request, authenticationIdentity);
    return request;
}

NTransactionServer::NProto::TReqAbortCypressTransaction BuildAbortCypressTransactionRequest(
    TTransactionId transactionId,
    bool force,
    const NRpc::TAuthenticationIdentity& authenticationIdentity)
{
    NTransactionServer::NProto::TReqAbortCypressTransaction request;
    ToProto(request.mutable_transaction_id(), transactionId);
    request.set_force(force);
    WriteAuthenticationIdentityToProto(&request, authenticationIdentity);
    return request;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
