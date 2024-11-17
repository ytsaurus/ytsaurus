#include "actions.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/ytlib/sequoia_client/helpers.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>
#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>

#include <yt/yt/ytlib/sequoia_client/records/child_node.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

using namespace NCypressServer::NProto;

using NYT::FromProto;
using NYT::ToProto;

using TYPathBuf = NSequoiaClient::TYPathBuf;

////////////////////////////////////////////////////////////////////////////////

namespace {

void WriteSequoiaNodeRows(
    TNodeId id,
    TAbsoluteYPathBuf path,
    const std::optional<TAbsoluteYPath>& linkTargetPath,
    const ISequoiaTransactionPtr& transaction)
{
    YT_VERIFY(IsLinkType(TypeFromId(id)) == linkTargetPath.has_value());

    transaction->WriteRow(NRecords::TPathToNodeId{
        .Key = {.Path = path.ToMangledSequoiaPath()},
        .NodeId = id,
    });

    auto record = NRecords::TNodeIdToPath{
        .Key = {.NodeId = id},
        .Path = path.ToRawYPath().Underlying(),
        .TargetPath = NYPath::TYPath(linkTargetPath ? linkTargetPath->Underlying() : ""),
    };

    transaction->WriteRow(record);
}

void DeleteSequoiaNodeRows(
    TNodeId id,
    const TMangledSequoiaPath& path,
    const ISequoiaTransactionPtr& transaction)
{
    YT_VERIFY(TypeFromId(id) != EObjectType::Rootstock);

    // Remove from path-to-node-id table.
    transaction->DeleteRow(NRecords::TPathToNodeIdKey{
        .Path = path,
    });

    // Remove from node-id-to-path table.
    transaction->DeleteRow(NRecords::TNodeIdToPathKey{
        .NodeId = id,
    });
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void SetNode(
    TVersionedNodeId nodeId,
    TYPathBuf path,
    const TYsonString& value,
    bool force,
    const TSuppressableAccessTrackingOptions& options,
    const ISequoiaTransactionPtr& transaction)
{
    NCypressServer::NProto::TReqSetNode reqSetNode;
    ToProto(reqSetNode.mutable_node_id(), nodeId.ObjectId);
    reqSetNode.set_path(path.ToRawYPath().Underlying());
    reqSetNode.set_value(value.ToString());
    reqSetNode.set_force(force);
    ToProto(reqSetNode.mutable_transaction_id(), nodeId.TransactionId);
    ToProto(reqSetNode.mutable_access_tracking_options(), options);
    transaction->AddTransactionAction(
        CellTagFromId(nodeId.ObjectId),
        MakeTransactionActionData(reqSetNode));
}

void MultisetNodeAttributes(
    TVersionedNodeId nodeId,
    TYPathBuf path,
    const std::vector<TMultisetAttributesSubrequest>& subrequests,
    bool force,
    const TSuppressableAccessTrackingOptions& options,
    const ISequoiaTransactionPtr& transaction)
{
    NCypressServer::NProto::TReqMultisetAttributes reqMultisetAttributes;
    ToProto(reqMultisetAttributes.mutable_node_id(), nodeId.ObjectId);
    reqMultisetAttributes.set_path(path.ToRawYPath().Underlying());
    ToProto(reqMultisetAttributes.mutable_subrequests(), subrequests);
    reqMultisetAttributes.set_force(force);
    ToProto(reqMultisetAttributes.mutable_transaction_id(), nodeId.TransactionId);
    ToProto(reqMultisetAttributes.mutable_access_tracking_options(), options);
    transaction->AddTransactionAction(
        CellTagFromId(nodeId.ObjectId),
        MakeTransactionActionData(reqMultisetAttributes));
}

void CreateNode(
    TNodeId id,
    TNodeId parentId,
    TAbsoluteYPathBuf path,
    const IAttributeDictionary* explicitAttributes,
    const ISequoiaTransactionPtr& transaction)
{
    if (!explicitAttributes) {
        explicitAttributes = &EmptyAttributes();
    }

    auto type = TypeFromId(id);

    WriteSequoiaNodeRows(
        id,
        path,
        type == EObjectType::SequoiaLink
            ? std::optional(TAbsoluteYPath(explicitAttributes->Get<TString>(
                EInternedAttributeKey::TargetPath.Unintern())))
            : std::nullopt,
        transaction);

    NCypressServer::NProto::TReqCreateNode createNodeRequest;
    createNodeRequest.set_type(ToProto(type));
    ToProto(createNodeRequest.mutable_node_id(), id);
    createNodeRequest.set_path(path.ToRawYPath().Underlying());
    ToProto(createNodeRequest.mutable_node_attributes(), *explicitAttributes);
    ToProto(createNodeRequest.mutable_parent_id(), parentId);
    transaction->AddTransactionAction(CellTagFromId(id), MakeTransactionActionData(createNodeRequest));
}

TNodeId CopyNode(
    const NRecords::TNodeIdToPath& sourceNode,
    TAbsoluteYPathBuf destinationNodePath,
    TNodeId destinationParentId,
    const TCopyOptions& options,
    const ISequoiaTransactionPtr& transaction)
{
    // TypeFromId here might break everything for Cypress->Sequoia copy.
    // Add exception somewhere to not crash all the time.
    // TODO(h0pless): Do that.
    auto sourceNodeId = sourceNode.Key.NodeId;
    auto sourceNodeType = TypeFromId(sourceNodeId);
    YT_VERIFY(
        sourceNodeType != EObjectType::Rootstock &&
        sourceNodeType != EObjectType::Scion);

    auto cellTag = CellTagFromId(sourceNodeId);
    auto destinationNodeId = transaction->GenerateObjectId(sourceNodeType, cellTag);

    WriteSequoiaNodeRows(
        destinationNodeId,
        destinationNodePath,
        sourceNodeType == EObjectType::SequoiaLink
            ? std::optional(TAbsoluteYPath(sourceNode.TargetPath))
            : std::nullopt,
        transaction);


    NCypressServer::NProto::TReqCloneNode cloneNodeRequest;
    ToProto(cloneNodeRequest.mutable_src_id(), sourceNodeId);
    ToProto(cloneNodeRequest.mutable_dst_id(), destinationNodeId);
    cloneNodeRequest.set_dst_path(destinationNodePath.ToRawYPath().Underlying());
    ToProto(cloneNodeRequest.mutable_options(), options);
    ToProto(cloneNodeRequest.mutable_dst_parent_id(), destinationParentId);
    // TODO(h0pless): Add cypress transaction id here.

    transaction->AddTransactionAction(cellTag, MakeTransactionActionData(cloneNodeRequest));

    return destinationNodeId;
}

void RemoveNode(
    TVersionedNodeId nodeId,
    const TMangledSequoiaPath& path,
    const ISequoiaTransactionPtr& transaction)
{
    DeleteSequoiaNodeRows(nodeId.ObjectId, path, transaction);

    NCypressServer::NProto::TReqRemoveNode reqRemoveNode;
    ToProto(reqRemoveNode.mutable_node_id(), nodeId.ObjectId);
    ToProto(reqRemoveNode.mutable_transaction_id(), nodeId.TransactionId);
    transaction->AddTransactionAction(
        CellTagFromId(nodeId.ObjectId),
        MakeTransactionActionData(reqRemoveNode));
}

void RemoveNodeAttribute(
    NCypressClient::TVersionedNodeId nodeId,
    NSequoiaClient::TYPathBuf attributePath,
    bool force,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction)
{
    NCypressServer::NProto::TReqRemoveNodeAttribute reqRemoveNodeAttribute;
    ToProto(reqRemoveNodeAttribute.mutable_node_id(), nodeId.ObjectId);
    reqRemoveNodeAttribute.set_path(attributePath.ToRawYPath().Underlying());
    reqRemoveNodeAttribute.set_force(force);
    ToProto(reqRemoveNodeAttribute.mutable_transaction_id(), nodeId.TransactionId);
    transaction->AddTransactionAction(
        CellTagFromId(nodeId.ObjectId),
        MakeTransactionActionData(reqRemoveNodeAttribute));
}

void AttachChild(
    TNodeId parentId,
    TNodeId childId,
    const std::string& childKey,
    const TSuppressableAccessTrackingOptions& options,
    const ISequoiaTransactionPtr& transaction)
{
    transaction->WriteRow(NRecords::TChildNode{
        .Key = {
            .ParentId = parentId,
            .ChildKey = childKey,
        },
        .ChildId = childId,
    });

    NCypressServer::NProto::TReqAttachChild attachChildRequest;
    ToProto(attachChildRequest.mutable_parent_id(), parentId);
    ToProto(attachChildRequest.mutable_child_id(), childId);
    attachChildRequest.set_key(childKey);
    ToProto(attachChildRequest.mutable_access_tracking_options(), options);
    transaction->AddTransactionAction(CellTagFromId(parentId), MakeTransactionActionData(attachChildRequest));
}

void DetachChild(
    TNodeId parentId,
    const std::string& childKey,
    const TSuppressableAccessTrackingOptions& options,
    const ISequoiaTransactionPtr& transaction)
{
    transaction->DeleteRow(NRecords::TChildNodeKey{
        .ParentId = parentId,
        .ChildKey = childKey,
    });

    NCypressServer::NProto::TReqDetachChild reqDetachChild;
    ToProto(reqDetachChild.mutable_parent_id(), parentId);
    reqDetachChild.set_key(childKey);
    ToProto(reqDetachChild.mutable_access_tracking_options(), options);
    transaction->AddTransactionAction(
        CellTagFromId(parentId),
        MakeTransactionActionData(reqDetachChild));
}

void LockRowInNodeIdToPathTable(
    TNodeId nodeId,
    const ISequoiaTransactionPtr& transaction,
    ELockType lockType)
{
    transaction->LockRow(NRecords::TNodeIdToPathKey{.NodeId = nodeId}, lockType);
}

TLockId LockNodeInMaster(
    TVersionedNodeId nodeId,
    ELockMode lockMode,
    const std::optional<std::string>& childKey,
    const std::optional<std::string>& attributeKey,
    TTimestamp timestamp,
    bool waitable,
    const ISequoiaTransactionPtr& sequoiaTransaction)
{
    YT_VERIFY(nodeId.IsBranched());

    NCypressServer::NProto::TReqLockNode request;
    ToProto(request.mutable_node_id(), nodeId.ObjectId);
    ToProto(request.mutable_transaction_id(), nodeId.TransactionId);
    request.set_mode(ToProto(lockMode));
    if (childKey) {
        request.set_child_key(*childKey);
    }
    if (attributeKey) {
        request.set_attribute_key(*attributeKey);
    }
    request.set_timestamp(timestamp);
    request.set_waitable(waitable);
    auto lockId = sequoiaTransaction->GenerateObjectId(
        EObjectType::Lock,
        CellTagFromId(nodeId.ObjectId));
    ToProto(request.mutable_lock_id(), lockId);

    sequoiaTransaction->AddTransactionAction(
        CellTagFromId(nodeId.ObjectId),
        MakeTransactionActionData(request));

    return lockId;
}

void UnlockNodeInMaster(TVersionedNodeId nodeId, const ISequoiaTransactionPtr& sequoiaTransaction)
{
    YT_VERIFY(nodeId.IsBranched());

    NCypressServer::NProto::TReqUnlockNode request;
    ToProto(request.mutable_node_id(), nodeId.ObjectId);
    ToProto(request.mutable_transaction_id(), nodeId.TransactionId);

    sequoiaTransaction->AddTransactionAction(
        CellTagFromId(nodeId.ObjectId),
        MakeTransactionActionData(request));
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NYTree::NProto::TReqMultisetAttributes::TSubrequest* protoSubrequest,
    const TMultisetAttributesSubrequest& subrequest)
{
    protoSubrequest->set_attribute(ToProto<TProtobufString>(subrequest.Attribute));
    protoSubrequest->set_value(subrequest.Value.ToString());
}

void ToProto(NCypressServer::NProto::TAccessTrackingOptions* protoOptions, const TSuppressableAccessTrackingOptions& options)
{
    protoOptions->set_suppress_access_tracking(options.SuppressAccessTracking);
    protoOptions->set_suppress_modification_tracking(options.SuppressModificationTracking);
    protoOptions->set_suppress_expiration_timeout_renewal(options.SuppressExpirationTimeoutRenewal);
}

void ToProto(TReqCloneNode::TCloneOptions* protoOptions, const TCopyOptions& options)
{
    protoOptions->set_mode(ToProto(options.Mode));
    protoOptions->set_preserve_acl(options.PreserveAcl);
    protoOptions->set_preserve_account(options.PreserveAccount);
    protoOptions->set_preserve_owner(options.PreserveOwner);
    protoOptions->set_preserve_creation_time(options.PreserveCreationTime);
    protoOptions->set_preserve_modification_time(options.PreserveModificationTime);
    protoOptions->set_preserve_expiration_time(options.PreserveExpirationTime);
    protoOptions->set_preserve_expiration_timeout(options.PreserveExpirationTimeout);
    protoOptions->set_pessimistic_quota_check(options.PessimisticQuotaCheck);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
