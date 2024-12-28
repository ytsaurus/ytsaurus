#include "actions.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/ytlib/sequoia_client/helpers.h>
#include <yt/yt/ytlib/sequoia_client/record_helpers.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>
#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>

#include <yt/yt/ytlib/sequoia_client/records/child_node.record.h>
#include <yt/yt/ytlib/sequoia_client/records/child_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_snapshots.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_forks.record.h>
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
using namespace NServer;

using namespace NCypressServer::NProto;

using NYT::FromProto;
using NYT::ToProto;

using TYPathBuf = NSequoiaClient::TYPathBuf;

////////////////////////////////////////////////////////////////////////////////

namespace {

void WriteSequoiaNodeAndPathRows(
    const ISequoiaTransactionPtr& sequoiaTransaction,
    const TProgenitorTransactionCache& progenitorTransactionCache,
    TVersionedNodeId id,
    TAbsoluteYPathBuf path,
    std::optional<TAbsoluteYPathBuf> linkTargetPath)
{
    YT_VERIFY(IsLinkType(TypeFromId(id.ObjectId)) == linkTargetPath.has_value());

    sequoiaTransaction->WriteRow(NRecords::TPathToNodeId{
        .Key = {.Path = path.ToMangledSequoiaPath(), .TransactionId = id.TransactionId},
        .NodeId = id.ObjectId,
    });

    sequoiaTransaction->WriteRow(NRecords::TNodeIdToPath{
        .Key = {.NodeId = id.ObjectId, .TransactionId = id.TransactionId},
        .Path = path.ToRawYPath().Underlying(),
        .TargetPath = NYPath::TYPath(linkTargetPath ? linkTargetPath->Underlying() : ""),
        .ForkKind = EForkKind::Regular,
    });

    if (id.IsBranched()) {
        // Node is just created so the progenitor tx is the current Cypress tx.
        sequoiaTransaction->WriteRow(NRecords::TNodeFork{
            .Key = {.TransactionId = id.TransactionId, .NodeId = id.ObjectId},
            .Path = path.ToRawYPath().Underlying(),
            .TargetPath = NYPath::TYPath(linkTargetPath ? linkTargetPath->Underlying() : ""),
            .ProgenitorTransactionId = id.TransactionId,
        });

        // Some node with the same path can exist under ancestor transaction so we
        // have to get cached progenitor transaction ID.
        sequoiaTransaction->WriteRow(NRecords::TPathFork{
            .Key = {.TransactionId = id.TransactionId, .Path = path.ToMangledSequoiaPath()},
            .NodeId = id.ObjectId,
            .ProgenitorTransactionId = GetOrDefault(
                progenitorTransactionCache.Path,
                path,
                id.TransactionId),
        });
    }
}

void DeleteSequoiaNodeRows(
    const ISequoiaTransactionPtr& sequoiaTransaction,
    TVersionedNodeId nodeId)
{
    sequoiaTransaction->DeleteRow(NRecords::TNodeIdToPathKey{
        .NodeId = nodeId.ObjectId,
        .TransactionId = nodeId.TransactionId,
    });

    if (nodeId.IsBranched()) {
        sequoiaTransaction->DeleteRow(NRecords::TNodeForkKey{
            .TransactionId = nodeId.TransactionId,
            .NodeId = nodeId.ObjectId,
        });
    }
}

void WriteSequoiaNodeTombstoneRows(
    const ISequoiaTransactionPtr& sequoiaTransaction,
    TVersionedNodeId nodeId,
    TAbsoluteYPathBuf path,
    TTransactionId progenitorTransactionId)
{
    YT_VERIFY(nodeId.IsBranched());
    // If we decided to write tombstone we've already seen that node exists in
    // some ancestor transaction.
    YT_VERIFY(nodeId.TransactionId != progenitorTransactionId);

    sequoiaTransaction->WriteRow(NRecords::TNodeIdToPath{
        .Key = {.NodeId = nodeId.ObjectId, .TransactionId = nodeId.TransactionId},
        .Path = path.ToRawYPath().Underlying(),
        .TargetPath = LinkTargetPathTombstone,
        .ForkKind = EForkKind::Tombstone,
    });

    sequoiaTransaction->WriteRow(NRecords::TNodeFork{
        .Key = {.TransactionId = nodeId.TransactionId, .NodeId = nodeId.ObjectId},
        .Path = path.ToRawYPath().Underlying(),
        .TargetPath = LinkTargetPathTombstone,
        .ProgenitorTransactionId = progenitorTransactionId,
    });
}

void WriteSequoiaPathTombstoneRows(
    const ISequoiaTransactionPtr& sequoiaTransaction,
    const TMangledSequoiaPath path,
    TTransactionId cypressTransactionId,
    TTransactionId progenitorTransactionId)
{
    YT_VERIFY(cypressTransactionId);
    // See the similar comment in WriteSequoiaNodeTombstoneRows().
    YT_VERIFY(cypressTransactionId != progenitorTransactionId);

    sequoiaTransaction->WriteRow(NRecords::TPathToNodeId{
        .Key = {.Path = path, .TransactionId = cypressTransactionId},
        .NodeId = NodeTombstoneId,
    });

    sequoiaTransaction->WriteRow(NRecords::TPathFork{
        .Key = {.TransactionId = cypressTransactionId, .Path = path},
        .NodeId = NodeTombstoneId,
        .ProgenitorTransactionId = progenitorTransactionId,
    });
}

void DeleteSequoiaPathRows(
    const ISequoiaTransactionPtr& sequoiaTransaction,
    const TMangledSequoiaPath& path,
    TTransactionId cypressTransactionId)
{
    sequoiaTransaction->DeleteRow(NRecords::TPathToNodeIdKey{
        .Path = path,
        .TransactionId = cypressTransactionId,
    });

    if (cypressTransactionId) {
        sequoiaTransaction->DeleteRow(NRecords::TPathForkKey{
            .TransactionId = cypressTransactionId,
            .Path = path,
        });
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void SetNode(
    TVersionedNodeId nodeId,
    TYPathBuf path,
    const TYsonString& value,
    bool force,
    const TSuppressableAccessTrackingOptions& options,
    const ISequoiaTransactionPtr& sequoiaTransaction)
{
    NCypressServer::NProto::TReqSetNode reqSetNode;
    ToProto(reqSetNode.mutable_node_id(), nodeId.ObjectId);
    reqSetNode.set_path(path.ToRawYPath().Underlying());
    reqSetNode.set_value(value.ToString());
    reqSetNode.set_force(force);
    ToProto(reqSetNode.mutable_transaction_id(), nodeId.TransactionId);
    ToProto(reqSetNode.mutable_access_tracking_options(), options);
    sequoiaTransaction->AddTransactionAction(
        CellTagFromId(nodeId.ObjectId),
        MakeTransactionActionData(reqSetNode));
}

void MultisetNodeAttributes(
    TVersionedNodeId nodeId,
    TYPathBuf path,
    const std::vector<TMultisetAttributesSubrequest>& subrequests,
    bool force,
    const TSuppressableAccessTrackingOptions& options,
    const ISequoiaTransactionPtr& sequoiaTransaction)
{
    NCypressServer::NProto::TReqMultisetAttributes reqMultisetAttributes;
    ToProto(reqMultisetAttributes.mutable_node_id(), nodeId.ObjectId);
    reqMultisetAttributes.set_path(path.ToRawYPath().Underlying());
    ToProto(reqMultisetAttributes.mutable_subrequests(), subrequests);
    reqMultisetAttributes.set_force(force);
    ToProto(reqMultisetAttributes.mutable_transaction_id(), nodeId.TransactionId);
    ToProto(reqMultisetAttributes.mutable_access_tracking_options(), options);
    sequoiaTransaction->AddTransactionAction(
        CellTagFromId(nodeId.ObjectId),
        MakeTransactionActionData(reqMultisetAttributes));
}

void CreateNode(
    TVersionedNodeId nodeId,
    TNodeId parentId,
    TAbsoluteYPathBuf path,
    const IAttributeDictionary* explicitAttributes,
    const TProgenitorTransactionCache& progenitorTransactionCache,
    const ISequoiaTransactionPtr& sequoiaTransaction)
{
    if (!explicitAttributes) {
        explicitAttributes = &EmptyAttributes();
    }

    auto type = TypeFromId(nodeId.ObjectId);

    WriteSequoiaNodeAndPathRows(
        sequoiaTransaction,
        progenitorTransactionCache,
        nodeId,
        path,
        type == EObjectType::SequoiaLink
            ? std::optional(TAbsoluteYPath(explicitAttributes->Get<TString>(
                EInternedAttributeKey::TargetPath.Unintern())))
            : std::nullopt);

    NCypressServer::NProto::TReqCreateNode createNodeRequest;
    createNodeRequest.set_type(ToProto<int>(type));
    ToProto(createNodeRequest.mutable_node_id(), nodeId.ObjectId);
    createNodeRequest.set_path(path.ToRawYPath().Underlying());
    ToProto(createNodeRequest.mutable_node_attributes(), *explicitAttributes);
    ToProto(createNodeRequest.mutable_transaction_id(), nodeId.TransactionId);
    ToProto(createNodeRequest.mutable_parent_id(), parentId);
    sequoiaTransaction->AddTransactionAction(
        CellTagFromId(nodeId.ObjectId),
        MakeTransactionActionData(createNodeRequest));
}

TNodeId CopyNode(
    const NRecords::TNodeIdToPath& sourceNode,
    TAbsoluteYPathBuf destinationNodePath,
    TNodeId destinationParentId,
    TTransactionId cypressTransactionId,
    const TCopyOptions& options,
    const TProgenitorTransactionCache& progenitorTransactionCache,
    const ISequoiaTransactionPtr& sequoiaTransaction)
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
    auto destinationNodeId = sequoiaTransaction->GenerateObjectId(sourceNodeType, cellTag);

    WriteSequoiaNodeAndPathRows(
        sequoiaTransaction,
        progenitorTransactionCache,
        {destinationNodeId, cypressTransactionId},
        destinationNodePath,
        sourceNodeType == EObjectType::SequoiaLink
            ? std::optional(TAbsoluteYPath(sourceNode.TargetPath))
            : std::nullopt);

    NCypressServer::NProto::TReqCloneNode cloneNodeRequest;
    ToProto(cloneNodeRequest.mutable_src_id(), sourceNodeId);
    ToProto(cloneNodeRequest.mutable_dst_id(), destinationNodeId);
    cloneNodeRequest.set_dst_path(destinationNodePath.ToRawYPath().Underlying());
    ToProto(cloneNodeRequest.mutable_options(), options);
    ToProto(cloneNodeRequest.mutable_dst_parent_id(), destinationParentId);
    ToProto(cloneNodeRequest.mutable_transaction_id(), cypressTransactionId);

    sequoiaTransaction->AddTransactionAction(cellTag, MakeTransactionActionData(cloneNodeRequest));

    return destinationNodeId;
}

void RemoveNode(
    TVersionedNodeId nodeId,
    TAbsoluteYPathBuf path,
    const TProgenitorTransactionCache& progenitorTransactionCache,
    const ISequoiaTransactionPtr& sequoiaTransaction)
{
    YT_VERIFY(TypeFromId(nodeId.ObjectId) != EObjectType::Rootstock);

    NCypressServer::NProto::TReqRemoveNode reqRemoveNode;
    ToProto(reqRemoveNode.mutable_node_id(), nodeId.ObjectId);
    ToProto(reqRemoveNode.mutable_transaction_id(), nodeId.TransactionId);
    sequoiaTransaction->AddTransactionAction(
        CellTagFromId(nodeId.ObjectId),
        MakeTransactionActionData(reqRemoveNode));

    auto nodeProgenitorTransactionId = GetOrCrash(progenitorTransactionCache.Node, nodeId.ObjectId);
    if (!nodeId.IsBranched() || nodeProgenitorTransactionId == nodeId.TransactionId) {
        DeleteSequoiaNodeRows(sequoiaTransaction, nodeId);
    } else {
        WriteSequoiaNodeTombstoneRows(
            sequoiaTransaction,
            nodeId,
            path,
            nodeProgenitorTransactionId);
    }

    auto pathProgenitorTransactionId = GetOrCrash(progenitorTransactionCache.Path, path);
    if (!nodeId.IsBranched() || pathProgenitorTransactionId == nodeId.TransactionId) {
        DeleteSequoiaPathRows(sequoiaTransaction, path.ToMangledSequoiaPath(), nodeId.TransactionId);
    } else {
        WriteSequoiaPathTombstoneRows(
            sequoiaTransaction,
            path.ToMangledSequoiaPath(),
            nodeId.TransactionId,
            pathProgenitorTransactionId);
    }
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
    TVersionedNodeId parentId,
    TNodeId childId,
    const std::string& childKey,
    const TSuppressableAccessTrackingOptions& options,
    const TProgenitorTransactionCache& progenitorTransactionCache,
    const ISequoiaTransactionPtr& sequoiaTransaction)
{
    sequoiaTransaction->WriteRow(NRecords::TChildNode{
        .Key = {
            .ParentId = parentId.ObjectId,
            .TransactionId = parentId.TransactionId,
            .ChildKey = childKey,
        },
        .ChildId = childId,
    });

    if (parentId.IsBranched()) {
        sequoiaTransaction->WriteRow(NRecords::TChildFork{
            .Key = {
                .TransactionId = parentId.TransactionId,
                .ParentId = parentId.ObjectId,
                .ChildKey = childKey,
            },
            .ChildId = childId,
            .ProgenitorTransactionId = GetOrDefault(
                progenitorTransactionCache.Child,
                std::pair(parentId.ObjectId, childKey),
                parentId.TransactionId),
        });
    }

    NCypressServer::NProto::TReqAttachChild attachChildRequest;
    ToProto(attachChildRequest.mutable_parent_id(), parentId.ObjectId);
    ToProto(attachChildRequest.mutable_child_id(), childId);
    attachChildRequest.set_key(childKey);
    ToProto(attachChildRequest.mutable_access_tracking_options(), options);
    ToProto(attachChildRequest.mutable_transaction_id(), parentId.TransactionId);
    sequoiaTransaction->AddTransactionAction(
        CellTagFromId(parentId.ObjectId),
        MakeTransactionActionData(attachChildRequest));
}

void DetachChild(
    TVersionedNodeId parentId,
    const std::string& childKey,
    const TSuppressableAccessTrackingOptions& options,
    const TProgenitorTransactionCache& progenitorTransactionCache,
    const ISequoiaTransactionPtr& transaction)
{
    auto progenitorTransactionId = GetOrCrash(
        progenitorTransactionCache.Child,
        std::pair(parentId.ObjectId, childKey));
    if (!parentId.IsBranched() || progenitorTransactionId == parentId.TransactionId) {
        transaction->DeleteRow(NRecords::TChildNodeKey{
            .ParentId = parentId.ObjectId,
            .TransactionId = parentId.TransactionId,
            .ChildKey = TString(childKey), // TODO(babenko): migrate to std::string.
        });

        if (parentId.IsBranched()) {
            transaction->DeleteRow(NRecords::TChildForkKey{
                .TransactionId = parentId.TransactionId,
                .ParentId = parentId.ObjectId,
                .ChildKey = childKey,
            });
        }
    } else {
        transaction->WriteRow(NRecords::TChildNode{
            .Key = {
                .ParentId = parentId.ObjectId,
                .TransactionId = parentId.TransactionId,
                .ChildKey = childKey,
            },
            .ChildId = ChildNodeTombstoneId,
        });

        transaction->WriteRow(NRecords::TChildFork{
            .Key = {
                .TransactionId = parentId.TransactionId,
                .ParentId = parentId.ObjectId,
                .ChildKey = childKey,
            },
            .ChildId = ChildNodeTombstoneId,
            .ProgenitorTransactionId = progenitorTransactionId,
        });
    }

    NCypressServer::NProto::TReqDetachChild reqDetachChild;
    ToProto(reqDetachChild.mutable_parent_id(), parentId.ObjectId);
    reqDetachChild.set_key(childKey);
    ToProto(reqDetachChild.mutable_access_tracking_options(), options);
    ToProto(reqDetachChild.mutable_transaction_id(), parentId.TransactionId);
    transaction->AddTransactionAction(
        CellTagFromId(parentId.ObjectId),
        MakeTransactionActionData(reqDetachChild));
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

void CreateSnapshotLockInSequoia(
    TVersionedNodeId nodeId,
    TAbsoluteYPathBuf path,
    std::optional<TAbsoluteYPathBuf> targetPath,
    const ISequoiaTransactionPtr& sequoiaTransaction)
{
    YT_VERIFY(nodeId.IsBranched());

    sequoiaTransaction->WriteRow(NRecords::TNodeIdToPath{
        .Key = {.NodeId = nodeId.ObjectId, .TransactionId = nodeId.TransactionId},
        .Path = path.ToRawYPath().Underlying(),
        .TargetPath = NYPath::TYPath(targetPath.has_value() ? targetPath->Underlying() : ""),
        .ForkKind = EForkKind::Snapshot,
    });

    sequoiaTransaction->WriteRow(NRecords::TNodeSnapshot{
        .Key = {.TransactionId = nodeId.TransactionId, .NodeId = nodeId.ObjectId},
        .Dummy = 0,
    });
}

void RemoveSnapshotLockFromSequoia(
    TVersionedNodeId nodeId,
    const ISequoiaTransactionPtr& sequoiaTransaction)
{
    YT_VERIFY(nodeId.IsBranched());

    sequoiaTransaction->DeleteRow(NRecords::TNodeIdToPathKey{
        .NodeId = nodeId.ObjectId,
        .TransactionId = nodeId.TransactionId,
    });

    sequoiaTransaction->DeleteRow(NRecords::TNodeSnapshotKey{
        .TransactionId = nodeId.TransactionId,
        .NodeId = nodeId.ObjectId,
    });
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NYTree::NProto::TReqMultisetAttributes::TSubrequest* protoSubrequest,
    const TMultisetAttributesSubrequest& subrequest)
{
    protoSubrequest->set_attribute(ToProto(subrequest.AttributeKey));
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
