#include "actions.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/lib/sequoia/protobuf_helpers.h>

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

#include <yt/yt/core/yson/protobuf_helpers.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NSequoiaServer;
using namespace NServer;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

using namespace NCypressServer::NProto;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

void WriteSequoiaNodeAndPathRows(
    const ISequoiaTransactionPtr& sequoiaTransaction,
    const TProgenitorTransactionCache& progenitorTransactionCache,
    TVersionedNodeId id,
    TAbsolutePathBuf path,
    std::optional<TYPathBuf> linkTargetPath)
{
    YT_VERIFY(IsLinkType(TypeFromId(id.ObjectId)) == linkTargetPath.has_value());

    sequoiaTransaction->WriteRow(NRecords::TPathToNodeId{
        .Key = {.Path = path.ToMangledSequoiaPath(), .TransactionId = id.TransactionId},
        .NodeId = id.ObjectId,
    });

    sequoiaTransaction->WriteRow(NRecords::TNodeIdToPath{
        .Key = {.NodeId = id.ObjectId, .TransactionId = id.TransactionId},
        .Path = path.ToRealPath().Underlying(),
        .TargetPath = TYPath(linkTargetPath.value_or("")),
        .ForkKind = EForkKind::Regular,
    });

    if (id.IsBranched()) {
        // Node is just created so the progenitor tx is the current Cypress tx.
        sequoiaTransaction->WriteRow(NRecords::TNodeFork{
            .Key = {.TransactionId = id.TransactionId, .NodeId = id.ObjectId},
            .Path = path.ToRealPath().Underlying(),
            .TargetPath = TYPath(linkTargetPath.value_or("")),
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
    TAbsolutePathBuf path,
    TTransactionId progenitorTransactionId)
{
    YT_VERIFY(nodeId.IsBranched());
    // If we decided to write tombstone we've already seen that node exists in
    // some ancestor transaction.
    YT_VERIFY(nodeId.TransactionId != progenitorTransactionId);

    sequoiaTransaction->WriteRow(NRecords::TNodeIdToPath{
        .Key = {.NodeId = nodeId.ObjectId, .TransactionId = nodeId.TransactionId},
        .Path = path.ToRealPath().Underlying(),
        .TargetPath = LinkTargetPathTombstone,
        .ForkKind = EForkKind::Tombstone,
    });

    sequoiaTransaction->WriteRow(NRecords::TNodeFork{
        .Key = {.TransactionId = nodeId.TransactionId, .NodeId = nodeId.ObjectId},
        .Path = path.ToRealPath().Underlying(),
        .TargetPath = LinkTargetPathTombstone,
        .ProgenitorTransactionId = progenitorTransactionId,
    });
}

void WriteSequoiaPathTombstoneRows(
    const ISequoiaTransactionPtr& sequoiaTransaction,
    TMangledSequoiaPath path,
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
        .Key = {.TransactionId = cypressTransactionId, .Path = std::move(path)},
        .NodeId = NodeTombstoneId,
        .ProgenitorTransactionId = progenitorTransactionId,
    });
}

void DeleteSequoiaPathRows(
    const ISequoiaTransactionPtr& sequoiaTransaction,
    TMangledSequoiaPath path,
    TTransactionId cypressTransactionId)
{
    sequoiaTransaction->DeleteRow(NRecords::TPathToNodeIdKey{
        .Path = path,
        .TransactionId = cypressTransactionId,
    });

    if (cypressTransactionId) {
        sequoiaTransaction->DeleteRow(NRecords::TPathForkKey{
            .TransactionId = cypressTransactionId,
            .Path = std::move(path),
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
    reqSetNode.set_path(path);
    reqSetNode.set_value(ToProto(value));
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
    reqMultisetAttributes.set_path(path);
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
    TAbsolutePathBuf path,
    const IAttributeDictionary* explicitAttributes,
    const TProgenitorTransactionCache& progenitorTransactionCache,
    const ISequoiaTransactionPtr& sequoiaTransaction)
{
    if (!explicitAttributes) {
        explicitAttributes = &EmptyAttributes();
    }

    auto type = TypeFromId(nodeId.ObjectId);

    std::optional<TYPath> linkTargetPath;
    if (type == EObjectType::SequoiaLink) {
        linkTargetPath = ValidateAndMakeYPath(
            explicitAttributes->Get<TRawYPath>(EInternedAttributeKey::TargetPath.Unintern()));
    }

    WriteSequoiaNodeAndPathRows(
        sequoiaTransaction,
        progenitorTransactionCache,
        nodeId,
        path,
        linkTargetPath);

    NCypressServer::NProto::TReqCreateNode reqCreateNode;
    reqCreateNode.set_type(ToProto(type));
    ToProto(reqCreateNode.mutable_node_id(), nodeId.ObjectId);
    reqCreateNode.set_path(path.ToRealPath().Underlying());
    ToProto(reqCreateNode.mutable_node_attributes(), *explicitAttributes);
    ToProto(reqCreateNode.mutable_transaction_id(), nodeId.TransactionId);
    ToProto(reqCreateNode.mutable_parent_id(), parentId);
    sequoiaTransaction->AddTransactionAction(
        CellTagFromId(nodeId.ObjectId),
        MakeTransactionActionData(reqCreateNode));
}

TNodeId CopyNode(
    const NRecords::TNodeIdToPath& sourceNode,
    TAbsolutePathBuf destinationNodePath,
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
            ? std::optional(sourceNode.TargetPath)
            : std::nullopt);

    NCypressServer::NProto::TReqCloneNode reqCloneNode;
    ToProto(reqCloneNode.mutable_src_id(), sourceNodeId);
    ToProto(reqCloneNode.mutable_dst_id(), destinationNodeId);
    reqCloneNode.set_dst_path(destinationNodePath.ToRealPath().Underlying());
    ToProto(reqCloneNode.mutable_options(), options);
    ToProto(reqCloneNode.mutable_dst_parent_id(), destinationParentId);
    ToProto(reqCloneNode.mutable_transaction_id(), cypressTransactionId);

    sequoiaTransaction->AddTransactionAction(cellTag, MakeTransactionActionData(reqCloneNode));

    return destinationNodeId;
}

void MaterializeNodeOnMaster(
    TVersionedNodeId nodeId,
    NObjectClient::NProto::TReqMaterializeNode* originalRequest,
    const ISequoiaTransactionPtr& sequoiaTransaction)
{
    auto request = BuildMaterializeNodeRequest(nodeId, originalRequest);

    sequoiaTransaction->AddTransactionAction(
        CellTagFromId(nodeId.ObjectId),
        MakeTransactionActionData(request));
}

void MaterializeNodeInSequoia(
    TVersionedNodeId nodeId,
    NCypressClient::TNodeId parentId,
    TAbsolutePathBuf path,
    bool preserveAcl,
    bool preserveModificationTime,
    const TProgenitorTransactionCache& progenitorTransactionCache,
    const ISequoiaTransactionPtr& sequoiaTransaction)
{
    WriteSequoiaNodeAndPathRows(
        sequoiaTransaction,
        progenitorTransactionCache,
        nodeId,
        path,
        std::nullopt);

    NCypressServer::NProto::TReqFinishNodeMaterialization reqFinishNodeMaterialization;
    ToProto(reqFinishNodeMaterialization.mutable_node_id(), nodeId.ObjectId);
    ToProto(reqFinishNodeMaterialization.mutable_parent_id(), parentId);
    reqFinishNodeMaterialization.set_path(path.ToRealPath().Underlying());

    reqFinishNodeMaterialization.set_preserve_acl(preserveAcl);
    reqFinishNodeMaterialization.set_preserve_modification_time(preserveModificationTime);

    ToProto(reqFinishNodeMaterialization.mutable_transaction_id(), nodeId.TransactionId);

    sequoiaTransaction->AddTransactionAction(
        CellTagFromId(nodeId.ObjectId),
        MakeTransactionActionData(reqFinishNodeMaterialization));
}

void RemoveNode(
    TVersionedNodeId nodeId,
    TAbsolutePathBuf path,
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
    NYPath::TYPathBuf attributePath,
    bool force,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction)
{
    NCypressServer::NProto::TReqRemoveNodeAttribute reqRemoveNodeAttribute;
    ToProto(reqRemoveNodeAttribute.mutable_node_id(), nodeId.ObjectId);
    reqRemoveNodeAttribute.set_path(attributePath);
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

    NCypressServer::NProto::TReqAttachChild reqAttachChild;
    ToProto(reqAttachChild.mutable_parent_id(), parentId.ObjectId);
    ToProto(reqAttachChild.mutable_child_id(), childId);
    reqAttachChild.set_key(childKey);
    ToProto(reqAttachChild.mutable_access_tracking_options(), options);
    ToProto(reqAttachChild.mutable_transaction_id(), parentId.TransactionId);
    sequoiaTransaction->AddTransactionAction(
        CellTagFromId(parentId.ObjectId),
        MakeTransactionActionData(reqAttachChild));
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

TLockId ExplicitlyLockNodeInMaster(
    TVersionedNodeId nodeId,
    ELockMode lockMode,
    const std::optional<std::string>& childKey,
    const std::optional<std::string>& attributeKey,
    TTimestamp timestamp,
    bool waitable,
    const ISequoiaTransactionPtr& sequoiaTransaction)
{
    YT_VERIFY(nodeId.IsBranched());

    NCypressServer::NProto::TReqExplicitlyLockNode request;
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

void ImplicitlyLockNodeInMaster(
    NCypressClient::TVersionedNodeId nodeId,
    NCypressClient::ELockMode lockMode,
    const std::optional<std::string>& childKey,
    const std::optional<std::string>& attributeKey,
    const NSequoiaClient::ISequoiaTransactionPtr& sequoiaTransaction)
{
    YT_VERIFY(nodeId.IsBranched());

    NCypressServer::NProto::TReqImplicitlyLockNode request;
    ToProto(request.mutable_node_id(), nodeId.ObjectId);
    ToProto(request.mutable_transaction_id(), nodeId.TransactionId);
    request.set_mode(ToProto(lockMode));
    if (childKey) {
        request.set_child_key(*childKey);
    }
    if (attributeKey) {
        request.set_attribute_key(*attributeKey);
    }

    sequoiaTransaction->AddTransactionAction(
        CellTagFromId(nodeId.ObjectId),
        MakeTransactionActionData(request));
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
    TAbsolutePathBuf path,
    std::optional<TYPath> targetPath,
    const ISequoiaTransactionPtr& sequoiaTransaction)
{
    YT_VERIFY(nodeId.IsBranched());

    sequoiaTransaction->WriteRow(NRecords::TNodeIdToPath{
        .Key = {.NodeId = nodeId.ObjectId, .TransactionId = nodeId.TransactionId},
        .Path = path.ToRealPath().Underlying(),
        .TargetPath = TYPath(targetPath.value_or("")),
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
    protoSubrequest->set_value(ToProto(subrequest.Value));
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
