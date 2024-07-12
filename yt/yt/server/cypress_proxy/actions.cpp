#include "actions.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/ytlib/sequoia_client/helpers.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>
#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>

#include <yt/ytlib/sequoia_client/records/child_node.record.h>
#include <yt/ytlib/sequoia_client/records/node_id_to_path.record.h>
#include <yt/ytlib/sequoia_client/records/path_to_node_id.record.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NCypressProxy {

using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

using namespace NCypressClient::NProto;
using namespace NCypressServer::NProto;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

void WriteSequoiaNodeRows(
    TNodeId id,
    TAbsoluteYPathBuf path,
    const std::optional<TAbsoluteYPath>& symlinkTargetPath,
    const ISequoiaTransactionPtr& transaction)
{
    YT_VERIFY(IsLinkType(TypeFromId(id)) == symlinkTargetPath.has_value());

    transaction->WriteRow(NRecords::TPathToNodeId{
        .Key = {.Path = path.ToMangledSequoiaPath()},
        .NodeId = id,
    });

    auto record = NRecords::TNodeIdToPath{
        .Key = {.NodeId = id},
        .Path = path.ToRawYPath().Underlying(),
        .TargetPath = NYPath::TYPath(symlinkTargetPath ? symlinkTargetPath->Underlying() : ""),
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
    TNodeId id,
    const TYsonString& value,
    const ISequoiaTransactionPtr& transaction)
{
    NCypressServer::NProto::TReqSetNode setNodeRequest;
    ToProto(setNodeRequest.mutable_node_id(), id);
    setNodeRequest.set_value(value.ToString());
    transaction->AddTransactionAction(CellTagFromId(id), MakeTransactionActionData(setNodeRequest));
}

void CreateNode(
    TNodeId id,
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
    createNodeRequest.set_type(ToProto<int>(type));
    ToProto(createNodeRequest.mutable_node_id(), id);
    createNodeRequest.set_path(path.ToRawYPath().Underlying());
    ToProto(createNodeRequest.mutable_node_attributes(), *explicitAttributes);
    transaction->AddTransactionAction(CellTagFromId(id), MakeTransactionActionData(createNodeRequest));
}

TNodeId CopyNode(
    const NRecords::TNodeIdToPath& sourceNode,
    TAbsoluteYPathBuf destinationNodePath,
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
    // TODO(h0pless): Add cypress transaction id here.

    transaction->AddTransactionAction(cellTag, MakeTransactionActionData(cloneNodeRequest));

    return destinationNodeId;
}

void RemoveNode(
    TNodeId id,
    const TMangledSequoiaPath& path,
    const ISequoiaTransactionPtr& transaction)
{
    DeleteSequoiaNodeRows(id, path, transaction);

    NCypressServer::NProto::TReqRemoveNode reqRemoveNode;
    ToProto(reqRemoveNode.mutable_node_id(), id);
    transaction->AddTransactionAction(
        CellTagFromId(id),
        MakeTransactionActionData(reqRemoveNode));
}

void AttachChild(
    TNodeId parentId,
    TNodeId childId,
    const TString& childKey,
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
    transaction->AddTransactionAction(CellTagFromId(parentId), MakeTransactionActionData(attachChildRequest));
}

void DetachChild(
    TNodeId parentId,
    const TString& childKey,
    const ISequoiaTransactionPtr& transaction)
{
    transaction->DeleteRow(NRecords::TChildNodeKey{
        .ParentId = parentId,
        .ChildKey = childKey,
    });

    NCypressServer::NProto::TReqDetachChild reqDetachChild;
    ToProto(reqDetachChild.mutable_parent_id(), parentId);
    reqDetachChild.set_key(childKey);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
