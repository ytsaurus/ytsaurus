#pragma once

#include "public.h"

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/ytlib/object_client/proto/master_ypath.pb.h>

#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/client/api/client_common.h>

#include <yt/yt/client/table_client/schema.h>

#include <library/cpp/yt/yson_string/string.h>

// NB: Almost always these helpers should not be called directly since they
// don't take into account transactions and branches. Moreover, in the future
// tx action ordering should be moved from Sequoia tx to |TSequoiaSession| which
// means that _all_ interactions with Cypress should be done via
// |TSequoiaSession|.
// TODO(kvk1920 or someone else): place all these helpers inside
// |TSequoiaSession| and make them private.

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

void SetNode(
    NCypressClient::TVersionedNodeId nodeId,
    NSequoiaClient::TYPathBuf path,
    const NYson::TYsonString& value,
    bool force,
    const NApi::TSuppressableAccessTrackingOptions& options,
    const NSequoiaClient::ISequoiaTransactionPtr& sequoiaTransaction);

void MultisetNodeAttributes(
    NCypressClient::TVersionedNodeId nodeId,
    NSequoiaClient::TYPathBuf path,
    const std::vector<TMultisetAttributesSubrequest>& subrequests,
    bool force,
    const NApi::TSuppressableAccessTrackingOptions& options,
    const NSequoiaClient::ISequoiaTransactionPtr& sequoiaTransaction);

void CreateNode(
    NCypressClient::TVersionedNodeId nodeId,
    NCypressClient::TNodeId parentId,
    NSequoiaClient::TAbsoluteYPathBuf path,
    const NYTree::IAttributeDictionary* explicitAttributes,
    const TProgenitorTransactionCache& progenitorTransactionCache,
    const NSequoiaClient::ISequoiaTransactionPtr& sequoiaTransaction);

NCypressClient::TNodeId CopyNode(
    const NSequoiaClient::NRecords::TNodeIdToPath& sourceNode,
    NSequoiaClient::TAbsoluteYPathBuf destinationNodePath,
    NCypressClient::TNodeId destinationParentId,
    NCypressClient::TTransactionId cypressTransactionId,
    const TCopyOptions& options,
    const TProgenitorTransactionCache& progenitorTransactionCache,
    const NSequoiaClient::ISequoiaTransactionPtr& sequoiaTransaction);

void MaterializeNodeOnMaster(
    NCypressClient::TVersionedNodeId nodeId,
    NObjectClient::NProto::TReqMaterializeNode* originalRequest,
    const NSequoiaClient::ISequoiaTransactionPtr& sequoiaTransaction);

void MaterializeNodeInSequoia(
    NCypressClient::TVersionedNodeId nodeId,
    NCypressClient::TNodeId parentId,
    NSequoiaClient::TAbsoluteYPathBuf path,
    bool preserveAcl,
    bool preserveModificationTime,
    const TProgenitorTransactionCache& progenitorTransactionCache,
    const NSequoiaClient::ISequoiaTransactionPtr& sequoiaTransaction);

//! Removes node but not detaches it from its parent.
void RemoveNode(
    NCypressClient::TVersionedNodeId nodeId,
    NSequoiaClient::TAbsoluteYPathBuf path,
    const TProgenitorTransactionCache& progenitorTransactionCache,
    const NSequoiaClient::ISequoiaTransactionPtr& sequoiaTransaction);

void RemoveNodeAttribute(
    NCypressClient::TVersionedNodeId nodeId,
    NSequoiaClient::TYPathBuf path,
    bool force,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

//! Changes visible to user should be applied at coordinator with late prepare mode.
void AttachChild(
    NCypressClient::TVersionedNodeId parentId,
    NCypressClient::TNodeId childId,
    const std::string& childKey,
    const NApi::TSuppressableAccessTrackingOptions& options,
    const TProgenitorTransactionCache& progenitorTransactionCache,
    const NSequoiaClient::ISequoiaTransactionPtr& sequoiaTransaction);

void DetachChild(
    NCypressClient::TVersionedNodeId parentId,
    const std::string& childKey,
    const NApi::TSuppressableAccessTrackingOptions& options,
    const TProgenitorTransactionCache& progenitorTransactionCache,
    const NSequoiaClient::ISequoiaTransactionPtr& sequoiaTransaction);

NCypressClient::TLockId ExplicitlyLockNodeInMaster(
    NCypressClient::TVersionedNodeId nodeId,
    NCypressClient::ELockMode lockMode,
    const std::optional<std::string>& childKey,
    const std::optional<std::string>& attributeKey,
    NTransactionClient::TTimestamp timestamp,
    bool waitable,
    const NSequoiaClient::ISequoiaTransactionPtr& sequoiaTransaction);

void ImplicitlyLockNodeInMaster(
    NCypressClient::TVersionedNodeId nodeId,
    NCypressClient::ELockMode lockMode,
    const std::optional<std::string>& childKey,
    const std::optional<std::string>& attributeKey,
    const NSequoiaClient::ISequoiaTransactionPtr& sequoiaTransaction);

void UnlockNodeInMaster(
    NCypressClient::TVersionedNodeId nodeId,
    const NSequoiaClient::ISequoiaTransactionPtr& sequoiaTransaction);

void CreateSnapshotLockInSequoia(
    NCypressClient::TVersionedNodeId nodeId,
    NSequoiaClient::TAbsoluteYPathBuf path,
    std::optional<NSequoiaClient::TAbsoluteYPathBuf> targetPath,
    const NSequoiaClient::ISequoiaTransactionPtr& sequoiaTransaction);

void RemoveSnapshotLockFromSequoia(
    NCypressClient::TVersionedNodeId nodeId,
    const NSequoiaClient::ISequoiaTransactionPtr& sequoiaTransaction);

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NYTree::NProto::TReqMultisetAttributes::TSubrequest* protoSubrequest,
    const TMultisetAttributesSubrequest& subrequest);

void ToProto(
    NCypressServer::NProto::TAccessTrackingOptions* protoOptions,
    const NApi::TSuppressableAccessTrackingOptions& options);

void ToProto(
    NCypressServer::NProto::TReqCloneNode::TCloneOptions* protoOptions,
    const TCopyOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
