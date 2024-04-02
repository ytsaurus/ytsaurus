#pragma once

#include "public.h"
#include "action_helpers.h"

#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>

#include <yt/yt/client/api/transaction_client.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

//! Starts Cypress proxy transaction which is just Sequoia transaction with some
//! reordering master actions and dyntable requests to make Cypress-related
//! operations a bit simplier.
/*!
 *  The order of master tx actions is following:
 *    1. CloneNode (it should happen before removal)
 *    2. DetachChild (it should happen before any AttachChild)
 *    3. RemoveNode
 *    4. CreateNode
 *    5. AttachChild (it should happen after CreateNode)
 *    6. SetNode
 *  The order of dyntable requests is following:
 *    1. DatalessLock
 *    2. Lock
 *    3. Delete (there are no cases when we want to write row and then delete it
                 under the same Sequoia tx)
      4. Write (the typical use case: delete old row and then write new one)
 */
TFuture<NSequoiaClient::ISequoiaTransactionPtr> StartCypressProxyTransaction(
    const NSequoiaClient::ISequoiaClientPtr& sequoiaClient,
    const NApi::TTransactionStartOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

std::vector<NYPath::TYPathBuf> TokenizeUnresolvedSuffix(const NYPath::TYPath& unresolvedSuffix);
NYPath::TYPath JoinNestedNodesToPath(
    const NYPath::TYPath& parentPath,
    const std::vector<NYPath::TYPathBuf>& childKeys);

////////////////////////////////////////////////////////////////////////////////

bool IsSupportedSequoiaType(NCypressClient::EObjectType type);
bool IsSequoiaCompositeNodeType(NCypressClient::EObjectType type);
void ValidateSupportedSequoiaType(NCypressClient::EObjectType type);
void ThrowAlreadyExists(const NYPath::TYPath& path);
void ThrowNoSuchChild(const NYPath::TYPath& existingPath, const NYPath::TYPathBuf& missingPath);

////////////////////////////////////////////////////////////////////////////////

std::vector<NSequoiaClient::NRecords::TPathToNodeId> SelectSubtree(
    const NYPath::TYPath& path,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

NCypressClient::TNodeId LookupNodeId(
    const NYPath::TYPath& path,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

//! Returns bottommost created node ID.
NCypressClient::TNodeId CreateIntermediateNodes(
    const NYPath::TYPath& parentPath,
    NCypressClient::TNodeId parentId,
    const std::vector<NYPath::TYPathBuf>& nodeKeys,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

NCypressClient::TNodeId CopySubtree(
    const std::vector<NSequoiaClient::NRecords::TPathToNodeId>& sourceNodes,
    const NYPath::TYPath& sourceRootPath,
    const NYPath::TYPath& destinationRootPath,
    const TCopyOptions& options,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

//! Select subtree and remove it. #subtreeParentIdHint is only used when #removeRoot is set to true.
TFuture<void> RemoveSubtree(
    const NYPath::TYPath& path,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction,
    bool removeRoot = true,
    NCypressClient::TNodeId subtreeParentIdHint = {});

//! Same as RemoveSubtree, but does not provoke an additional select.
void RemoveSelectedSubtree(
    const std::vector<NSequoiaClient::NRecords::TPathToNodeId>& nodesToRemove,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction,
    bool removeRoot = true,
    NCypressClient::TNodeId subtreeParentIdHint = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
