#pragma once

#include "public.h"
#include "action_helpers.h"

#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>

#include <yt/yt/client/api/transaction_client.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

bool IsLinkType(NCypressClient::EObjectType type);

void ValidateLinkNodeCreation(
    const ISequoiaServiceContextPtr& context,
    const NCypressClient::NProto::TReqCreate& request);

////////////////////////////////////////////////////////////////////////////////

//! Executes a given request at Sequoia. The target path might be altered after the
//! resolving process for it to be correctly handled by master in case of forwarding.
TFuture<TSharedRefArray> ExecuteVerb(
    const ISequoiaServicePtr& service,
    TSharedRefArray* requestMessage,
    const NSequoiaClient::ISequoiaClientPtr& client,
    NLogging::TLogger logger = NLogging::TLogger(),
    NLogging::ELogLevel logLevel = NLogging::ELogLevel::Debug);

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

std::vector<TString> TokenizeUnresolvedSuffix(const NSequoiaClient::TYPath& unresolvedSuffix);
NSequoiaClient::TAbsoluteYPath JoinNestedNodesToPath(
    const NSequoiaClient::TAbsoluteYPath& parentPath,
    const std::vector<TString>& childKeys);

////////////////////////////////////////////////////////////////////////////////

bool IsSupportedSequoiaType(NCypressClient::EObjectType type);
bool IsSequoiaCompositeNodeType(NCypressClient::EObjectType type);
void ValidateSupportedSequoiaType(NCypressClient::EObjectType type);
void ThrowAlreadyExists(const NSequoiaClient::TAbsoluteYPath& path);
void ThrowNoSuchChild(const NSequoiaClient::TAbsoluteYPath& existingPath, TStringBuf missingPath);

////////////////////////////////////////////////////////////////////////////////

std::vector<NSequoiaClient::NRecords::TPathToNodeId> SelectSubtree(
    const NSequoiaClient::TAbsoluteYPath& path,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

NCypressClient::TNodeId LookupNodeId(
    NSequoiaClient::TAbsoluteYPathBuf path,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

//! Returns bottommost created node ID.
NCypressClient::TNodeId CreateIntermediateNodes(
    const NSequoiaClient::TAbsoluteYPath& parentPath,
    NCypressClient::TNodeId parentId,
    const std::vector<TString>& nodeKeys,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

NCypressClient::TNodeId CopySubtree(
    const std::vector<NSequoiaClient::NRecords::TPathToNodeId>& sourceNodes,
    const NSequoiaClient::TAbsoluteYPath& sourceRootPath,
    const NSequoiaClient::TAbsoluteYPath& destinationRootPath,
    const TCopyOptions& options,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

//! Select subtree and remove it. #subtreeParentIdHint is only used when #removeRoot is set to true.
TFuture<void> RemoveSubtree(
    const NSequoiaClient::TAbsoluteYPath& path,
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
