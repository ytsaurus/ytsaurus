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
    const TSequoiaSessionPtr& session,
    NSequoiaClient::TRawYPath targetPath,
    const TResolveResult& resolveResult);

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
// TODO(kvk1920): from now we could sort transaction actions in
// |TSequoiaSession| instead of Sequoia transaction internals. Move all action
// sorting to |TSequoiaSession| and get rid of this function.
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

TFuture<std::vector<NSequoiaClient::NRecords::TPathToNodeId>> SelectSubtree(
    const NSequoiaClient::TAbsoluteYPath& path,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

NCypressClient::TNodeId LookupNodeId(
    NSequoiaClient::TAbsoluteYPathBuf path,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

//! Returns bottommost node ID (created or not).
/*!
 *  NB: in case of empty #nodeKeys this function just returns #parentId.
 */
NCypressClient::TNodeId CreateIntermediateNodes(
    const NSequoiaClient::TAbsoluteYPath& parentPath,
    NCypressClient::TNodeId parentId,
    TRange<TString> nodeKeys,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

//! Copies subtree.
/*!
 *  NB: all symlinks in subtree have to be passed in this function via
 *  #subtreeLinks in order to copy their target paths.
 */
NCypressClient::TNodeId CopySubtree(
    const std::vector<NSequoiaClient::NRecords::TPathToNodeId>& sourceNodes,
    const NSequoiaClient::TAbsoluteYPath& sourceRootPath,
    const NSequoiaClient::TAbsoluteYPath& destinationRootPath,
    const TCopyOptions& options,
    const THashMap<NCypressClient::TNodeId, NYPath::TYPath>& subtreeLinks,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

//! Removes previously selected subtree. If root removal is requested then
//! #subtreeParent has to be provided (it's needed to detach removed root from
//! its parent).
void RemoveSelectedSubtree(
    const std::vector<NSequoiaClient::NRecords::TPathToNodeId>& nodesToRemove,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction,
    bool removeRoot = true,
    NCypressClient::TNodeId subtreeParentId = {});

////////////////////////////////////////////////////////////////////////////////

struct TParsedReqCreate
{
    NObjectClient::EObjectType Type;
    NYTree::IAttributeDictionaryPtr ExplicitAttributes;
};

//! On parse error replies it to underlying context and returns |nullopt|.
std::optional<TParsedReqCreate> TryParseReqCreate(
    ISequoiaServiceContextPtr context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
