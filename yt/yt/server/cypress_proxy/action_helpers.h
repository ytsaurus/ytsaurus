#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>

#include <yt/yt/client/api/transaction_client.h>

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
// TODO(kvk1920): from now we could sort transaction actions in
// |TSequoiaSession| instead of Sequoia transaction internals. Move all action
// sorting to |TSequoiaSession| and get rid of this function.
TFuture<NSequoiaClient::ISequoiaTransactionPtr> StartCypressProxyTransaction(
    const NSequoiaClient::ISequoiaClientPtr& sequoiaClient,
    NSequoiaClient::ESequoiaTransactionType type,
    const NApi::TTransactionStartOptions& options = {});

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
 *  NB: all links in subtree have to be passed in this function via
 *  #subtreeLinks in order to copy their target paths.
 */
NCypressClient::TNodeId CopySubtree(
    const std::vector<TCypressNodeDescriptor>& sourceNodes,
    const NSequoiaClient::TAbsoluteYPath& sourceRootPath,
    const NSequoiaClient::TAbsoluteYPath& destinationRootPath,
    const TCopyOptions& options,
    const THashMap<NCypressClient::TNodeId, NSequoiaClient::TAbsoluteYPath>& subtreeLinks,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

//! Removes previously selected subtree. If root removal is requested then
//! #subtreeParent has to be provided (it's needed to detach removed root from
//! its parent).
void RemoveSelectedSubtree(
    const std::vector<TCypressNodeDescriptor>& nodesToRemove,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction,
    NCypressClient::TTransactionId cypressTransactionId,
    bool removeRoot = true,
    NCypressClient::TNodeId subtreeParentId = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
