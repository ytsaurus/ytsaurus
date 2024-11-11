#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

//! This class is responsible for all interaction with Sequoia persistent state
//! (either master or dynamic tables).
/*!
 *  It's used as an abstraction layer to hide Cypress transactions and branching
 *  from higher-level code. All methods are designed to be used as "building
 *  blocks" for Cypress verbs implementation. They take care of locking nodes in
 *  Sequoia tables and will take care of Cypress transactions and branching.
 */
class TSequoiaSession
    : public TRefCounted
{
public:
    DEFINE_BYREF_RO_PROPERTY(NSequoiaClient::ISequoiaTransactionPtr, SequoiaTransaction);
    DEFINE_BYVAL_RO_PROPERTY(NCypressClient::TTransactionId, CypressTransactionId);

public:
    ~TSequoiaSession();

    //! Represents Cypress subtree selected from "path_to_node_id" Sequoia
    //! table. Used as strong typedef with some guarantees.
    struct TSubtree
    {
        // In-order traverse of subtree.
        std::vector<TCypressNodeDescriptor> Nodes;
    };

    // Start and finish.

    //! Initializes Sequoia session and starts Sequoia tx.
    static TSequoiaSessionPtr Start(
        IBootstrap* bootstrap,
        NCypressClient::TTransactionId cypressTransactionId);

    //! Commits Sequoia transaction.
    // TODO(kvk1920): derive #coordinatorCellId automatically from registered actions.
    void Commit(NObjectClient::TCellId coordinatorCellId);

    //! Cancels all background activity related to this session and aborts
    //! Sequoia transaction.
    void Abort();

    // Operations over subtree.

    //! Selects subtree from "path_to_node_id" Sequoia table.
    TSubtree FetchSubtree(NSequoiaClient::TAbsoluteYPathBuf path);

    //! Removes the whole subtree (including its root) from all resolve tables.
    //! If subtree root is not a scion then it is detached from its parent and
    //! parent is locked in "node_id_to_path" table.
    void DetachAndRemoveSubtree(const TSubtree& subtree, NCypressClient::TNodeId parentId);

    //! Creates the copy of previously selected subtree and attaches it to
    //! parent. Locks source subtree. Does _not_ locks destination parent.
    // TODO(kvk1920): consider removing #sourceRoot since it can be derived
    // from #subtree.
    NCypressClient::TNodeId CopySubtree(
        const TSubtree& subtree,
        NSequoiaClient::TAbsoluteYPathBuf sourceRoot,
        NSequoiaClient::TAbsoluteYPathBuf destinationRoot,
        NCypressClient::TNodeId destinationParentId,
        const TCopyOptions& options);

    //! Removes only the content of map-node.
    //! Detaches all removed nodes from their parents.
    //! Acquires shared lock for subree root in "node_id_to_path" table.
    void ClearSubtree(const TSubtree& subtree);

    //! The same as SelectSubtree() + ClearSubtree(). Prefer using another
    //! override of this method to avoid fetching the same subtree twice.
    void ClearSubtree(NSequoiaClient::TAbsoluteYPathBuf path);

    // Miscelanous.

    //! Behaves like the "set" verb in Cypress. Acquires a shared lock on row in
    //! "node_id_to_path" Sequoia table.
    /*!
     *  Locking rationale: the only case when this action is done for multiple
     *  nodes per request is subtree setting. In this case only one node was
     *  visible before current request (it's subtree root). Due to late prepare
     *  _most_ of node modifications cannot interfere with each other except for
     *  subtree removal. So it's sufficient to conflict with removal: we can do
     *  it with shared lock because node removal takes an exclusive lock.
     */
    void SetNode(NCypressClient::TNodeId nodeId, NYson::TYsonString value);

    //! Behaves the same as |TSequoiaSession::SetNode|, but used specifically
    //! for setting node attributes.
    void SetNodeAttribute(
        NCypressClient::TNodeId nodeId,
        NSequoiaClient::TYPathBuf path,
        NYson::TYsonString value,
        bool force);

    //! Behaves like the "multiset_attributes" verb in Cypress. Acquires a shared
    //! lock on row in "node_id_to_path" Sequoia table.
    void MultisetNodeAttributes(
        NCypressClient::TNodeId nodeId,
        NSequoiaClient::TYPathBuf path,
        const std::vector<TMultisetAttributesSubrequest>& subrequests,
        bool force);

    //! Removes node attribute at a given path. Acquires a shared lock on row in
    //! "node_id_to_path" Sequoia table.
    void RemoveNodeAttribute(
        NCypressClient::TNodeId nodeId,
        NSequoiaClient::TYPathBuf path,
        bool force);

    //! Removes single node. If it's map-node it has to be empty.
    //! If node is not a scion it will be detached from its parent and parent
    //! will be locked in "node_id_to_path" table.
    void DetachAndRemoveSingleNode(
        NCypressClient::TNodeId nodeId,
        NSequoiaClient::TAbsoluteYPathBuf path,
        NCypressClient::TNodeId parentId);

    //! For parent P and node names [A_1, ..., A_n] creates P/A_1/.../A_n and
    //! returns ID of A_n. Attaches A_1 to P and locks P in "node_id_to_path"
    //! Sequoia table.
    //! If #names is empty just returns #startId but still locks it.
    NCypressClient::TNodeId CreateMapNodeChain(
        NSequoiaClient::TAbsoluteYPathBuf startPath,
        NCypressClient::TNodeId startId,
        TRange<std::string> names);

    //! Generates ID, registers tx action and modifies "path_to_node_id",
    //! "node_id_to_path" Sequoia tables. Attaches created node to its parent.
    //! Does _not_ lock parent.
    // TODO(kvk1920): write a large comment about locks. Locks are needed in 2
    // cases: _near_ creation of subtree root and _near_ removal of subtree.
    NCypressClient::TNodeId CreateNode(
        NObjectClient::EObjectType type,
        NSequoiaClient::TAbsoluteYPathBuf path,
        const NYTree::IAttributeDictionary* explicitAttributes,
        NCypressClient::TNodeId parentId);

    // Map-node's children accessors.

    //! Selects children from "child_node" Sequoia table.
    TFuture<std::vector<TCypressChildDescriptor>> FetchChildren(NCypressClient::TNodeId nodeId);

    //! Used to check map-node's emptiness during non-recursive removal.
    /*!
     *  NB: of course, |IsMapNodeEmpty(X)| is semantically equal to
     *  |FetchChildren(X).empty()|, but |IsMapNodeEmpty()| is better in case of
     *  non-empty map-node. Both of methods are implemented as "SELECT FROM
     *  child_nodes WHERE parent_id == mapNodeId", but this metod has "LIMIT 1".
     */
    bool IsMapNodeEmpty(NCypressClient::TNodeId mapNodeId);

    // Resolve helpers.

    //! Finds node path by its ID.
    /*!
     *  If such node exists but this function returns |nullopt| then this node
     *  is a snapshot branch.
     *
     *  NB: caches link's target path if needed.
     */
    std::optional<NSequoiaClient::TAbsoluteYPath> FindNodePath(NCypressClient::TNodeId id);

    //! Finds target path of link.
    /*!
     *  NB: if this node was already fetched from "node_id_to_path" Sequoia
     *  table this method is no-op.
     */
    NSequoiaClient::TAbsoluteYPath GetLinkTargetPath(NCypressClient::TNodeId linkId);
    THashMap<NCypressClient::TNodeId, NSequoiaClient::TAbsoluteYPath> GetLinkTargetPaths(
        TRange<NCypressClient::TNodeId> linkIds);

    //! Looks up nodes in "path_to_node_id" Sequoia table.
    std::vector<NCypressClient::TNodeId> FindNodeIds(
        TRange<NSequoiaClient::TAbsoluteYPathBuf> paths);

    // Low-level helpers.

    //! Adds tx action for rootstock removal.
    NObjectClient::TCellTag RemoveRootstock(NCypressClient::TNodeId rootstockId);

    //! Used only to implement "lock" verb.
    NCypressClient::TLockId LockNode(
        NCypressClient::TNodeId nodeId,
        NCypressClient::ELockMode lockMode,
        const std::optional<std::string>& childKey,
        const std::optional<std::string>& attributeKey,
        NTransactionClient::TTimestamp timestamp,
        bool waitable);

    void UnlockNode(NCypressClient::TNodeId nodeId);

private:
    IBootstrap* const Bootstrap_;
    // Scheduled asynchronoius requests which must be done before session is
    // committed.
    std::vector<TFuture<void>> AdditionalFutures_;
    bool Finished_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, CachedLinkTargetPathsLock_);
    THashMap<NCypressClient::TNodeId, NSequoiaClient::TAbsoluteYPath> CachedLinkTargetPaths_;

    TSequoiaSession(
        IBootstrap* bootstrap,
        NSequoiaClient::ISequoiaTransactionPtr sequoiaTransaction,
        NCypressClient::TTransactionId cypressTransactionId);

    //! Acquires lock in "node_id_to_path" Sequoia table. If object was created
    //! during this session this method is no-op.
    void MaybeLockNodeInSequoiaTable(NCypressClient::TNodeId nodeId, NTableClient::ELockType lockType);

    //! Acquires removal lock in "node_id_to_path" Sequoia table.
    void MaybeLockForRemovalInSequoiaTable(NCypressClient::TNodeId nodeId);

    //! Returns whether or not an object with a given ID _could_ be created in
    //! this session.
    /*!
     *  This method is used to avoid unnecessary locks.
     *
     *  NB: relies only on Sequoia tx start timestamp so it cannot be determined
     *  if a given ID was actually generated in this session, but considering
     *  object existence the result of this method is guaranteed to be correct.
     */
    bool JustCreated(NObjectClient::TObjectId nodeId);

    void LockAndReplicateCypressTransaction();

    void DoSetNode(
        NCypressClient::TNodeId nodeId,
        NSequoiaClient::TYPathBuf path,
        NYson::TYsonString value,
        bool force);

    DECLARE_NEW_FRIEND()
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTY::NCypressProxy
