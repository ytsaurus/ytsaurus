#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/client/api/client_common.h>

#include <yt/yt/client/table_client/schema.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

//! This class is responsible for all interaction with Sequoia persistent state
//! (either master or dynamic tables).
/*!
 *  Thread affinity: single.
 *
 *  It's used as an abstraction layer to hide Cypress transactions and branching
 *  from higher-level code. All methods are designed to be used as "building
 *  blocks" for Cypress verbs implementation. They take care of locking nodes in
 *  Sequoia tables and Cypress transactions and branching.
 *
 *  Cypress locks (without snapshot locks).
 *
 *  Since we cannot release a specific Cypress lock in master we have to check
 *  lock conflicts in prepare phase and acquire locks in commit phase. We need
 *  to ensure that after successful prepare for Cypress lock node locking state
 *  won't be changed until commit. The way to achieve this is to lock rows in
 *  Sequoia dynamic tables.
 *
 *  There are 2 major cases: Cypress lock can be acquired on particular
 *  participant either in 2PC or LP (late prepare). In case of LP we don't
 *  actually need to conflict with other locks acquired in LP mode since all
 *  conflicts can be resolved by master. But we still have to take care of 2PC
 *  case. For now, let's just acquire the shared (read) lock on row
 *  (nodeId, trunk) in "node_id_to_path" Sequoia table.
 *
 *  For 2PC locks we're acquiring exclusve lock on row (nodeId, trunk). This
 *  allows to 2PC lock to conflict with other 2PC locks and LP locks.
 *
 *  Snapshot locks.
 *
 *  Snapshot locks are always acquired in LP (late prepare) mode so it should
 *  only conflict with 2PC locks in Sequoia tables. Snapshot lock of node N
 *  under transaction T conflicts with all non-snapshot locks of node N under
 *  T or any descendant of T. It can be done in a following way:
 *    - every 2PC lock acquires exclusive lock on rows (N, T_) for T_ in [T] +
 *      ancestors_of(T);
 *    - every snapshot lock acquires shared lock on row (N, T).
 *  Therefore, every snapshot lock will conflict with other locks in Sequoia
 *  tables only if it could conflict with them in master.
 */
class TSequoiaSession
    : public TRefCounted
{
public:

    //! Consists of Cypress transaction and all its ancestors plus null tx ID
    //! signifying trunk. Transactions are ordered from trunk to current.
    using TCypressTransactionAncestry = std::vector<NCypressClient::TTransactionId>;
    using TCypressTransactionAncestryView = TRange<NCypressClient::TTransactionId>;

    // NB: Trunk depth is zero.
    using TCypressTransactionDepths = absl::flat_hash_map<
        NCypressClient::TTransactionId,
        int,
        ::THash<NCypressClient::TTransactionId>>;

public:
    DEFINE_BYREF_RO_PROPERTY(NSequoiaClient::ISequoiaTransactionPtr, SequoiaTransaction);

public:
    ~TSequoiaSession();

    NCypressClient::TTransactionId GetCurrentCypressTransactionId() const;

    //! Represents Cypress subtree selected from "path_to_node_id" Sequoia
    //! table. Used as strong typedef with some guarantees.
    struct TSubtree
    {
        // In-order traverse of subtree.
        std::vector<TCypressNodeDescriptor> Nodes;

        const TCypressNodeDescriptor& GetRoot() const;
    };

    // Start and finish.

    //! Initializes Sequoia session and starts Sequoia tx.
    static TSequoiaSessionPtr Start(
        IBootstrap* bootstrap,
        NCypressClient::TTransactionId cypressTransactionId = NCypressClient::NullTransactionId);

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
    //! parent is S-locked in "node_id_to_path" table. If detach occurs in late
    //! prepare mode then parent is S-locked in shared mode. Otherwise,
    //! exclusive S-lock is used to conflict with explicit C-lock requests.
    void DetachAndRemoveSubtree(
        const TSubtree& subtree,
        NCypressClient::TNodeId parentId,
        bool detachInLatePrepare);

    //! Creates the copy of previously selected subtree and attaches it to
    //! parent. Does _not_ S-lock destination's parent.
    NCypressClient::TNodeId CopySubtree(
        const TSubtree& sourceSubtree,
        NSequoiaClient::TAbsoluteYPathBuf destinationRoot,
        NCypressClient::TNodeId destinationParentId,
        const TCopyOptions& options);

    //! Removes only the content of map-node.
    //! Detaches all removed nodes from their parents.
    //! Acquires shared S-lock for subree root in "node_id_to_path" table and
    //! exclusive S-locks for subtree.
    /*!
     *  NB: requires "late prepare" with subtree root's cell as a coordinator.
     */
    void ClearSubtree(
        NSequoiaClient::TAbsoluteYPathBuf path,
        const NApi::TSuppressableAccessTrackingOptions& options);

    // Miscelanous.

    //! Behaves like the "set" verb in Cypress. Acquires a shared S-lock on row
    //! in "node_id_to_path" Sequoia table.
    /*!
     *  Locking rationale: the only case when this action is done for multiple
     *  nodes per request is subtree setting. In this case only one node was
     *  visible before current request (it's subtree root). Due to late prepare
     *  _most_ of node modifications cannot interfere with each other except for
     *  subtree removal. So it's sufficient to conflict with removal: we can do
     *  it with shared S-lock because node removal takes an exclusive S-lock.
     */
    void SetNode(
        NCypressClient::TNodeId nodeId,
        NYson::TYsonString value,
        const NApi::TSuppressableAccessTrackingOptions& options);

    //! Behaves the same as |TSequoiaSession::SetNode|, but used specifically
    //! for setting node attributes.
    void SetNodeAttribute(
        NCypressClient::TNodeId nodeId,
        NSequoiaClient::TYPathBuf path,
        NYson::TYsonString value,
        bool force,
        const NApi::TSuppressableAccessTrackingOptions& options);

    //! Behaves like the "multiset_attributes" verb in Cypress. Acquires a
    //! shared S-lock on row in "node_id_to_path" Sequoia table.
    void MultisetNodeAttributes(
        NCypressClient::TNodeId nodeId,
        NSequoiaClient::TYPathBuf path,
        const std::vector<TMultisetAttributesSubrequest>& subrequests,
        bool force,
        const NApi::TSuppressableAccessTrackingOptions& options);

    //! Removes node attribute at a given path. Acquires a shared S-lock on row
    //! in "node_id_to_path" Sequoia table.
    void RemoveNodeAttribute(
        NCypressClient::TNodeId nodeId,
        NSequoiaClient::TYPathBuf path,
        bool force);

    //! Removes single node. If it's map-node it has to be empty.
    //! If node is not a scion it will be detached from its parent and parent
    //! will be S-locked in "node_id_to_path" table (in shared mode).
    //! Requires late prepare on cell which owns |parentId|.
    void DetachAndRemoveSingleNode(
        NCypressClient::TNodeId nodeId,
        NSequoiaClient::TAbsoluteYPathBuf path,
        NCypressClient::TNodeId parentId);

    //! For parent P and node names [A_1, ..., A_n] creates P/A_1/.../A_n and
    //! returns ID of A_n. Attaches A_1 to P and S-locks P in "node_id_to_path"
    //! Sequoia table (in shared mode).
    //! If #names is empty just returns #startId but still S-locks it.
    //! Requires late prepare on cell which owns |startId|.
    NCypressClient::TNodeId CreateMapNodeChain(
        NSequoiaClient::TAbsoluteYPathBuf startPath,
        NCypressClient::TNodeId startId,
        TRange<std::string> names,
        const NApi::TSuppressableAccessTrackingOptions& options);

    //! Generates ID, registers tx action and modifies "path_to_node_id",
    //! "node_id_to_path" and "child_node" Sequoia tables. Attaches created node
    //! to its parent. Does _not_ S-lock parent.
    /*!
     *  NB: parent is usually locked by CreateMapNodeChain().
     */
    NCypressClient::TNodeId CreateNode(
        NObjectClient::EObjectType type,
        NSequoiaClient::TAbsoluteYPathBuf path,
        const NYTree::IAttributeDictionary* explicitAttributes,
        NCypressClient::TNodeId parentId,
        const NApi::TSuppressableAccessTrackingOptions& options);

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

    struct TResolvedNodeId
    {
        NSequoiaClient::TAbsoluteYPath Path;
        bool IsSnapshot;
    };

    //! Finds node path by its ID.
    /*!
     *  Returns |nullopt| if there is no such node under current Cypress tx.
     *
     *  NB: Caches link's target path if needed.
     */
    std::optional<TResolvedNodeId> FindNodePath(NCypressClient::TNodeId id);

    //! Finds target path of link.
    /*!
     *  NB: If this node was already fetched from "node_id_to_path" Sequoia
     *  table this method is no-op.
     */
    NSequoiaClient::TAbsoluteYPath GetLinkTargetPath(NCypressClient::TNodeId linkId);
    THashMap<NCypressClient::TNodeId, NSequoiaClient::TAbsoluteYPath> GetLinkTargetPaths(
        TRange<NCypressClient::TNodeId> linkIds);

    //! Looks up nodes in "path_to_node_id" Sequoia table.
    /*!
     *  #path[i] is required to be prefix of #path[i + 1].
     *  #path[0] must always be "/".
     */
    std::vector<NCypressClient::TNodeId> FindNodeIds(
        TRange<NSequoiaClient::TAbsoluteYPathBuf> paths);

    // Low-level helpers.

    //! Adds tx action for rootstock removal.
    NObjectClient::TCellTag RemoveRootstock(NCypressClient::TNodeId rootstockId);

    //! Used only to implement "lock" verb (and mustn't be used in other verbs).
    //! Acquires shared S-lock in "node_id_to_path" table.
    //! Requires late prepare on cell which owns |nodeId|.
    NCypressClient::TLockId LockNode(
        NCypressClient::TNodeId nodeId,
        NCypressClient::ELockMode lockMode,
        const std::optional<std::string>& childKey,
        const std::optional<std::string>& attributeKey,
        NTransactionClient::TTimestamp timestamp,
        bool waitable);

    void UnlockNode(NCypressClient::TNodeId nodeId, bool snapshot);

private:
    IBootstrap* const Bootstrap_;
    const TCypressTransactionAncestry CypressTransactionAncestry_;
    const TCypressTransactionDepths CypressTransactionDepths_;

    bool Finished_ = false;

    NObjectClient::TCellTag RequiredCoordinatorCellTag_ = {};

    THashMap<NCypressClient::TNodeId, NSequoiaClient::TAbsoluteYPath> CachedLinkTargetPaths_;

    // This cache is filled on every read of Sequoia resolve tables.
    TProgenitorTransactionCache ProgenitorTransactionCache_;

    // Locking of rows in "node_id_to_path" and "child_node" table heavily
    // depends on Sequoia transaction commit mode on node's native cell. Since
    // we already have |RequiredCoordinatorCellTag_| we can just check it before
    // transaction commit and acquire corresponding dynamic table locks.
    struct TCypressLock
    {
        NCypressClient::TNodeId NodeId;
        bool IsSnapshot;
    };
    std::vector<TCypressLock> AcquiredCypressLocks_;

    TSequoiaSession(
        IBootstrap* bootstrap,
        NSequoiaClient::ISequoiaTransactionPtr sequoiaTransaction,
        std::vector<NCypressClient::TTransactionId> cypressTransactionIds);

    //! Returns whether or not an object with a given ID _could_ be created in
    //! this session.
    /*!
     *  This method is used to avoid unnecessary S-locks.
     *
     *  NB: relies only on Sequoia tx start timestamp so it cannot be determined
     *  if a given ID was actually generated in this session, but considering
     *  object existence the result of this method is guaranteed to be correct.
     */
    bool JustCreated(NObjectClient::TObjectId nodeId);

    //! Replicates Cypress transaction if needed.
    void MaybeLockAndReplicateCypressTransaction();

    void DoSetNode(
        NCypressClient::TNodeId nodeId,
        NSequoiaClient::TYPathBuf path,
        NYson::TYsonString value,
        bool force,
        const NApi::TSuppressableAccessTrackingOptions& options);

    //! Some actions have to be done in late prepare on a certain cell. This
    //! method is used to detect conflicts between such actions.
    void RequireLatePrepare(NObjectClient::TCellTag coordinatorCellTag);
    void RequireLatePrepareOnNativeCellFor(NCypressClient::TNodeId nodeId);

    TFuture<TSubtree> DoFetchSubtree(NSequoiaClient::TAbsoluteYPathBuf path);

    // Locks rows in "node_id_to_path" Sequoia table.
    void AcquireCypressLockInSequoia(
        NCypressClient::TNodeId nodeId,
        NCypressClient::ELockMode mode);
    void ProcessAcquiredCypressLocksInSequoia();

    DECLARE_NEW_FRIEND()
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
