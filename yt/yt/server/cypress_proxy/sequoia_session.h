#pragma once

#include "public.h"

// TODO(kvk1920): move |TCopyOptions| to somewhere to not include such a
// low-level header.
#include "action_helpers.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/ytlib/sequoia_client/records/child_node.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/table_client/schema.h>

#include <library/cpp/yt/misc/property.h>

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

public:
    static TSequoiaSessionPtr Start(const NSequoiaClient::ISequoiaClientPtr& client);

    //! Commits Sequoia transaction. If #coordinatorCellId is specified then
    //! late prepare mode will be used.
    void Commit(NObjectClient::TCellId coordinatorCellId = {});

    //! Adds tx action for rootstock removal.
    NObjectClient::TCellTag RemoveRootstock(NCypressClient::TNodeId rootstockId);

    //! Acquires lock in "node_id_to_path" Sequoia table. If object was created
    //! during this session this method is no-op.
    /*!
     *  NB: most of primitives are already taking all necessary locks. Think
     *  twice before using this method.
     */
    void LockNodeInSequoiaTable(NCypressClient::TNodeId nodeId, NTableClient::ELockType lockType);

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

    //! Used to check map-node's emptiness during non-recursive removal.
    bool IsMapNodeEmpty(NCypressClient::TNodeId mapNodeId);

    //! Removes single node. If it's map-node it has to be empty.
    //! If node is not a scion it will be detached from its parent and parent
    //! will be locked in "node_id_to_path" table.
    void DetachAndRemoveSingleNode(
        NCypressClient::TNodeId nodeId,
        NSequoiaClient::TAbsoluteYPathBuf path,
        NCypressClient::TNodeId parentId);

    //! Represents Cypress subtree selected from "path_to_node_id" Sequoia
    //! table. Used as strong typedef with some guarantees.
    struct TSelectedSubtree
    {
        // All records are sorted by mangled Sequoia path as it's done in
        // "node_id_to_path" Sequoia table. It means that the first record is
        // always root.
        std::vector<NSequoiaClient::NRecords::TPathToNodeId> Records;
    };

    //! Selects subtree from "path_to_node_id" Sequoia table.
    TSelectedSubtree SelectSubtree(NSequoiaClient::TAbsoluteYPathBuf path);

    //! Removes the whole subtree (including its root) from all resolve tables.
    //! If subtree root is not a scion then it is detached from its parent and
    //! parent is locked in "node_id_to_path" table.
    void DetachAndRemoveSubtree(const TSelectedSubtree& subtree, NCypressClient::TNodeId parentId);

    //! Creates the copy of previously selected subtree and attaches it to
    //! parent. Locks source subtree. Does _not_ locks destination parent.
    // TODO(kvk1920): consider removing #sourceRoot since it can be derived
    // from #subtree.
    NCypressClient::TNodeId CopySubtree(
        const TSelectedSubtree& subtree,
        NSequoiaClient::TAbsoluteYPathBuf sourceRoot,
        NSequoiaClient::TAbsoluteYPathBuf destinationRoot,
        NCypressClient::TNodeId destinationParentId,
        const TCopyOptions& options);

    //! Removes only the content of map-node.
    //! Detaches all removed nodes from their parents.
    //! Acquires shared lock for subree root in "node_id_to_path" table.
    void ClearSubtree(const TSelectedSubtree& subtree);

    //! The same as SelectSubtree() + ClearSubtree(). Prefer using another
    //! override of this method to avoid fetching the same subtree twice.
    void ClearSubtree(NSequoiaClient::TAbsoluteYPathBuf path);

    //! Looks up node in "node_id_to_path" Sequoia table.
    std::optional<NSequoiaClient::NRecords::TNodeIdToPath> FindNodeById(NCypressClient::TNodeId);

    //! Looks up nodes in "path_to_node_id" Sequoia table.
    std::vector<std::optional<NSequoiaClient::NRecords::TPathToNodeId>>
    FindNodesByPath(const std::vector<NSequoiaClient::NRecords::TPathToNodeIdKey>& keys);

    //! For parent P and node names [A_1, ..., A_n] creates P/A_1/.../A_n and
    //! returns ID of A_n. Attaches A_1 to P and locks P in "node_id_to_path"
    //! Sequoia table.
    //! If #names is empty just returns #startId but still locks it.
    NCypressClient::TNodeId CreateMapNodeChain(
        NSequoiaClient::TAbsoluteYPathBuf startPath,
        NCypressClient::TNodeId startId,
        TRange<TString> names);

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

    //! Selects children from "child_node" Sequoia table.
    TFuture<std::vector<NSequoiaClient::NRecords::TChildNode>> FetchChildren(
        NCypressClient::TNodeId nodeId);

private:
    // Scheduled asynchronoius requests which must be done before session is
    // committed.
    std::vector<TFuture<void>> AdditionalFutures_;

    TSequoiaSession(NSequoiaClient::ISequoiaTransactionPtr sequoiaTransaction);

    DECLARE_NEW_FRIEND()
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTY::NCypressProxy
