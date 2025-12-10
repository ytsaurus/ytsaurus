#include "mapper.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/chunk_server/chunk_owner_base.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>
#include <yt/yt/server/master/cypress_server/helpers.h>
#include <yt/yt/server/master/cypress_server/link_node.h>
#include <yt/yt/server/master/cypress_server/node_detail.h>
#include <yt/yt/server/master/cypress_server/node.h>

#include <yt/yt/server/master/object_server/attribute_set.h>

#include <yt/yt/server/master/security_server/group.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/lib/sequoia/cypress_transaction.h>

#include <yt/yt/ytlib/sequoia_client/records/acls.record.h>
#include <yt/yt/ytlib/sequoia_client/records/child_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/child_node.record.h>
#include <yt/yt/ytlib/sequoia_client/records/dependent_transactions.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_snapshots.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transaction_descendants.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transaction_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transactions.record.h>

#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSequoiaReconstructor {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSequoiaServer;
using namespace NSequoiaClient;
using namespace NTransactionServer;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TSequoiaReconstructorMapper
{
public:
    TSequoiaReconstructorMapper(TBootstrap* bootstrap, TRecordsConsumer* consumer)
        : Bootstrap_(bootstrap)
        , Consumer_(consumer)
    { }

    void ProcessNode(TCypressNode* node)
    {
        YT_VERIFY(IsSequoiaId(node->GetId()));

        if (node->IsTrunk()) {
            ProcessTrunkNode(node);
        } else {
            ProcessBranchedNode(node);
        }

        if (node->GetType() == EObjectType::SequoiaMapNode || node->GetType() == EObjectType::Scion) {
            ProcessMapNode(node->As<TSequoiaMapNode>());
        }
    }

    void ProcessTransaction(const TTransaction* transaction)
    {
        YT_VERIFY(transaction->GetIsCypressTransaction());

        if (transaction->IsNative()) {
            ProcessNativeTransaction(transaction);
        } else {
            ProcessTransactionReplica(transaction);
        }
    }

private:
    TBootstrap* Bootstrap_;
    TRecordsConsumer* Consumer_;

    void ProcessTrunkNode(const TCypressNode* trunkNode)
    {
        YT_VERIFY(!trunkNode->GetTransaction());
        YT_VERIFY(trunkNode->IsTrunk());

        if (trunkNode->GetReachable()) {
            // If the trunk node is reachable, than the node is created outside of unfinished transaction.
            // We should write info to NodeIdToPath table without any node forks.
            WriteNodeIdToPath(trunkNode, EForkKind::Regular);
            WritePathToNodeIdForTrunkNode(trunkNode);
        }

        WriteAclsForTrunkNode(trunkNode);
    }

    void ProcessBranchedNode(const TCypressNode* branchedNode)
    {
        const auto* trunkNode = branchedNode->GetTrunkNode();
        YT_VERIFY(trunkNode);
        YT_VERIFY(branchedNode != trunkNode);
        YT_VERIFY(branchedNode->GetTransaction());

        const auto* originatorNode = branchedNode->GetOriginator();

        if (branchedNode->MutableSequoiaProperties()->Tombstone) {
            if (originatorNode->GetReachable()) {
                // In case of tombstone we should check that originator node is reachable for node_id_to_path tables.
                WriteNodeIdToPath(branchedNode, EForkKind::Tombstone);
                WriteNodeFork(branchedNode);

                if (GetOldestReachableBranchingAncestor(branchedNode) == branchedNode->GetTrunkNode()) {
                    // We don't write PathToNodeIdInfo for every trunk node to optimize out later reduce.
                    // However, for reachable trunk nodes that were removed inside transactions, we need to add this records.
                    // During the reduce stage all tombstone records that have no records for ancestor transactions will be filtered out.
                    WritePathToNodeChange(branchedNode->GetTrunkNode());
                }
            }
            // We may need to write tombstone records for nodes that were removed from every transaction.
            // This will happen if the same path exists in some ancestor transactions (it will be checked during reduce stage).
            WritePathToNodeChange(branchedNode);
        } else if (branchedNode->GetLockMode() == ELockMode::Snapshot) {
            WriteNodeIdToPath(branchedNode, EForkKind::Snapshot);
            WriteSnapshotNodeRow(branchedNode);
        } else if (branchedNode->GetReachable()) {
            if (!originatorNode->GetReachable()) {
                // The node should be created in this transaction.
                WriteNodeIdToPath(branchedNode, EForkKind::Regular);
                WriteNodeFork(branchedNode);
                WritePathToNodeChange(branchedNode);
            }
        }

    }

    void WriteNodeIdToPath(const TCypressNode* node, EForkKind forkKind)
    {
        if (Consumer_->NodeIdToPath) {
            auto row = NRecords::TNodeIdToPath{
                .Key = {.NodeId = node->GetId(), .TransactionId = GetObjectId(node->GetTransaction())},
                .Path = node->ImmutableSequoiaProperties()->Path,
                .TargetPath = GetTargetPathOrEmpty(node),
                .ForkKind = forkKind,
            };

            Consumer_->NodeIdToPath->Consume(row);
        }
    }

    NYPath::TYPath GetTargetPathOrEmpty(const TCypressNode* node)
    {
        if (node->GetType() != EObjectType::SequoiaLink) {
            return "";
        }

        const auto* link = node->As<TLinkNode>();
        YT_VERIFY(link);
        return link->GetTargetPath();
    }

    void WriteNodeFork(const TCypressNode* node)
    {
        if (Consumer_->NodeForks) {
            YT_VERIFY(node->GetTransaction());

            auto row = NRecords::TNodeFork{
                .Key = {.TransactionId = node->GetTransaction()->GetId(), .NodeId = node->GetId()},
                .Path = node->ImmutableSequoiaProperties()->Path,
                .TargetPath = GetTargetPathOrEmpty(node),
            };

            const auto* oldestReachableBranchingAncestor = GetOldestReachableBranchingAncestor(node);
            YT_VERIFY(oldestReachableBranchingAncestor);

            row.ProgenitorTransactionId = GetObjectId(oldestReachableBranchingAncestor->GetTransaction());

            Consumer_->NodeForks->Consume(row);
        }
    }

    const TCypressNode* GetOldestReachableBranchingAncestor(const TCypressNode* node)
    {
        const TCypressNode* oldestReachableBranchingAncestor = nullptr;

        while (true) {
            if (node->GetReachable()) {
                oldestReachableBranchingAncestor = node;
            }
            if (node->IsTrunk()) {
                break;
            }

            YT_VERIFY(node->GetTransaction());
            node = node->GetOriginator();
        }
        return oldestReachableBranchingAncestor;
    }

    void WriteSnapshotNodeRow(const TCypressNode* node)
    {
        if (Consumer_->NodeSnapshots) {
            Consumer_->NodeSnapshots->Consume(NRecords::TNodeSnapshot{
                .Key = {.TransactionId = node->GetTransaction()->GetId(), .NodeId = node->GetId()},
                .Dummy = 0,
            });
        }
    }

    void WritePathToNodeIdForTrunkNode(const TCypressNode* node)
    {
        YT_VERIFY(node->IsTrunk());
        YT_VERIFY(node->GetReachable());

        if (Consumer_->PathToNodeId) {
            auto path = node->ImmutableSequoiaProperties()->Path;
            Consumer_->PathToNodeId->Consume(NRecords::TPathToNodeId{
                .Key = {
                    .Path = TAbsolutePath::MakeCanonicalPathOrThrow(path).ToMangledSequoiaPath(),
                    .TransactionId = NullTransactionId,
                },
                .NodeId = node->GetId(),
            });
        }
    }

    void WritePathToNodeChange(const TCypressNode* node)
    {
        if (Consumer_->PathToNodeChanges) {
            TPathToNodeChangeRecord pathToNodeChange;

            auto path = node->ImmutableSequoiaProperties()->Path;
            pathToNodeChange.Path = TAbsolutePath::MakeCanonicalPathOrThrow(path).ToMangledSequoiaPath();

            if (node->GetReachable()) {
                pathToNodeChange.NodeId = node->GetId();
            }
            if (node->GetTransaction()) {
                pathToNodeChange.TransactionId = node->GetTransaction()->GetId();
            }

            while (!node->IsTrunk()) {
                pathToNodeChange.TransactionAncestors.push_back(node->GetTransaction()->GetId());
                node = node->GetOriginator();
            }
            pathToNodeChange.TransactionAncestors.push_back(NullTransactionId);

            Consumer_->PathToNodeChanges->Consume(pathToNodeChange);
        }
    }

    void WriteAclsForTrunkNode(const TCypressNode* node)
    {
        YT_VERIFY(node->IsTrunk());

        if (Consumer_->Acls) {
            Consumer_->Acls->Consume(NRecords::TAcls{
                .Key = {.NodeId = node->GetId()},
                .Acl = ConvertToYsonString(node->Acd().Acl()),
                .Inherit = node->Acd().Inherit(),
            });
        }
    }

    void ProcessMapNode(const TSequoiaMapNode* mapNode)
    {
        if (!mapNode->GetReachable()) {
            YT_VERIFY(mapNode->KeyToChild().empty());
            return;
        }

        if (mapNode->IsTrunk()) {
            ProcessTrunkMapNode(mapNode);
        } else {
            ProcessBranchedMapNode(mapNode);
        }
    }

    void ProcessTrunkMapNode(const TSequoiaMapNode* trunkNode)
    {
        YT_VERIFY(trunkNode->IsTrunk());

        for (const auto& [key, childId] : trunkNode->KeyToChild()) {
            YT_VERIFY(childId, "Trunk node has empty child");
            WriteChildNode(trunkNode, key, childId);
        }
    }

    void ProcessBranchedMapNode(const TSequoiaMapNode* branchedNode)
    {
        YT_VERIFY(!branchedNode->IsTrunk());
        YT_VERIFY(branchedNode->GetTransaction());

        const auto* originatorNode = branchedNode->GetOriginator()->As<TSequoiaMapNode>();
        YT_VERIFY(originatorNode);

        for (const auto& [key, childId] : branchedNode->KeyToChild()) {
            auto originatorChildId = GetChildId(originatorNode, key);

            if (childId != originatorChildId ||
                (childId == NullObjectId && GetOldestBranchingAncestorWithExistingChild(branchedNode, key) != nullptr)) {
                // If childId is different from originator child id, than we definitely have child changed in current transaction
                // and we should write both ChildNode and ChildFork rows.

                // If childId is same as originator child, but we have NullObjectId record, we may still need to write these rows.
                // That may happen if in the ancestor transactions the child was removed and recreated
                // and in commited descendant transactions child was recreated and removed.

                WriteChildNode(branchedNode, key, childId);
                WriteChildFork(branchedNode, key, childId);
            }
        }
    }

    void WriteChildNode(
        const TSequoiaMapNode* parent,
        const std::string& key,
        NCypressServer::TNodeId childId)
    {
        if (Consumer_->ChildNode) {
            auto row = NRecords::TChildNode{
                .Key = {.ParentId = parent->GetId(), .TransactionId = GetObjectId(parent->GetTransaction()), .ChildKey = key},
                .ChildId = childId,
            };

            Consumer_->ChildNode->Consume(row);
        }
    }

    void WriteChildFork(
        const TSequoiaMapNode* parent,
        const std::string& key,
        NCypressServer::TNodeId childId)
    {
        if (Consumer_->ChildForks) {
            auto childFork = NRecords::TChildFork{
                .Key = {.TransactionId = parent->GetTransaction()->GetId(), .ParentId = parent->GetId(), .ChildKey = key},
                .ChildId = childId,
            };

            const auto* oldestBranchingAncestorWithExistingChild = GetOldestBranchingAncestorWithExistingChild(parent, key);
            YT_VERIFY(oldestBranchingAncestorWithExistingChild);

            childFork.ProgenitorTransactionId = GetObjectId(oldestBranchingAncestorWithExistingChild->GetTransaction());

            Consumer_->ChildForks->Consume(childFork);
        }
    }

    const TSequoiaMapNode* GetOldestBranchingAncestorWithExistingChild(
        const TSequoiaMapNode* node,
        const std::string& key)
    {
        const TSequoiaMapNode* oldestBranchingAncestorWithExistingChild = nullptr;

        while (true) {
            if (auto childIt = node->KeyToChild().find(key);
                childIt != node->KeyToChild().end() && childIt->second != NullObjectId)
            {
                oldestBranchingAncestorWithExistingChild = node;
            }
            if (node->IsTrunk()) {
                break;
            }

            YT_VERIFY(node->GetTransaction());
            node = node->GetOriginator()->As<TSequoiaMapNode>();
        }
        return oldestBranchingAncestorWithExistingChild;
    }

    NCypressServer::TNodeId GetChildId(const TSequoiaMapNode* parent, const std::string& key)
    {
        // We traverse through branching ancestors searching for an existing child.
        // Found child can be either node or NullObjectId indicating tombstone child record.
        // If no child is found in all branching ancestors up to trunk node, we return NullObjectId.
        while (true) {
            if (auto childIt = parent->KeyToChild().find(key); childIt != parent->KeyToChild().end()) {
                return childIt->second;
            }
            if (parent->IsTrunk()) {
                return NullObjectId;
            }
            parent = parent->GetOriginator()->As<TSequoiaMapNode>();
        }
    }

    void ProcessNativeTransaction(const TTransaction* transaction)
    {
        YT_VERIFY(transaction->IsNative());

        std::vector<TTransactionId> ancestorIds;
        auto ancestor = transaction->GetParent();
        while (ancestor) {
            WriteTransactionDescendant(ancestor, transaction);

            ancestorIds.push_back(ancestor->GetId());
            ancestor = ancestor->GetParent();
        }
        // Transaction ancestor ids should be ordered from most oldest to newest parent.
        std::ranges::reverse(ancestorIds);

        std::vector<TTransactionId> prerequisiteTransactionIds;
        for (auto prerequisiteTransaction : transaction->PrerequisiteTransactions()) {
            WriteDependentTransaction(prerequisiteTransaction, transaction);

            prerequisiteTransactionIds.push_back(prerequisiteTransaction->GetId());
        }

        WriteTransaction(transaction, std::move(ancestorIds), std::move(prerequisiteTransactionIds));
    }

    void ProcessTransactionReplica(const TTransaction* transaction)
    {
        YT_VERIFY(!transaction->IsNative());
        WriteTransactionReplica(transaction);
    }

    void WriteDependentTransaction(TTransactionRawPtr prerequisite, const TTransaction* dependent)
    {
        if (Consumer_->DependentTransactions) {
            Consumer_->DependentTransactions->Consume(NRecords::TDependentTransaction{
                .Key = {.TransactionId = prerequisite->GetId(), .DependentTransactionId = dependent->GetId()},
                .Dummy = 0,
            });
        }
    }

    void WriteTransactionDescendant(TTransactionRawPtr ancestor, const TTransaction* descendant)
    {
        if (Consumer_->TransactionDescendants) {
            Consumer_->TransactionDescendants->Consume(NRecords::TTransactionDescendant{
                .Key = {.TransactionId = ancestor->GetId(), .DescendantId = descendant->GetId()},
                .Dummy = 0,
            });
        }
    }

    void WriteTransaction(
        const TTransaction* transaction,
        std::vector<TTransactionId>&& ancestorIds,
        std::vector<TTransactionId>&& prerequisiteTransactionIds)
    {
        if (Consumer_->Transactions) {
            TAttributeSet::TAttributeMap attributes;

            if (transaction->GetTitle()) {
                attributes["title"] = ConvertToYsonString(transaction->GetTitle());
            }

            if (transaction->GetAttributes()) {
                for (const auto& [attributeName, attributeValue] : transaction->GetAttributes()->Attributes()) {
                    if (ShouldMirrorTransactionAttributeToSequoia(attributeName)) {
                        attributes[attributeName] = attributeValue;
                    }
                }
            }

            Consumer_->Transactions->Consume(NRecords::TTransaction{
                .Key = {.TransactionId = transaction->GetId()},
                .AncestorIds = std::move(ancestorIds),
                .Attributes = ConvertToYsonString(attributes),
                .PrerequisiteTransactionIds = std::move(prerequisiteTransactionIds),
            });
        }
    }

    void WriteTransactionReplica(const TTransaction* transaction)
    {
        if (Consumer_->TransactionReplicas) {
            Consumer_->TransactionReplicas->Consume(NRecords::TTransactionReplica{
                .Key = {.TransactionId = transaction->GetId(), .CellTag = Bootstrap_->GetCellTag()},
                .Dummy = 0,
            });
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void ReconstructNodeTables(TBootstrap* bootstrap, TSequoiaReconstructorMapper& mapper)
{
    // TODO(grphil): Make unified with snapshot exporter for non single run map.

    const auto& cypressManager = bootstrap->GetCypressManager();
    for (auto [nodeId, node] : cypressManager->Nodes()) {
        if (!IsObjectAlive(node->GetTrunkNode())) {
            continue;
        }

        if (IsSequoiaId(nodeId.ObjectId)) {
            mapper.ProcessNode(node);
        }
    }
}

void ReconstructTransactionTables(TBootstrap* bootstrap, TSequoiaReconstructorMapper& mapper)
{
    const auto& cypressManager = bootstrap->GetTransactionManager();
    for (auto [transactionId, transaction] : cypressManager->Transactions()) {
        if (IsObjectAlive(transaction) &&
            transaction->GetIsCypressTransaction())
        {
            mapper.ProcessTransaction(transaction);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void ExecuteSequoiaReconstructorMapStage(
    TBootstrap *bootstrap,
    TRecordsConsumer *consumer)
{
    TSequoiaReconstructorMapper mapper(bootstrap, consumer);

    if (consumer->NodeForks ||
        consumer->NodeSnapshots ||
        consumer->NodeIdToPath ||
        consumer->ChildNode ||
        consumer->ChildForks ||
        consumer->PathToNodeId ||
        consumer->PathForks ||
        consumer->PathToNodeChanges ||
        consumer->Acls)
    {
        ReconstructNodeTables(bootstrap, mapper);
    }

    if (consumer->Transactions ||
        consumer->TransactionDescendants ||
        consumer->DependentTransactions ||
        consumer->TransactionReplicas)
    {
        ReconstructTransactionTables(bootstrap, mapper);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor
