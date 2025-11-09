#include "hydra_facade.h"
#include "private.h"
#include "sequoia_reconstructor.h"

#include <yt/yt/server/master/chunk_server/chunk_owner_base.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>
#include <yt/yt/server/master/cypress_server/helpers.h>
#include <yt/yt/server/master/cypress_server/link_node.h>
#include <yt/yt/server/master/cypress_server/node_detail.h>
#include <yt/yt/server/master/cypress_server/node.h>

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
#include <yt/yt/ytlib/sequoia_client/records/path_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transaction_descendants.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transaction_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transactions.record.h>

#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/public.h>

namespace NYT::NCellMaster {

using namespace NConcurrency;
using namespace NCypressServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSequoiaClient;
using namespace NSequoiaServer;
using namespace NTransactionServer;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CellMasterLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

DECLARE_REFCOUNTED_STRUCT(TTableOutputConfig);

struct TTableOutputConfig
    : public TYsonStruct
{
    std::string FileName;
    bool TextYsonOutputFormat;
    // TODO(grphil): Add other output options.

    REGISTER_YSON_STRUCT(TTableOutputConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("file_name", &TThis::FileName)
            .IsRequired();
        registrar.Parameter("text_yson_output_format", &TThis::TextYsonOutputFormat)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TTableOutputConfig);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSequoiaReconstructorConfig);

struct TSequoiaReconstructorConfig
    : public TYsonStruct
{
    bool SingleCell;

    std::optional<TTableOutputConfigPtr> NodeIdToPathOutput;
    std::optional<TTableOutputConfigPtr> NodeForksOutput;
    std::optional<TTableOutputConfigPtr> NodeSnapshotsOutput;

    std::optional<TTableOutputConfigPtr> ChildNodeOutput;
    std::optional<TTableOutputConfigPtr> ChildForksOutput;

    std::optional<TTableOutputConfigPtr> PathToNodeIdOutput;
    std::optional<TTableOutputConfigPtr> PathForksOutput;

    std::optional<TTableOutputConfigPtr> PathToNodeChangesOutput;
    std::optional<TTableOutputConfigPtr> IncompletePathToNodeIdOutput;

    std::optional<TTableOutputConfigPtr> AclsOutput;

    std::optional<TTableOutputConfigPtr> Transactions;
    std::optional<TTableOutputConfigPtr> TransactionDescendants;
    std::optional<TTableOutputConfigPtr> DependentTransactions;
    std::optional<TTableOutputConfigPtr> TransactionReplicas;

    REGISTER_YSON_STRUCT(TSequoiaReconstructorConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("single_cell", &TThis::SingleCell)
            .Default(false);

        registrar.Parameter("node_id_to_path_output", &TThis::NodeIdToPathOutput)
            .Default();

        registrar.Parameter("node_forks_output", &TThis::NodeForksOutput)
            .Default();

        registrar.Parameter("node_snapshots_output", &TThis::NodeSnapshotsOutput)
            .Default();

        registrar.Parameter("child_node_output", &TThis::ChildNodeOutput)
            .Default();

        registrar.Parameter("child_forks_output", &TThis::ChildForksOutput)
            .Default();

        registrar.Parameter("path_to_node_id_output", &TThis::PathToNodeIdOutput)
            .Default();

        registrar.Parameter("path_forks_output", &TThis::PathForksOutput)
            .Default();

        registrar.Parameter("path_to_node_changes_output", &TThis::PathToNodeChangesOutput)
            .Default();

        registrar.Parameter("incomplete_path_to_node_id_output", &TThis::IncompletePathToNodeIdOutput)
            .Default();

        registrar.Parameter("acls_output", &TThis::AclsOutput)
            .Default();

        registrar.Parameter("transactions_output", &TThis::Transactions)
            .Default();

        registrar.Parameter("transaction_descendants_output", &TThis::TransactionDescendants)
            .Default();

        registrar.Parameter("dependent_transactions_output", &TThis::DependentTransactions)
            .Default();

        registrar.Parameter("transaction_replicas_output", &TThis::TransactionReplicas)
            .Default();

        registrar.Postprocessor([] (TThis* config) {
            if (!config->SingleCell) {
                THROW_ERROR_EXCEPTION_IF(
                    config->PathToNodeIdOutput,
                    "Multicell sequoia reconstructor can not reconstruct \"path_to_node_id\" table. "
                    "Use \"path_to_node_changes\" output and reducer to correctly reconstruct sequoia."
                    "Some rows of \"path_to_node_id\" can be written to \"incomplete_path_to_node_id_info\".");
                THROW_ERROR_EXCEPTION_IF(
                    config->PathForksOutput,
                    "Multicell sequoia reconstructor can not reconstruct \"path_forks\" table. "
                    "Use \"path_to_node_changes\" output and reducer to correctly reconstruct sequoia.");
            } else {
                THROW_ERROR_EXCEPTION_IF(
                    config->IncompletePathToNodeIdOutput,
                    "Single cell sequoia reconstructor can not construct \"incomplete_path_to_node_id\" table. "
                    "It can reconstruct complete \"path_to_node_id\" table instead.");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TSequoiaReconstructorConfig);

////////////////////////////////////////////////////////////////////////////////

template <class TRecord>
struct IRecordWriter
{
    virtual ~IRecordWriter() = default;

    virtual void Write(const TRecord& record) = 0;
};

template <class TRecord>
class TFileWriter
    : public IRecordWriter<TRecord>
{
public:
    TFileWriter(TTableOutputConfigPtr config)
        : FileToWrite_(config->FileName, EOpenModeFlag::CreateNew | EOpenModeFlag::WrOnly | EOpenModeFlag::ARW)
        , OutputFile_(FileToWrite_)
        , Output_(std::make_unique<TYsonWriter>(
            &OutputFile_,
            config->TextYsonOutputFormat ? EYsonFormat::Text : EYsonFormat::Binary,
            EYsonType::Node))
    {
        Output_->OnBeginList();
    }

    ~TFileWriter() override
    {
        Output_->OnEndList();
    }

    void Write(const TRecord& record) override
    {
        Output_->OnListItem();
        if constexpr (
            requires {
                Serialize(record, Output_.get());
            })
        {
            Serialize(record, Output_.get());
        } else {
            auto rowBuffer = New<NTableClient::TRowBuffer>();
            Serialize(
                record.ToUnversionedRow(
                    rowBuffer,
                    TRecord::TRecordDescriptor::Get()->GetPartialIdMapping()),
                Output_.get());
        }
    }

private:
    TFile FileToWrite_;
    TFileOutput OutputFile_;
    std::unique_ptr<TYsonWriter> Output_;
};

template <class TRecord>
std::unique_ptr<IRecordWriter<TRecord>> CreateRecordWriter(std::optional<TTableOutputConfigPtr> config)
{
    if (config) {
        return std::make_unique<TFileWriter<TRecord>>(config.value());
    } else {
        return nullptr;
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TPathToNodeChangeRecord
    : public TYsonStructLite
{
    TMangledSequoiaPath Path;
    TNodeId NodeId = NullObjectId;
    TTransactionId TransactionId = NullTransactionId;
    std::vector<TTransactionId> TransactionAncestors;

    REGISTER_YSON_STRUCT_LITE(TPathToNodeChangeRecord);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("path", &TThis::Path);
        registrar.Parameter("node_id", &TThis::NodeId);
        registrar.Parameter("transaction_id", &TThis::TransactionId);
        registrar.Parameter("transaction_ancestors", &TThis::TransactionAncestors);
    }
};

class TPathToNodeChangesConsumer
    : public IRecordWriter<TPathToNodeChangeRecord>
{
public:
    TPathToNodeChangesConsumer(std::vector<TPathToNodeChangeRecord>* pathToNodeChanges)
        : PathToNodeChanges_(pathToNodeChanges)
    { }

    void Write(const TPathToNodeChangeRecord& pathToNodeChange) override
    {
        PathToNodeChanges_->push_back(pathToNodeChange);
    }

private:
    std::vector<TPathToNodeChangeRecord>* PathToNodeChanges_;
};

////////////////////////////////////////////////////////////////////////////////

void ReducePathToNodeChangeRecords(
    std::vector<TPathToNodeChangeRecord>&& records,
    IRecordWriter<NRecords::TPathToNodeId>* pathToNodeIdWriter,
    IRecordWriter<NRecords::TPathFork>* pathForksWriter)
{
    SortBy(
        records,
        [] (const auto& record) {
            return std::make_tuple(record.Path, record.TransactionAncestors.size());
        });

    // Do we have any group by function for this?
    for (size_t recordIndex = 0; recordIndex < records.size();) {
        THashMap<TTransactionId, const TPathToNodeChangeRecord*> transactionRecords;
        const auto& path = records[recordIndex].Path;

        // We view iterate over all records for the same path.
        // For them we need to do three actions:
        // 1. For every transaction keep only single record (multiple records may happen if node was replaced in transaction).
        // 2. Remove all tombstone records that have no node creation in ancestor transactions.
        // 3. Set ProgenitorTransactionId to the oldest transaction that has node created.
        for (; recordIndex < records.size() && records[recordIndex].Path == path; ++recordIndex) {
            const auto& record = records[recordIndex];

            if (auto existingRecordIt = transactionRecords.find(record.TransactionId); existingRecordIt != transactionRecords.end()) {
                // If we have multiple records for single transaction, we should keep the non tombstone one if it exists.
                if (record.NodeId != NullObjectId) {
                    // If we have non tombstone record for transaction, all other records should be tombstone.
                    YT_LOG_FATAL_IF(
                        existingRecordIt->second->NodeId != NullObjectId,
                        "Multiple node creation records for the same path with the same transaction (TransactionId: %v, Path: %v",
                        record.TransactionId,
                        path);

                    existingRecordIt->second = &record;
                }
            } else {
                if (record.NodeId == NullObjectId) {
                    // For tombstone records we should check that at least one record exists for ancestor transactions.
                    // All ancestor records should be already processed because records are sorted by transaction depth.

                    auto ancestorTransactionRecordFound = false;
                    for (const auto& ancestorTransactionId : record.TransactionAncestors) {
                        if (transactionRecords.contains(ancestorTransactionId)) {
                            ancestorTransactionRecordFound = true;
                            break;
                        }
                    }
                    if (!ancestorTransactionRecordFound) {
                        continue;
                    }
                }
                transactionRecords[record.TransactionId] = &record;
            }
        }

        for (const auto& [transactionId, record] : transactionRecords) {
            if (transactionId == NullTransactionId) {
                // We have already processed trunk nodes and written PathToNodeId rows for them during map stage.
                // We do not need PathForks for trunk nodes.
                continue;
            }
            if (pathToNodeIdWriter) {
                pathToNodeIdWriter->Write(NRecords::TPathToNodeId{
                    .Key = {.Path = path, .TransactionId = transactionId},
                    .NodeId = record->NodeId,
                });
            }
            if (pathForksWriter) {
                auto progenitorTransactionId = transactionId;
                // We iterate over transaction ancestors in order from topmost to current transaction searching for any record.
                for (const auto& ancestorTransactionId : record->TransactionAncestors | std::views::reverse) {
                    if (auto it = transactionRecords.find(ancestorTransactionId); it != transactionRecords.end()) {
                        YT_VERIFY(it->second->NodeId != NullObjectId);
                        progenitorTransactionId = ancestorTransactionId;
                        break;
                    }
                }
                pathForksWriter->Write(NRecords::TPathFork{
                    .Key = {.TransactionId = transactionId, .Path = path},
                    .NodeId = record->NodeId,
                    .ProgenitorTransactionId = progenitorTransactionId,
                });
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSequoiaReconstructor
{
public:
    TSequoiaReconstructor(TBootstrap* bootstrap, TSequoiaReconstructorConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(config)
        , NodeIdToPathWriter_(CreateRecordWriter<NRecords::TNodeIdToPath>(config->NodeIdToPathOutput))
        , NodeForksWriter_(CreateRecordWriter<NRecords::TNodeFork>(config->NodeForksOutput))
        , NodeSnapshotsWriter_(CreateRecordWriter<NRecords::TNodeSnapshot>(config->NodeSnapshotsOutput))
        , ChildNodeWriter_(CreateRecordWriter<NRecords::TChildNode>(config->ChildNodeOutput))
        , ChildForksWriter_(CreateRecordWriter<NRecords::TChildFork>(config->ChildForksOutput))
        , AclsWriter_(CreateRecordWriter<NRecords::TAcls>(config->AclsOutput))
        , TransactionsWriter_(CreateRecordWriter<NRecords::TTransaction>(config->Transactions))
        , TransactionDescendantsWriter_(CreateRecordWriter<NRecords::TTransactionDescendant>(config->TransactionDescendants))
        , DependentTransactionsWriter_(CreateRecordWriter<NRecords::TDependentTransaction>(config->DependentTransactions))
        , TransactionReplicasWriter_(CreateRecordWriter<NRecords::TTransactionReplica>(config->TransactionReplicas))
    {
        if (Config_->SingleCell) {
            YT_VERIFY(!Config_->IncompletePathToNodeIdOutput);

            if (Config_->PathToNodeIdOutput || Config_->PathForksOutput) {
                PathToNodeChangesWriter_ = std::make_unique<TPathToNodeChangesConsumer>(&PathToNodeChangeRecords_);
            }

            if (Config_->PathToNodeIdOutput) {
                PathToNodeIdWriter_ = CreateRecordWriter<NRecords::TPathToNodeId>(Config_->PathToNodeIdOutput);
            }
        } else {
            YT_VERIFY(!Config_->PathToNodeIdOutput && !Config_->PathForksOutput);

            if (Config_->PathToNodeChangesOutput) {
                PathToNodeChangesWriter_ = CreateRecordWriter<TPathToNodeChangeRecord>(Config_->PathToNodeChangesOutput);
            }

            if (Config_->IncompletePathToNodeIdOutput) {
                PathToNodeIdWriter_ = CreateRecordWriter<NRecords::TPathToNodeId>(Config_->IncompletePathToNodeIdOutput);
            }
        }
    }

    bool ShouldReconstructNodes() const
    {
        return Config_->NodeForksOutput ||
            Config_->NodeSnapshotsOutput ||
            Config_->NodeIdToPathOutput ||
            Config_->ChildNodeOutput ||
            Config_->ChildForksOutput ||
            Config_->PathToNodeIdOutput ||
            Config_->PathForksOutput ||
            Config_->PathToNodeChangesOutput ||
            Config_->IncompletePathToNodeIdOutput ||
            Config_->AclsOutput;
    }

    bool ShouldReconstructTransactions() const
    {
        return Config_->Transactions ||
            Config_->TransactionDescendants ||
            Config_->DependentTransactions ||
            Config_->TransactionReplicas;
    }

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

    void RunSingleCellReducer()
    {
        YT_VERIFY(Config_->SingleCell);
        auto pathForksWriter = CreateRecordWriter<NRecords::TPathFork>(Config_->PathForksOutput);
        if (PathToNodeChangeRecords_.empty()) {
            return;
        }
        ReducePathToNodeChangeRecords(
            std::move(PathToNodeChangeRecords_),
            PathToNodeIdWriter_.get(),
            pathForksWriter.get()
        );
    }

private:
    TBootstrap* Bootstrap_;
    TSequoiaReconstructorConfigPtr Config_;

    std::unique_ptr<IRecordWriter<NRecords::TNodeIdToPath>> NodeIdToPathWriter_;
    std::unique_ptr<IRecordWriter<NRecords::TNodeFork>> NodeForksWriter_;
    std::unique_ptr<IRecordWriter<NRecords::TNodeSnapshot>> NodeSnapshotsWriter_;

    std::unique_ptr<IRecordWriter<NRecords::TChildNode>> ChildNodeWriter_;
    std::unique_ptr<IRecordWriter<NRecords::TChildFork>> ChildForksWriter_;

    std::unique_ptr<IRecordWriter<NRecords::TAcls>> AclsWriter_;

    std::unique_ptr<IRecordWriter<NRecords::TTransaction>> TransactionsWriter_;
    std::unique_ptr<IRecordWriter<NRecords::TTransactionDescendant>> TransactionDescendantsWriter_;
    std::unique_ptr<IRecordWriter<NRecords::TDependentTransaction>> DependentTransactionsWriter_;
    std::unique_ptr<IRecordWriter<NRecords::TTransactionReplica>> TransactionReplicasWriter_;

    std::unique_ptr<IRecordWriter<TPathToNodeChangeRecord>> PathToNodeChangesWriter_;
    std::vector<TPathToNodeChangeRecord> PathToNodeChangeRecords_;
    // We can write path to node id records for trunk nodes even in multicell.
    // This will optimize the later reduces.
    std::unique_ptr<IRecordWriter<NRecords::TPathToNodeId>> PathToNodeIdWriter_;

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
        if (NodeIdToPathWriter_) {
            auto row = NRecords::TNodeIdToPath{
                .Key = {.NodeId = node->GetId(), .TransactionId = GetObjectId(node->GetTransaction())},
                .Path = node->ImmutableSequoiaProperties()->Path,
                .TargetPath = GetTargetPathOrEmpty(node),
                .ForkKind = forkKind,
            };

            NodeIdToPathWriter_->Write(row);
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
        if (NodeForksWriter_) {
            YT_VERIFY(node->GetTransaction());

            auto row = NRecords::TNodeFork{
                .Key = {.TransactionId = node->GetTransaction()->GetId(), .NodeId = node->GetId()},
                .Path = node->ImmutableSequoiaProperties()->Path,
                .TargetPath = GetTargetPathOrEmpty(node),
            };

            const auto* oldestReachableBranchingAncestor = GetOldestReachableBranchingAncestor(node);
            YT_VERIFY(oldestReachableBranchingAncestor);

            row.ProgenitorTransactionId = GetObjectId(oldestReachableBranchingAncestor->GetTransaction());

            NodeForksWriter_->Write(row);
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
        if (NodeSnapshotsWriter_) {
            NodeSnapshotsWriter_->Write(NRecords::TNodeSnapshot{
                .Key = {.TransactionId = node->GetTransaction()->GetId(), .NodeId = node->GetId()},
                .Dummy = 0,
            });
        }
    }

    void WritePathToNodeIdForTrunkNode(const TCypressNode* node)
    {
        YT_VERIFY(node->IsTrunk());
        YT_VERIFY(node->GetReachable());

        if (PathToNodeIdWriter_) {
            auto path = node->ImmutableSequoiaProperties()->Path;
            PathToNodeIdWriter_->Write(NRecords::TPathToNodeId{
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
        if (PathToNodeChangesWriter_) {
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

            PathToNodeChangesWriter_->Write(pathToNodeChange);
        }
    }

    void WriteAclsForTrunkNode(const TCypressNode* node)
    {
        YT_VERIFY(node->IsTrunk());

        if (AclsWriter_) {
            AclsWriter_->Write(NRecords::TAcls{
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
        if (ChildNodeWriter_) {
            auto row = NRecords::TChildNode{
                .Key = {.ParentId = parent->GetId(), .TransactionId = GetObjectId(parent->GetTransaction()), .ChildKey = key},
                .ChildId = childId,
            };

            ChildNodeWriter_->Write(row);
        }
    }

    void WriteChildFork(
        const TSequoiaMapNode* parent,
        const std::string& key,
        NCypressServer::TNodeId childId)
    {
        if (ChildForksWriter_) {
            auto childFork = NRecords::TChildFork{
                .Key = {.TransactionId = parent->GetTransaction()->GetId(), .ParentId = parent->GetId(), .ChildKey = key},
                .ChildId = childId,
            };

            const auto* oldestBranchingAncestorWithExistingChild = GetOldestBranchingAncestorWithExistingChild(parent, key);
            YT_VERIFY(oldestBranchingAncestorWithExistingChild);

            childFork.ProgenitorTransactionId = GetObjectId(oldestBranchingAncestorWithExistingChild->GetTransaction());

            ChildForksWriter_->Write(childFork);
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
        if (DependentTransactionsWriter_) {
            DependentTransactionsWriter_->Write(NRecords::TDependentTransaction{
                .Key = {.TransactionId = prerequisite->GetId(), .DependentTransactionId = dependent->GetId()},
                .Dummy = 0,
            });
        }
    }

    void WriteTransactionDescendant(TTransactionRawPtr ancestor, const TTransaction* descendant)
    {
        if (TransactionDescendantsWriter_) {
            TransactionDescendantsWriter_->Write(NRecords::TTransactionDescendant{
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
        if (TransactionsWriter_) {
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

            TransactionsWriter_->Write(NRecords::TTransaction{
                .Key = {.TransactionId = transaction->GetId()},
                .AncestorIds = std::move(ancestorIds),
                .Attributes = ConvertToYsonString(attributes),
                .PrerequisiteTransactionIds = std::move(prerequisiteTransactionIds),
            });
        }
    }

    void WriteTransactionReplica(const TTransaction* transaction)
    {
        if (TransactionReplicasWriter_) {
            TransactionReplicasWriter_->Write(NRecords::TTransactionReplica{
                .Key = {.TransactionId = transaction->GetId(), .CellTag = Bootstrap_->GetCellTag()},
                .Dummy = 0,
            });
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void ReconstructNodeTables(TBootstrap* bootstrap, TSequoiaReconstructor& sequoiaReconstructor)
{
    // TODO(grphil): Make unified with snapshot exporter.

    const auto& cypressManager = bootstrap->GetCypressManager();
    for (auto [nodeId, node] : cypressManager->Nodes()) {
        if (!IsObjectAlive(node->GetTrunkNode())) {
            continue;
        }

        if (IsSequoiaId(nodeId.ObjectId)) {
            sequoiaReconstructor.ProcessNode(node);
        }
    }
}

void ReconstructTransactionTables(TBootstrap* bootstrap, TSequoiaReconstructor& sequoiaReconstructor)
{
    const auto& cypressManager = bootstrap->GetTransactionManager();
    for (auto [transactionId, transaction] : cypressManager->Transactions()) {
        if (IsObjectAlive(transaction) &&
            transaction->GetIsCypressTransaction())
        {
            sequoiaReconstructor.ProcessTransaction(transaction);
        }
    }
}

void DoReconstructSequoia(TBootstrap* bootstrap, const TSequoiaReconstructorConfigPtr& config)
{
    TSequoiaReconstructor sequoiaReconstructor(bootstrap, config);

    if (sequoiaReconstructor.ShouldReconstructNodes()) {
        ReconstructNodeTables(bootstrap, sequoiaReconstructor);
    }

    if (sequoiaReconstructor.ShouldReconstructTransactions()) {
        ReconstructTransactionTables(bootstrap, sequoiaReconstructor);
    }

    if (config->SingleCell) {
        sequoiaReconstructor.RunSingleCellReducer();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void ReconstructSequoia(TBootstrap* bootstrap, const std::string& configPath)
{
    auto config = ConvertTo<TSequoiaReconstructorConfigPtr>(TYsonString(configPath));

    BIND(&DoReconstructSequoia, Unretained(bootstrap), config)
        .AsyncVia(bootstrap->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default))
        .Run()
        .Get()
        .ThrowOnError();
}

} // namespace NYT::NCellMaster
