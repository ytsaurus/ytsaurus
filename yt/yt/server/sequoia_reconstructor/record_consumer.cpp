#include "record_consumer.h"

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/ytlib/sequoia_client/records/acls.record.h>
#include <yt/yt/ytlib/sequoia_client/records/child_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/child_nodes.record.h>
#include <yt/yt/ytlib/sequoia_client/records/dependent_transactions.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_snapshots.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transaction_descendants.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transaction_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transactions.record.h>

namespace NYT::NSequoiaReconstructor {

using namespace NYson;
using namespace NSequoiaClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TRecord>
class TFileWriter
    : public IRecordConsumer<TRecord>
{
public:
    explicit TFileWriter(TTableOutputConfigPtr config)
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

    void Consume(const TRecord& record) override
    {
        Output_->OnListItem();
        if constexpr (
            requires {
                Serialize(record, Output_.get());
            })
        {
            Serialize(record, Output_.get());
        } else {
            auto rowBuffer = New<TRowBuffer>();
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
    const std::unique_ptr<TYsonWriter> Output_;
};

////////////////////////////////////////////////////////////////////////////////

class TPathToNodeChangesConsumer
    : public IRecordConsumer<TPathToNodeChangeRecord>
{
public:
    explicit TPathToNodeChangesConsumer(std::vector<TPathToNodeChangeRecord>* pathToNodeChanges)
        : PathToNodeChanges_(pathToNodeChanges)
    { }

    void Consume(const TPathToNodeChangeRecord& pathToNodeChange) override
    {
        PathToNodeChanges_->push_back(pathToNodeChange);
    }

private:
    std::vector<TPathToNodeChangeRecord>* const PathToNodeChanges_;
};

////////////////////////////////////////////////////////////////////////////////

// TODO(grphil): Add table output option for map and reduce stages.
template <class TRecord>
std::unique_ptr<IRecordConsumer<TRecord>> CreateRecordWriter(std::optional<TTableOutputConfigPtr> config)
{
    if (config) {
        return std::make_unique<TFileWriter<TRecord>>(config.value());
    } else {
        return nullptr;
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TRecordsConsumer::TRecordsConsumer(TSequoiaReconstructorConfigPtr config)
    : NodeIdToPath(CreateRecordWriter<NRecords::TNodeIdToPath>(config->NodeIdToPathOutput))
    , NodeForks(CreateRecordWriter<NRecords::TNodeFork>(config->NodeForksOutput))
    , NodeSnapshots(CreateRecordWriter<NRecords::TNodeSnapshot>(config->NodeSnapshotsOutput))
    , ChildNodes(CreateRecordWriter<NRecords::TChildNode>(config->ChildNodesOutput))
    , ChildForks(CreateRecordWriter<NRecords::TChildFork>(config->ChildForksOutput))
    , Acls(CreateRecordWriter<NRecords::TAcls>(config->AclsOutput))
    , Transactions(CreateRecordWriter<NRecords::TTransaction>(config->TransactionsOutput))
    , TransactionDescendants(CreateRecordWriter<NRecords::TTransactionDescendant>(config->TransactionDescendantsOutput))
    , DependentTransactions(CreateRecordWriter<NRecords::TDependentTransaction>(config->DependentTransactionsOutput))
    , TransactionReplicas(CreateRecordWriter<NRecords::TTransactionReplica>(config->TransactionReplicasOutput))
    , PathToNodeId(CreateRecordWriter<NRecords::TPathToNodeId>(config->PathToNodeIdOutput))
    , PathForks(CreateRecordWriter<NRecords::TPathFork>(config->PathForksOutput))
{
    if (config->SingleCellSingleRun) {
        if (PathToNodeId || PathForks) {
            PathToNodeChanges = std::make_unique<TPathToNodeChangesConsumer>(&PathToNodeChangeRecords);
        }
    } else {
        PathToNodeChanges = CreateRecordWriter<TPathToNodeChangeRecord>(config->PathToNodeChangesOutput);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor
