#include "record_consumer.h"

#include "helpers.h"

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

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

#include <yt/cpp/mapreduce/interface/io.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_builder.h>

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
    explicit TFileWriter(TFileOutputConfigPtr config)
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
    explicit TPathToNodeChangesConsumer(std::shared_ptr<std::vector<TPathToNodeChangeRecord>> pathToNodeChanges)
        : PathToNodeChanges_(std::move(pathToNodeChanges))
    { }

    void Consume(const TPathToNodeChangeRecord& pathToNodeChange) override
    {
        PathToNodeChanges_->push_back(pathToNodeChange);
    }

private:
    const std::shared_ptr<std::vector<TPathToNodeChangeRecord>> PathToNodeChanges_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TRecord>
class TTableRecordWriter
    : public IRecordConsumer<TRecord>
{
public:
    TTableRecordWriter(TTableWriter<TNode>* writer, int tableIndex)
        : Writer_(writer)
        , TableIndex_(tableIndex)
    { }

    void Consume(const TRecord& record) override
    {
        Writer_->AddRow(ConvertRecordToNode(record), TableIndex_);
    }

private:
    TTableWriter<TNode>* const Writer_;
    const int TableIndex_;

    TNode ConvertRecordToNode(const TRecord& record)
    {
        auto rowBuffer = New<TRowBuffer>();
        auto row = record.ToUnversionedRow(
            rowBuffer,
            TRecord::TRecordDescriptor::Get()->GetPartialIdMapping());

        TNode node;
        const auto& schema = TRecord::TRecordDescriptor::Get()->GetSchema();
        for (int columnIndex = 0; columnIndex < std::ssize(schema->Columns()); ++columnIndex) {
            TNode value;
            TNodeBuilder valueBuilder(&value);
            Serialize(row[columnIndex], &valueBuilder);
            node[schema->Columns()[columnIndex].Name()] = value;
        }
        return node;
    }
};

template <>
TNode TTableRecordWriter<TPathToNodeChangeRecord>::ConvertRecordToNode(const TPathToNodeChangeRecord& record)
{
    return record.ToNode();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TRecordsConsumer CreateSingleCellRecordsConsumer(TSingleCellSequoiaReconstructorConfigPtr config)
{
    TRecordsConsumer recordsConsumer;

    recordsConsumer.PathToNodeChangeRecords = std::make_shared<std::vector<TPathToNodeChangeRecord>>();

    for (const auto& [tableType, tableOutputConfig] : config->TablesOutput) {
        switch (tableType) {
            case EOutputTableType::NodeIdToPath:
                recordsConsumer.NodeIdToPath = std::make_unique<TFileWriter<NRecords::TNodeIdToPath>>(tableOutputConfig);
                break;
            case EOutputTableType::NodeForks:
                recordsConsumer.NodeForks = std::make_unique<TFileWriter<NRecords::TNodeFork>>(tableOutputConfig);
                break;
            case EOutputTableType::NodeSnapshots:
                recordsConsumer.NodeSnapshots = std::make_unique<TFileWriter<NRecords::TNodeSnapshot>>(tableOutputConfig);
                break;
            case EOutputTableType::ChildNodes:
                recordsConsumer.ChildNodes = std::make_unique<TFileWriter<NRecords::TChildNode>>(tableOutputConfig);
                break;
            case EOutputTableType::ChildForks:
                recordsConsumer.ChildForks = std::make_unique<TFileWriter<NRecords::TChildFork>>(tableOutputConfig);
                break;
            case EOutputTableType::Acls:
                recordsConsumer.Acls = std::make_unique<TFileWriter<NRecords::TAcls>>(tableOutputConfig);
                break;
            case EOutputTableType::Transactions:
                recordsConsumer.Transactions = std::make_unique<TFileWriter<NRecords::TTransaction>>(tableOutputConfig);
                break;
            case EOutputTableType::TransactionDescendants:
                recordsConsumer.TransactionDescendants = std::make_unique<TFileWriter<NRecords::TTransactionDescendant>>(tableOutputConfig);
                break;
            case EOutputTableType::DependentTransactions:
                recordsConsumer.DependentTransactions = std::make_unique<TFileWriter<NRecords::TDependentTransaction>>(tableOutputConfig);
                break;
            case EOutputTableType::TransactionReplicas:
                recordsConsumer.TransactionReplicas = std::make_unique<TFileWriter<NRecords::TTransactionReplica>>(tableOutputConfig);
                break;
            case EOutputTableType::PathToNodeId:
                recordsConsumer.PathToNodeId = std::make_unique<TFileWriter<NRecords::TPathToNodeId>>(tableOutputConfig);
                if (!recordsConsumer.PathToNodeChanges) {
                    recordsConsumer.PathToNodeChanges = std::make_unique<TPathToNodeChangesConsumer>(recordsConsumer.PathToNodeChangeRecords);
                }
                break;
            case EOutputTableType::PathForks:
                recordsConsumer.PathForks = std::make_unique<TFileWriter<NRecords::TPathFork>>(tableOutputConfig);
                if (!recordsConsumer.PathToNodeChanges) {
                    recordsConsumer.PathToNodeChanges = std::make_unique<TPathToNodeChangesConsumer>(recordsConsumer.PathToNodeChangeRecords);
                }
                break;
            default:
                THROW_ERROR_EXCEPTION("Unsupported table output type for single cell records consumer")
                    .With("table_type", tableType);
        }
    }

    return recordsConsumer;
}

////////////////////////////////////////////////////////////////////////////////

TRecordsConsumer CreateMapperRecordsConsumer(TYtMapJobConfigPtr config, TTableWriter<TNode>* tableWriter)
{
    TRecordsConsumer recordsConsumer;

    for (const auto& [tableType, tableIndex] : config->TableIndexes) {
        switch (tableType) {
            case EOutputTableType::NodeIdToPath:
                recordsConsumer.NodeIdToPath = std::make_unique<TTableRecordWriter<NRecords::TNodeIdToPath>>(tableWriter, tableIndex);
                break;
            case EOutputTableType::NodeForks:
                recordsConsumer.NodeForks = std::make_unique<TTableRecordWriter<NRecords::TNodeFork>>(tableWriter, tableIndex);
                break;
            case EOutputTableType::NodeSnapshots:
                recordsConsumer.NodeSnapshots = std::make_unique<TTableRecordWriter<NRecords::TNodeSnapshot>>(tableWriter, tableIndex);
                break;
            case EOutputTableType::ChildNodes:
                recordsConsumer.ChildNodes = std::make_unique<TTableRecordWriter<NRecords::TChildNode>>(tableWriter, tableIndex);
                break;
            case EOutputTableType::ChildForks:
                recordsConsumer.ChildForks = std::make_unique<TTableRecordWriter<NRecords::TChildFork>>(tableWriter, tableIndex);
                break;
            case EOutputTableType::Acls:
                recordsConsumer.Acls = std::make_unique<TTableRecordWriter<NRecords::TAcls>>(tableWriter, tableIndex);
                break;
            case EOutputTableType::Transactions:
                recordsConsumer.Transactions = std::make_unique<TTableRecordWriter<NRecords::TTransaction>>(tableWriter, tableIndex);
                break;
            case EOutputTableType::TransactionDescendants:
                recordsConsumer.TransactionDescendants = std::make_unique<TTableRecordWriter<NRecords::TTransactionDescendant>>(tableWriter, tableIndex);
                break;
            case EOutputTableType::DependentTransactions:
                recordsConsumer.DependentTransactions = std::make_unique<TTableRecordWriter<NRecords::TDependentTransaction>>(tableWriter, tableIndex);
                break;
            case EOutputTableType::TransactionReplicas:
                recordsConsumer.TransactionReplicas = std::make_unique<TTableRecordWriter<NRecords::TTransactionReplica>>(tableWriter, tableIndex);
                break;
            case EOutputTableType::PathToNodeId:
                recordsConsumer.PathToNodeId = std::make_unique<TTableRecordWriter<NRecords::TPathToNodeId>>(tableWriter, tableIndex);
                break;
            case EOutputTableType::PathToNodeChanges:
                recordsConsumer.PathToNodeChanges = std::make_unique<TTableRecordWriter<TPathToNodeChangeRecord>>(tableWriter, tableIndex);
                break;
            default:
                THROW_ERROR_EXCEPTION("Unsupported table output type for mapper records consumer")
                    .With("table_type", tableType);
        }
    }

    return recordsConsumer;
}

////////////////////////////////////////////////////////////////////////////////

TRecordsConsumer CreateReducerRecordsConsumer(
    TTableWriter<TNode>* tableWriter,
    std::optional<int> pathToNodeIdTableIndex,
    std::optional<int> pathForksTableIndex)
{
    TRecordsConsumer recordsConsumer;

    if (pathToNodeIdTableIndex) {
        recordsConsumer.PathToNodeId = std::make_unique<TTableRecordWriter<NRecords::TPathToNodeId>>(tableWriter, *pathToNodeIdTableIndex);
    }

    if (pathForksTableIndex) {
        recordsConsumer.PathForks = std::make_unique<TTableRecordWriter<NRecords::TPathFork>>(tableWriter, *pathForksTableIndex);
    }

    return recordsConsumer;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor
