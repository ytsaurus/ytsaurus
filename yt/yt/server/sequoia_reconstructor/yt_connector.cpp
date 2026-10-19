#include "yt_connector.h"

#include "mapper.h"
#include "reducer.h"

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/comparator.h>

#include <yt/yt/ytlib/sequoia_client/records/acls.record.h>
#include <yt/yt/ytlib/sequoia_client/records/child_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/child_nodes.record.h>
#include <yt/yt/ytlib/sequoia_client/records/dependent_transactions.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_snapshots.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transaction_descendants.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transaction_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transactions.record.h>

#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/cpp/mapreduce/util/ypath_join.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/mapreduce/library/operation_tracker/operation_tracker.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYT::NSequoiaReconstructor {

using namespace NObjectClient;
using namespace NSequoiaClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = SequoiaReconstructorLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto CommandSuffix = " ytserver-sequoia-reconstructor";

IClientPtr CreateYtClient(TMulticellMapReduceSequoiaReconstructorConfigPtr config)
{
    if (config->RpcConnectionConfig) {
        THROW_ERROR_EXCEPTION("Config for rpc connection is not supported yet");
    }

    auto clientConfig = MakeIntrusive<NYT::TConfig>(*NYT::TConfig::Get());
    if (config->Pool) {
        clientConfig->Pool = *config->Pool;
    }

    return CreateClient(
        TString(config->ClusterName),
        TCreateClientOptions().Config(clientConfig));
}

////////////////////////////////////////////////////////////////////////////////

class TMapReduceSequoiaReconstructor
{
public:
    explicit TMapReduceSequoiaReconstructor(TMulticellMapReduceSequoiaReconstructorConfigPtr config)
        : Config_(std::move(config))
        , Client_(CreateYtClient(Config_))
    {
        for (const auto& cellConfig : Config_->Cells) {
            CellDirs_[cellConfig->CellTag] = JoinYPaths(Config_->WorkdirYPath, ToString(cellConfig->CellTag));
        }
    }

    void ReconstructSequoia()
    {
        YT_TLOG_INFO("Starting Sequoia reconstruction");

        Transaction_ = Client_->StartTransaction(
            TStartTransactionOptions()
                .Title(Format("Multicell sequoia reconstructor with workdir: %Qv", Config_->WorkdirYPath)));

        if (!Transaction_->Exists(Config_->WorkdirYPath)) {
            Transaction_->Create(Config_->WorkdirYPath, ENodeType::NT_MAP, TCreateOptions{
                .Recursive_ = true,
            });
        }

        RunMapOperations();

        StartMappedTablesSort();

        if (Config_->TablePaths.contains(EOutputTableType::PathToNodeId) ||
            Config_->TablePaths.contains(EOutputTableType::PathForks) ||
            Config_->TablePaths.contains(EOutputTableType::PathToNodeChanges))
        {
            RunReduce();
        }

        SortTracker_.WaitAllCompleted();

        Transaction_->Commit();
        Transaction_.Drop();
    }

private:
    const TMulticellMapReduceSequoiaReconstructorConfigPtr Config_;
    const IClientPtr Client_;
    ITransactionPtr Transaction_;
    THashMap<TCellTag, TYPath> CellDirs_;

    std::vector<EOutputTableType> MapperOutputTables_;
    THashMap<EOutputTableType, std::vector<TYPath>> TablesToSort_;

    TOperationTracker SortTracker_;

    NTableClient::TTableSchemaPtr GetTableClientSchema(EOutputTableType tableType)
    {
        switch (tableType) {
            case EOutputTableType::NodeIdToPath:
                return NRecords::TNodeIdToPathDescriptor::Get()->GetPrimaryTableSchema();
            case EOutputTableType::NodeForks:
                return NRecords::TNodeForkDescriptor::Get()->GetPrimaryTableSchema();
            case EOutputTableType::NodeSnapshots:
                return NRecords::TNodeSnapshotDescriptor::Get()->GetPrimaryTableSchema();
            case EOutputTableType::ChildNodes:
                return NRecords::TChildNodeDescriptor::Get()->GetPrimaryTableSchema();
            case EOutputTableType::ChildForks:
                return NRecords::TChildForkDescriptor::Get()->GetPrimaryTableSchema();
            case EOutputTableType::PathToNodeId:
                return NRecords::TPathToNodeIdDescriptor::Get()->GetPrimaryTableSchema();
            case EOutputTableType::PathForks:
                return NRecords::TPathForkDescriptor::Get()->GetPrimaryTableSchema();
            case EOutputTableType::Acls:
                return NRecords::TAclsDescriptor::Get()->GetPrimaryTableSchema();
            case EOutputTableType::Transactions:
                return NRecords::TTransactionDescriptor::Get()->GetPrimaryTableSchema();
            case EOutputTableType::TransactionDescendants:
                return NRecords::TTransactionDescendantDescriptor::Get()->GetPrimaryTableSchema();
            case EOutputTableType::DependentTransactions:
                return NRecords::TDependentTransactionDescriptor::Get()->GetPrimaryTableSchema();
            case EOutputTableType::TransactionReplicas:
                return NRecords::TTransactionReplicaDescriptor::Get()->GetPrimaryTableSchema();
            default:
                THROW_ERROR_EXCEPTION("Unsupported table type")
                    .With("table_type", tableType);
        }
    }

    TRichYPath CreateOutputWithSchema(TYPath path, EOutputTableType tableType, bool strict)
    {
        if (tableType == EOutputTableType::PathToNodeChanges) {
            return path;
        }
        auto tableClientSchema = GetTableClientSchema(tableType);

        if (!strict) {
            std::vector<NTableClient::TColumnSchema> columns;
            for (auto column : tableClientSchema->Columns()) {
                if (!column.Expression()) {
                    column.SetSortOrder(std::nullopt);
                    columns.push_back(column);
                }
            }
            tableClientSchema = New<NTableClient::TTableSchema>(columns);
        }

        // This code is a mess, but there are no simple conversions from server to client schema.
        auto schemaYson = NYson::ConvertToYsonString(tableClientSchema);
        auto clientSchema = TTableSchema::FromNode(NodeFromYsonString(schemaYson.ToString()));

        if (strict) {
            clientSchema.UniqueKeys(true);
        }

        return TRichYPath(path).Schema(clientSchema);
    }

    void RunMapOperations()
    {
        YT_TLOG_INFO("Starting map operations");

        THashMap<EOutputTableType, int> tableIndexes;

        int tableOutputIndex = 0;
        auto createTableOutput = [&] (EOutputTableType outputTableType) {
            if (!tableIndexes.contains(outputTableType)) {
                tableIndexes[outputTableType] = tableOutputIndex;
                MapperOutputTables_.push_back(outputTableType);
                ++tableOutputIndex;
            }
        };

        for (const auto& [tableType, tablePath] : Config_->TablePaths) {
            switch (tableType) {
                case EOutputTableType::PathToNodeId:
                    createTableOutput(tableType);
                    [[fallthrough]];
                case EOutputTableType::PathForks:
                case EOutputTableType::PathToNodeChanges:
                    createTableOutput(EOutputTableType::PathToNodeChanges);
                    break;
                default:
                    createTableOutput(tableType);
                    break;
            }
        }

        TOperationTracker tracker;
        for (const auto& cellConfig : Config_->Cells) {
            tracker.AddOperation(RunMapForCell(cellConfig, tableIndexes));
        }
        tracker.WaitAllCompleted();
    }

    IOperationPtr RunMapForCell(TCellConfigPtr cellConfig, const THashMap<EOutputTableType, int>& tableIndexes)
    {
        YT_TLOG_INFO("Preparing map operation")
            .With("CellTag", cellConfig->CellTag);

        const auto cellDir = GetOrCrash(CellDirs_, cellConfig->CellTag);
        Transaction_->Create(cellDir, ENodeType::NT_MAP, TCreateOptions{
            .Recursive_ = true,
            .Force_ = true,
        });

        auto jobConfigsTablePath = JoinYPaths(cellDir, "job_configs");
        Transaction_->Create(jobConfigsTablePath, ENodeType::NT_TABLE);
        auto writer = Transaction_->CreateTableWriter<TNode>(jobConfigsTablePath);

        for (int jobIndex = 0; jobIndex < Config_->MapperJobCount; ++jobIndex) {
            auto jobConfig = New<TYtMapJobConfig>();
            jobConfig->MasterConfigFile = "config";
            jobConfig->SnapshotFile = "snapshot";
            jobConfig->JobCount = Config_->MapperJobCount;
            jobConfig->JobIndex = jobIndex;
            jobConfig->TableIndexes = tableIndexes;

            auto configString = NYson::ConvertToYsonString(jobConfig);

            TNode row;
            row["config"] = configString.ToString();
            writer->AddRow(row);
        }
        writer->Finish();

        auto mapperSpec = TUserJobSpec()
            .AddFile(
                TRichYPath(cellConfig->ConfigYPath)
                    .FileName("config")
                    .Format("yson"))
            .AddFile(
                TRichYPath(cellConfig->SnapshotYPath)
                    .FileName("snapshot")
                    .BypassArtifactCache(true))
            .CpuLimit(Config_->MapperCpuLimit)
            .JobCommandSuffix(CommandSuffix)
            .JobTimeLimit(Config_->MapperTimeLimit)
            .MemoryLimit(Config_->MapperMemoryLimitGb * 1_GB);

        auto operationSpec = TMapOperationSpec()
            .MapperSpec(mapperSpec)
            .JobCount(Config_->MapperJobCount)
            .DataSizePerJob(1)
            .MaxFailedJobCount(Config_->MaxFailedJobCount)
            .Title(Format("Sequoia reconstructor mapper for cell %Qv", cellConfig->CellTag));

        for (auto mapperOutputTable : MapperOutputTables_) {
            auto tablePath = JoinYPaths(cellDir, ToString(mapperOutputTable));
            operationSpec.AddOutput<TNode>(
                CreateOutputWithSchema(tablePath, mapperOutputTable, /*strict*/ false));
            TablesToSort_[mapperOutputTable].push_back(tablePath);
        }

        operationSpec.AddInput<TNode>(jobConfigsTablePath);

        YT_TLOG_INFO("Starting map operation")
            .With("CellTag", cellConfig->CellTag);

        return Transaction_->Map(
            operationSpec,
            CreateYtMapper(),
            TOperationOptions().Wait(false));
    }

    IOperationPtr SortTable(EOutputTableType tableType, TYPath outputPath)
    {
        const auto& tablesToSort = GetOrCrash(TablesToSort_, tableType);

        TSortOperationSpec spec;
        for (const auto& table : tablesToSort) {
            spec.AddInput(table);
        }

        if (tableType == EOutputTableType::PathToNodeChanges) {
            spec.Output(outputPath);
            spec.SortBy("path");
        } else {
            auto schema = GetTableClientSchema(tableType);

            TSortColumns sortColumns;

            for (const auto& column : schema->Columns()) {
                if (column.SortOrder()) {
                    ESortOrder sortOrder;
                    switch (*column.SortOrder()) {
                        case NTableClient::ESortOrder::Ascending:
                            sortOrder = SO_ASCENDING;
                            break;
                        case NTableClient::ESortOrder::Descending:
                            sortOrder = SO_DESCENDING;
                            break;
                        default:
                            THROW_ERROR_EXCEPTION("Unsupported sort order")
                                .With("sort_order", *column.SortOrder());
                    }
                    sortColumns.Add(TSortColumn(column.Name(), sortOrder));
                }
            }
            spec.SortBy(sortColumns);

            spec.Output(CreateOutputWithSchema(outputPath, tableType, /*strict*/ true));
        }

        return Transaction_->Sort(spec, TOperationOptions().Wait(false));
    }

    void StartMappedTablesSort()
    {
        for (const auto& [tableType, tables] : TablesToSort_) {
            if (tableType != EOutputTableType::PathToNodeChanges &&
                tableType != EOutputTableType::PathToNodeId &&
                tableType != EOutputTableType::PathForks)
            {
                SortTracker_.AddOperation(SortTable(tableType, GetOrCrash(Config_->TablePaths, tableType)));
            }
        }
    }

    void RunReduce()
    {
        YT_VERIFY(TablesToSort_.contains(EOutputTableType::PathToNodeChanges));

        auto reduceWorkDir = JoinYPaths(Config_->WorkdirYPath, "reduce");
        Transaction_->Create(reduceWorkDir, ENodeType::NT_MAP, TCreateOptions{
            .Recursive_ = true,
            .Force_ = true,
        });

        TYPath pathToNodeChangesOutput;
        if (Config_->TablePaths.contains(EOutputTableType::PathToNodeChanges)) {
            pathToNodeChangesOutput = GetOrCrash(Config_->TablePaths, EOutputTableType::PathToNodeChanges);
        } else {
            pathToNodeChangesOutput = JoinYPaths(reduceWorkDir, ToString(EOutputTableType::PathToNodeChanges));
        }

        auto pathToNodeChangesSort = SortTable(EOutputTableType::PathToNodeChanges, pathToNodeChangesOutput);

        if (!Config_->TablePaths.contains(EOutputTableType::PathToNodeId) &&
            !Config_->TablePaths.contains(EOutputTableType::PathForks))
        {
            SortTracker_.AddOperation(pathToNodeChangesSort);
            return;
        }

        pathToNodeChangesSort->Watch().GetValueSync();

        TReduceOperationSpec spec;
        spec.ReduceBy("path");
        spec.AddInput<TNode>(pathToNodeChangesOutput);
        spec.ReducerSpec(TUserJobSpec().JobCommandSuffix(CommandSuffix));

        std::vector<std::function<IOperationPtr()>> sortProducers;
        int tableOutputIndex = 0;

        auto addTableOutput = [&] (EOutputTableType tableType) -> std::optional<int> {
            if (!Config_->TablePaths.contains(tableType)) {
                return std::nullopt;
            }
            auto tablePath = JoinYPaths(reduceWorkDir, ToString(tableType));

            spec.AddOutput<TNode>(CreateOutputWithSchema(tablePath, tableType, /*strict*/ false));
            TablesToSort_[tableType].push_back(tablePath);
            sortProducers.push_back([=, this] {
                return SortTable(tableType, GetOrCrash(Config_->TablePaths, tableType));
            });

            return std::make_optional(tableOutputIndex++);
        };

        auto pathToNodeIdTableIndex = addTableOutput(EOutputTableType::PathToNodeId);
        auto pathForksTableIndex = addTableOutput(EOutputTableType::PathForks);
        auto reducer = CreateYtReducer(pathToNodeIdTableIndex, pathForksTableIndex);

        Transaction_->Reduce(spec, reducer);

        for (const auto& sortProducer : sortProducers) {
            SortTracker_.AddOperation(sortProducer());
        }
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

void RunMapReduceReconstructor(TMulticellMapReduceSequoiaReconstructorConfigPtr config)
{
    TMapReduceSequoiaReconstructor reconstructor(std::move(config));
    reconstructor.ReconstructSequoia();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor
