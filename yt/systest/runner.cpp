
#include <library/cpp/yt/logging/logger.h>

#include <yt/systest/bootstrap_dataset.h>
#include <yt/systest/dataset.h>
#include <yt/systest/dataset_operation.h>
#include <yt/systest/decorate_dataset.h>
#include <yt/systest/sort_dataset.h>
#include <yt/systest/reduce_dataset.h>
#include <yt/systest/run.h>

#include <yt/systest/operation/map.h>
#include <yt/systest/operation/multi_map.h>
#include <yt/systest/operation/reduce.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/systest/runner.h>

#include <unistd.h>

namespace NYT::NTest {

bool isNumeric(NProto::EColumnType type)
{
    return type == NProto::EColumnType::EInt8 ||
        type == NProto::EColumnType::EInt16 ||
        type == NProto::EColumnType::EInt64;
}

bool isLowCardinality(NProto::EColumnType type)
{
    return type == NProto::EColumnType::EInt8 ||
        type == NProto::EColumnType::EInt16;
}

static std::pair<std::vector<TString>, TString> GetSortAndReduceColumnsAndIndex(
    std::mt19937& randomEngine,
    const TTable& schema)
{
    std::uniform_int_distribution<int> dist(0, 5);
    std::vector<int> reduceable;
    std::vector<int> summable;
    for (int i = 0; i < std::ssize(schema.DataColumns); i++) {
        if (isNumeric(schema.DataColumns[i].Type)) {
            summable.push_back(i);
        }
        if (isLowCardinality(schema.DataColumns[i].Type)) {
            reduceable.push_back(i);
        }
    }

    std::shuffle(reduceable.begin(), reduceable.end(), randomEngine);
    std::shuffle(summable.begin(), summable.end(), randomEngine);

    YT_VERIFY(!reduceable.empty());
    YT_VERIFY(!summable.empty());

    std::vector<TString> reduceColumns;
    reduceColumns.push_back(schema.DataColumns[reduceable[0]].Name);
    if (reduceable.size() > 1 && dist(randomEngine) >= 3) {
        reduceColumns.push_back(schema.DataColumns[reduceable[1]].Name);
    }

    return std::make_pair(reduceColumns, schema.DataColumns[summable[0]].Name);
}

static std::vector<int> allIndicesExcept(int start, int limit, std::vector<int> excepted) {
    std::sort(excepted.begin(), excepted.end());
    std::vector<int> result;
    int index = 0;
    for (int i = start; i < limit; i++) {
        if (index < std::ssize(excepted) && i == excepted[index]) {
            ++index;
            continue;
        }
        result.push_back(i);
    }
    return result;
}

TRunnerConfig::TRunnerConfig()
{
}

TRunner::TRunner(
    const TString& pool,
    TRunnerConfig runnerConfig,
    IClientPtr client,
    NApi::IClientPtr rpcClient,
    TTestHome& testHome,
    TValidator& validator)
    : Logger("test")
    , Pool_(pool)
    , RunnerConfig_(runnerConfig)
    , Client_(client)
    , RpcClient_(rpcClient)
    , TestHome_(testHome)
    , Validator_(validator)
{
    YT_VERIFY(RunnerConfig_.EnableRenames || !RunnerConfig_.EnableDeletes);
}

void TRunner::Run()
{
    std::mt19937 RandomEngine(RunnerConfig_.Seed);

    YT_LOG_INFO("Running test runner");

    if (RunnerConfig_.EnableRenames) {
        YT_LOG_INFO("Set config nodes that enable column renames");
        RpcClient_->SetNode("//sys/@config/enable_table_column_renaming", NYson::TYsonString(TStringBuf("%true"))).Get().ThrowOnError();
    }

    if (RunnerConfig_.EnableDeletes) {
        YT_LOG_INFO("Set config nodes that enable column deletes");
        RpcClient_->SetNode("//sys/@config/enable_static_table_drop_column", NYson::TYsonString(TStringBuf("%true"))).Get().ThrowOnError();
    }

    std::unique_ptr<IDataset> bootstrapDataset = std::make_unique<TBootstrapDataset>(RunnerConfig_.NumBootstrapRecords);

    auto bootstrapPath = TestHome_.CreateRandomTablePath();
    YT_LOG_INFO("Will write bootstrap table (Path: %v)", bootstrapPath);

    auto bootstrapInfo = MaterializeIntoTable(Client_, bootstrapPath, *bootstrapDataset);

    Infos_.push_back(TDatasetInfo{
        bootstrapDataset.get(),
        bootstrapDataset.get(),
        bootstrapInfo
    });

    for (int i = 0; i < RunnerConfig_.NumOperations; i++) {
        YT_LOG_INFO("Run iteration (Current: %v Total: %v)", i, RunnerConfig_.NumOperations);

        const auto& currentInfo = Infos_.back();

        auto operation = GenerateMultipleColumns(currentInfo.Dataset->table_schema(), 4, RunnerConfig_.Seed);
        auto dataset = Map(*currentInfo.ShallowDataset, *operation);
        auto path = TestHome_.CreateRandomTablePath();

        RunMap(Client_,
               Pool_,
               TestHome_,
               currentInfo.Stored.Path,
               path,
               currentInfo.Dataset->table_schema(),
               dataset->table_schema(),
               *operation);

        auto shallowDataset = std::make_unique<TTableDataset>(dataset->table_schema(), Client_, path);
        TStoredDataset storedDataset = Validator_.VerifyMap(
            currentInfo.Stored.Path,
            path,
            currentInfo.Dataset->table_schema(),
            *operation);

        Infos_.push_back(TDatasetInfo{
            dataset.get(),
            shallowDataset.get(),
            storedDataset
        });

        OperationPtrs_.push_back(std::move(operation));
        DatasetPtrs_.push_back(std::move(dataset));
        DatasetPtrs_.push_back(std::move(shallowDataset));

        if (RunnerConfig_.EnableReduce) {
            RunSortAndReduce(RandomEngine, Infos_.back());
        }

        if (RunnerConfig_.EnableRenames) {

            if (RunnerConfig_.EnableDeletes) {
                RenameAndDeleteColumn(Infos_.back());
            } else {
                RenameColumn(Infos_.back());
            }

            if (RunnerConfig_.EnableReduce) {
                RunSortAndReduce(RandomEngine, Infos_.back());
            }
        }
    }
}

void TRunner::RenameColumn(const TDatasetInfo& info)
{
    const int renameIndex = 5;
    const int numIndices = 10;

    auto& dataset = *info.ShallowDataset;
    const auto& path = info.Stored.Path;

    std::vector<std::unique_ptr<IRowMapper>> columnOperations;
    columnOperations.push_back(std::make_unique<TIdentityRowMapper>(dataset.table_schema(), allIndicesExcept(0, 5, {})));
    columnOperations.push_back(std::make_unique<TRenameColumnRowMapper>(dataset.table_schema(), renameIndex, "Y" + std::to_string(renameIndex)));
    columnOperations.push_back(std::make_unique<TIdentityRowMapper>(dataset.table_schema(), allIndicesExcept(6, numIndices, {})));

    auto renameColumnOperation = std::make_unique<TSingleMultiMapper>(
        dataset.table_schema(), std::make_unique<TConcatenateColumnsRowMapper>(dataset.table_schema(), std::move(columnOperations)));

    auto renameColumnDataset = Map(dataset, *renameColumnOperation);

    auto identityOp = std::make_unique<TSingleMultiMapper>(dataset.table_schema(),
        std::make_unique<TIdentityRowMapper>(dataset.table_schema(), allIndicesExcept(0, numIndices, {})));
    auto forkedPath = TestHome_.CreateRandomTablePath();
    RunMap(Client_, Pool_, TestHome_, path, forkedPath, dataset.table_schema(), dataset.table_schema(),
        *identityOp);

    AlterTable(RpcClient_, forkedPath, renameColumnDataset->table_schema());
    auto alterShallowDataset = std::make_unique<TTableDataset>(renameColumnDataset->table_schema(),
        Client_, forkedPath);

    auto storedAlterDataset = VerifyTable(Client_, forkedPath, *renameColumnDataset);

    Infos_.push_back(TDatasetInfo{
        renameColumnDataset.get(),
        alterShallowDataset.get(),
        storedAlterDataset
    });

    OperationPtrs_.push_back(std::move(renameColumnOperation));
    OperationPtrs_.push_back(std::move(identityOp));
    DatasetPtrs_.push_back(std::move(renameColumnDataset));
    DatasetPtrs_.push_back(std::move(alterShallowDataset));
}

void TRunner::RenameAndDeleteColumn(const TDatasetInfo& info)
{
    const int deleteIndex = 3;
    const int renameIndex = 5;
    const int numIndices = 10;

    auto& dataset = *info.ShallowDataset;
    const auto& path = info.Stored.Path;

    std::vector<std::unique_ptr<IRowMapper>> columnOperations;
    columnOperations.push_back(std::make_unique<TDeleteColumnRowMapper>(dataset.table_schema(), deleteIndex));
    columnOperations.push_back(std::make_unique<TIdentityRowMapper>(dataset.table_schema(), allIndicesExcept(0, 5, {3})));
    columnOperations.push_back(std::make_unique<TRenameColumnRowMapper>(dataset.table_schema(), renameIndex, "Y" + std::to_string(renameIndex)));
    columnOperations.push_back(std::make_unique<TIdentityRowMapper>(dataset.table_schema(), allIndicesExcept(6, numIndices, {})));

    auto deleteColumnOperation = std::make_unique<TSingleMultiMapper>(
        dataset.table_schema(), std::make_unique<TConcatenateColumnsRowMapper>(dataset.table_schema(), std::move(columnOperations)));

    auto deleteColumnDataset = std::make_unique<TDecorateDataset>(
        Map(dataset, *deleteColumnOperation), std::vector<TString>{"X3"});

    auto identityOp = std::make_unique<TSingleMultiMapper>(dataset.table_schema(),
        std::make_unique<TIdentityRowMapper>(dataset.table_schema(), allIndicesExcept(0, numIndices, {})));
    auto forkedPath = TestHome_.CreateRandomTablePath();
    RunMap(Client_,
           Pool_,
           TestHome_,
           path,
           forkedPath,
           dataset.table_schema(),
           dataset.table_schema(),
           *identityOp);

    AlterTable(RpcClient_, forkedPath, deleteColumnDataset->table_schema());
    auto alterShallowDataset = std::make_unique<TTableDataset>(deleteColumnDataset->table_schema(),
        Client_, forkedPath);

    auto storedAlterDataset = VerifyTable(Client_, forkedPath, *deleteColumnDataset);

    Infos_.push_back(TDatasetInfo{
        deleteColumnDataset.get(),
        alterShallowDataset.get(),
        storedAlterDataset
    });

    OperationPtrs_.push_back(std::move(deleteColumnOperation));
    OperationPtrs_.push_back(std::move(identityOp));
    DatasetPtrs_.push_back(std::move(deleteColumnDataset));
    DatasetPtrs_.push_back(std::move(alterShallowDataset));
}

void TRunner::RunSortAndReduce(std::mt19937& randomEngine, const TDatasetInfo& info)
{
    std::vector<TString> reduceColumns;
    TString sumColumn;
    std::tie(reduceColumns, sumColumn) = GetSortAndReduceColumnsAndIndex(
        randomEngine, info.ShallowDataset->table_schema());

    RunSortAndReduce(info, reduceColumns, sumColumn);
}

void TRunner::RunSortAndReduce(const TDatasetInfo& info, const std::vector<TString>& columns, const TString& sumColumn)
{
    TString sortColumnsString;
    for (int i = 0; i < std::ssize(columns); ++i) {
        if (i > 0) {
            sortColumnsString += ",";
        }
        sortColumnsString += columns[i];
    }

    auto sortedPath = TestHome_.CreateRandomTablePath();
    YT_LOG_INFO("Performing sort (InputTable: %v, Columns: %v, OutputTable: %v)", info.Stored.Path, sortColumnsString, sortedPath);

    RunSort(Client_, Pool_, info.Stored.Path, sortedPath,
        TSortColumns(TVector<TString>(columns.begin(), columns.end())));

    TSortOperation sortOperation{columns};

    Validator_.VerifySort(info.Stored.Path, sortedPath, info.Dataset->table_schema(), sortOperation);

    auto sortedDataset = std::make_unique<TSortDataset>(*info.ShallowDataset, sortOperation);
    const auto& sortedSchema = sortedDataset->table_schema();

    int sumIndex = -1;
    for (int i = 0; i < std::ssize(sortedSchema.DataColumns); i++) {
        if (sortedSchema.DataColumns[i].Name == sumColumn) {
            sumIndex = i;
            break;
        }
    }

    YT_VERIFY(sumIndex >= 0);

    auto reducer = std::make_unique<TSumReducer>(sortedDataset->table_schema(), sumIndex,
        TDataColumn{"S", NProto::EColumnType::EInt64, std::nullopt});

    TReduceOperation reduceOperation{
        std::move(reducer),
        columns
    };

    auto reducePath = TestHome_.CreateRandomTablePath();

    YT_LOG_INFO("Performing reduce (InputTable: %v, SumColumn: %v, SumIndex: %v, Columns: %v, OutputTable: %v",
        sortedPath, sumColumn, sumIndex, sortColumnsString, reducePath);

    auto reduceDataset = std::make_unique<TReduceDataset>(*sortedDataset, reduceOperation);
    RunReduce(Client_,
              Pool_,
              TestHome_,
              sortedPath,
              reducePath,
              sortedDataset->table_schema(),
              reduceDataset->table_schema(),
              reduceOperation);

    Validator_.VerifyReduce(sortedPath, reducePath, sortedDataset->table_schema(), reduceOperation);
    VerifyTable(Client_, reducePath, *reduceDataset);
}

}  // namespace NYT::NTest
