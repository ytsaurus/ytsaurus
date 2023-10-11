
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

static std::pair<std::vector<TString>, int> GetSortAndReduceColumnsAndIndex(int opIndex)
{
    std::vector<TString> columns;
    int index = 0;
    switch (opIndex % 3) {
        case 0:
            columns = {"X2", "X5"};
            index = 4;
            break;
        case 1:
            columns = {"X2", "X0"};
            index = 3;
            break;
        case 2:
            columns = {"X0"};
            index = 3;
            break;
    }

    return std::make_pair(columns, index);
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
    HomeDirectory = "//home";
    NumBootstrapRecords = 10000;
    Seed = 42;
    EnableRenamesDeletes = false;
    NumOperations = 4;
}

TRunner::TRunner(TRunnerConfig runnerConfig, IClientPtr client, NApi::IClientPtr rpcClient)
    : RunnerConfig_(runnerConfig)
    , Client_(client)
    , RpcClient_(rpcClient)
    , TestHome_(Client_, RunnerConfig_.HomeDirectory)
{
}

void TRunner::Run()
{
    NYT::NLogging::TLogger Logger("test");
    std::mt19937 RandomEngine(RunnerConfig_.Seed);

    if (RunnerConfig_.EnableRenamesDeletes) {
        EnableRenamesDeletes();
    }

    TestHome_.Init();
    YT_LOG_INFO("Initialized test home (Directory: %v)", TestHome_.Dir());

    std::unique_ptr<IDataset> bootstrapDataset = std::make_unique<TBootstrapDataset>(RunnerConfig_.NumBootstrapRecords);

    auto bootstrapPath = TestHome_.CreateRandomTablePath();
    YT_LOG_INFO("Will write bootstrap table (Path: %v)", bootstrapPath);

    auto bootstrapInfo = MaterializeIntoTable(Client_, bootstrapPath, *bootstrapDataset);

    Infos_.push_back(TDatasetInfo{
        bootstrapInfo.Dataset,
        bootstrapInfo.Dataset,
        bootstrapInfo
    });

    for (int i = 0; i < RunnerConfig_.NumOperations; i++) {
        const auto& currentInfo = Infos_.back();

        auto operation = GenerateMultipleColumns(currentInfo.Dataset->table_schema(), 4, RunnerConfig_.Seed);
        auto dataset = Map(*currentInfo.ShallowDataset, *operation);
        auto path = TestHome_.CreateRandomTablePath();

        RunMap(Client_, TestHome_, currentInfo.Stored.Path, path, currentInfo.Dataset->table_schema(), dataset->table_schema(), *operation);
        auto shallowDataset = std::make_unique<TTableDataset>(dataset->table_schema(), Client_, path);
        auto storedDataset = VerifyTable(Client_, path, *dataset);

        Infos_.push_back(TDatasetInfo{
            dataset.get(),
            shallowDataset.get(),
            storedDataset
        });

        OperationPtrs_.push_back(std::move(operation));
        DatasetPtrs_.push_back(std::move(dataset));
        DatasetPtrs_.push_back(std::move(shallowDataset));

        std::vector<TString> reduceColumns;
        int sumIndex;
        std::tie(reduceColumns, sumIndex) = GetSortAndReduceColumnsAndIndex(i + 1);

        RunSortAndReduce(Infos_.back(), reduceColumns, sumIndex);

        if (RunnerConfig_.EnableRenamesDeletes) {
            RenameAndDeleteColumn(Infos_.back());
        }
    }
}

void TRunner::EnableRenamesDeletes()
{
    RpcClient_->SetNode("//sys/@config/enable_table_column_renaming", NYson::TYsonString(TStringBuf("%true"))).Get().ThrowOnError();
    RpcClient_->SetNode("//sys/@config/enable_static_table_drop_column", NYson::TYsonString(TStringBuf("%true"))).Get().ThrowOnError();
}

void TRunner::RenameAndDeleteColumn(const TDatasetInfo& info)
{
    const int deleteIndex = 3;
    const int renameIndex = 5;

    auto& dataset = *info.ShallowDataset;
    const auto& path = info.Stored.Path;

    std::vector<std::unique_ptr<IRowMapper>> columnOperations;
    columnOperations.push_back(std::make_unique<TDeleteColumnRowMapper>(dataset.table_schema(), deleteIndex));
    columnOperations.push_back(std::make_unique<TIdentityRowMapper>(dataset.table_schema(), allIndicesExcept(0, 5, {3})));
    columnOperations.push_back(std::make_unique<TRenameColumnRowMapper>(dataset.table_schema(), renameIndex, "Y" + std::to_string(renameIndex)));
    columnOperations.push_back(std::make_unique<TIdentityRowMapper>(dataset.table_schema(), allIndicesExcept(6, 10, {})));

    auto deleteColumnOperation = std::make_unique<TSingleMultiMapper>(
        dataset.table_schema(), std::make_unique<TConcatenateColumnsRowMapper>(dataset.table_schema(), std::move(columnOperations)));

    auto deleteColumnDataset = std::make_unique<TDecorateDataset>(
        Map(dataset, *deleteColumnOperation), std::vector<TString>{"X3"});

    AlterTable(RpcClient_, path, deleteColumnDataset->table_schema());
    VerifyTable(Client_, path, *deleteColumnDataset);
}

void TRunner::RunSortAndReduce(const TDatasetInfo& info, const std::vector<TString>& columns, int sumIndex)
{
    NYT::NLogging::TLogger Logger("test");

    auto sortedPath = TestHome_.CreateRandomTablePath();
    YT_LOG_INFO("Performing sort (InputTable: %v, OutputTable: %v)", info.Stored.Path, sortedPath);

    RunSort(Client_, info.Stored.Path, sortedPath,
        TSortColumns(TVector<TString>(columns.begin(), columns.end())));

    auto sortedDataset = std::make_unique<TSortDataset>(*info.ShallowDataset, TSortOperation{columns});
    auto reducer = std::make_unique<TSumReducer>(sortedDataset->table_schema(), sumIndex,
        TDataColumn{"S", NProto::EColumnType::EInt64, std::nullopt});

    TReduceOperation reduceOperation{
        std::move(reducer),
        columns
    };

    auto reducePath = TestHome_.CreateRandomTablePath();

    YT_LOG_INFO("Performing reduce (InputTable: %v, OutputTable: %v, ReduceIndex: %v)",
        sortedPath, reducePath, sumIndex);

    auto reduceDataset = std::make_unique<TReduceDataset>(*sortedDataset, reduceOperation);
    RunReduce(Client_, TestHome_, sortedPath, reducePath, sortedDataset->table_schema(), reduceDataset->table_schema(), reduceOperation);

    VerifyTable(Client_, reducePath, *reduceDataset);
}

}  // namespace NYT::NTest
