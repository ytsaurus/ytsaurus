
#include <library/cpp/yt/logging/logger.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>

#include <yt/systest/bootstrap_dataset.h>

#include <yt/systest/operation.h>
#include <yt/systest/operation/map.h>
#include <yt/systest/operation/multi_map.h>
#include <yt/systest/operation/reduce.h>

#include <yt/systest/run.h>
#include <yt/systest/test_home.h>
#include <yt/systest/test_program.h>
#include <yt/systest/table.h>

namespace NYT::NTest {

void RunTestProgram(int argc, char* argv[]) {
    NYT::Initialize();
    NYT::SetLogger(NYT::CreateStdErrLogger(NYT::ILogger::INFO));
    NYT::NLogging::TLogger Logger("test");

    YT_LOG_INFO("Starting tester");

    int NumBootstrapRecords = 1000;
    TString HomeDirectory("//home");

    int opt;
    while ((opt = getopt(argc, argv, "bh:")) != -1) {
        switch (opt) {
            case 'b':
                NumBootstrapRecords = atoi(optarg);
                break;
            case 'h':
                HomeDirectory = optarg;
                break;
            default:
                printf("Usage: yttester [-b <num bootstrap records>]\n");
                exit(EXIT_FAILURE);
        }
    }

    auto client = NYT::CreateClientFromEnv();

    YT_LOG_INFO("Created client, home directory %v", HomeDirectory);

    TTestHome testHome(client, HomeDirectory);
    testHome.Init();

    YT_LOG_INFO("Initialized test home with directory %v", testHome.Dir());

    auto bootstrapDataset = std::make_unique<TBootstrapDataset>(NumBootstrapRecords);
    auto bootstrapPath = testHome.CreateRandomTablePath();
    auto tablePath = testHome.CreateRandomTablePath();
    auto secondTablePath = testHome.CreateRandomTablePath();

    YT_LOG_INFO("Will write bootstrap table %v", bootstrapPath);

    MaterializeIntoTable(client, bootstrapPath, *bootstrapDataset);

    const auto& bootstrapTable = bootstrapDataset->table_schema();

    std::vector<std::unique_ptr<IRowMapper>> singleOperations;
    singleOperations.push_back(std::make_unique<TSetSeedRowMapper>(bootstrapTable, 0));

    auto operation = std::make_unique<TCombineMultiMapper>(
        bootstrapTable,
        std::move(singleOperations),
        std::make_unique<TRepeatMultiMapper>(
            bootstrapTable,
            10,
            std::make_unique<TGenerateRandomRowMapper>(
                bootstrapTable,
                NProto::EColumnType::EInt64,
                TDataColumn{"X"})
        )
    );

    auto mappedDataset = Map(*bootstrapDataset, *operation);
    RunMap(client, testHome, bootstrapPath, tablePath, bootstrapTable, *operation);

    VerifyTable(client, tablePath, *mappedDataset);

    std::vector<std::unique_ptr<IRowMapper>> randomColumns;

    randomColumns.push_back(std::make_unique<TGenerateRandomRowMapper>(
                bootstrapTable,
                NProto::EColumnType::EInt8,
                TDataColumn{"X0"}));

    for (int i = 1; i < 10; i++) {
        auto type = i % 2 == 0 ? NProto::EColumnType::EInt64 : NProto::EColumnType::ELatinString100;

        TString columnName = "X" + std::to_string(i);

        randomColumns.push_back(std::make_unique<TGenerateRandomRowMapper>(
                bootstrapTable,
                type,
                TDataColumn{columnName}));
    }

    singleOperations.clear();
    singleOperations.push_back(std::make_unique<TSetSeedRowMapper>(mappedDataset->table_schema(), 0));

    auto operation2 = std::make_unique<TCombineMultiMapper>(
        mappedDataset->table_schema(),
        std::move(singleOperations),
        std::make_unique<TRepeatMultiMapper>(
            mappedDataset->table_schema(),
            10,
            std::make_unique<TConcatenateColumnsRowMapper>(mappedDataset->table_schema(), std::move(randomColumns))
        )
    );
    auto mappedDataset2 = Map(*mappedDataset, *operation2);
    RunMap(client, testHome, tablePath, secondTablePath, mappedDataset->table_schema(), *operation2);

    VerifyTable(client, secondTablePath, *mappedDataset2);

    auto operation3 = std::make_unique<TFilterMultiMapper>(mappedDataset2->table_schema(), 0, 100);

    auto thirdTablePath = testHome.CreateRandomTablePath();
    auto filteredDataset = Map(*mappedDataset2, *operation3);
    RunMap(client, testHome, secondTablePath, thirdTablePath, mappedDataset2->table_schema(), *operation3);

    VerifyTable(client, thirdTablePath, *filteredDataset);
}

}  // namespace NYT::NTest
