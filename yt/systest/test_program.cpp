
#include <library/cpp/yt/logging/logger.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>

#include <yt/systest/bootstrap_dataset.h>
#include <yt/systest/dataset_operation.h>
#include <yt/systest/map_dataset.h>
#include <yt/systest/reduce_dataset.h>

#include <yt/systest/operation.h>
#include <yt/systest/operation/map.h>
#include <yt/systest/operation/multi_map.h>
#include <yt/systest/operation/reduce.h>

#include <yt/systest/run.h>
#include <yt/systest/runner.h>
#include <yt/systest/sort_dataset.h>
#include <yt/systest/table_dataset.h>
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

    int NumOperations = 16;
    int Seed = 42;
    int opt;
    while ((opt = getopt(argc, argv, "bhn:")) != -1) {
        switch (opt) {
            case 'b':
                NumBootstrapRecords = atoi(optarg);
                break;
            case 'h':
                HomeDirectory = optarg;
                break;
            case 's':
                Seed = atoi(optarg);
                break;
            case 'n':
                NumOperations = atoi(optarg);
                break;
            default:
                printf("Usage: yttester [-b <num bootstrap records>]\n");
                exit(EXIT_FAILURE);
        }
    }
    auto client = NYT::CreateClientFromEnv();

    YT_LOG_INFO("Start systest (Seed: %v, NumOperations: %v)", Seed, NumOperations);
    YT_LOG_INFO("Created client (HomeDirectory: %v)", HomeDirectory);

    TTestHome testHome(client, HomeDirectory);
    testHome.Init();

    YT_LOG_INFO("Initialized test home (Directory: %v)", testHome.Dir());

    std::unique_ptr<IDataset> bootstrapDataset = std::make_unique<TBootstrapDataset>(NumBootstrapRecords);

    auto bootstrapPath = testHome.CreateRandomTablePath();
    YT_LOG_INFO("Will write bootstrap table (Path: %v)", bootstrapPath);

    auto bootstrapInfo = MaterializeIntoTable(client, bootstrapPath, *bootstrapDataset);

    Runner runner(client, NumOperations, Seed, &testHome, bootstrapInfo);
    runner.Run();
}

}  // namespace NYT::NTest
