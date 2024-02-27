
#include <library/cpp/yt/logging/logger.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>

#include <yt/systest/bootstrap_dataset.h>
#include <yt/systest/config.h>
#include <yt/systest/dataset_operation.h>
#include <yt/systest/map_dataset.h>
#include <yt/systest/reduce_dataset.h>
#include <yt/systest/validator.h>

#include <yt/systest/operation.h>
#include <yt/systest/operation/map.h>
#include <yt/systest/operation/multi_map.h>
#include <yt/systest/operation/reduce.h>

#include <yt/systest/run.h>
#include <yt/systest/runner.h>
#include <yt/systest/sort_dataset.h>
#include <yt/systest/table_dataset.h>
#include <yt/systest/test_home.h>
#include <yt/systest/test_spec.h>
#include <yt/systest/table.h>

#include <yt/systest/rpc_client.h>
#include <yt/systest/test_program.h>

#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

namespace NYT::NTest {

static void SetSysOptions(const TTestConfig& config, NApi::IClientPtr rpcClient)
{
    YT_VERIFY(config.EnableRenames || !config.EnableDeletes);

    NYT::NLogging::TLogger Logger("test");
    if (config.EnableRenames) {
        YT_LOG_INFO("Set config nodes that enable column renames");
        rpcClient->SetNode("//sys/@config/enable_table_column_renaming", NYson::TYsonString(TStringBuf("%true"))).Get().ThrowOnError();
    }

    if (config.EnableDeletes) {
        YT_LOG_INFO("Set config nodes that enable column deletes");
        rpcClient->SetNode("//sys/@config/enable_static_table_drop_column", NYson::TYsonString(TStringBuf("%true"))).Get().ThrowOnError();
    }
}

TProgram::TProgram()
{
    Config_.RegisterOptions(&Opts_);
}

void TProgram::DoRun(const NLastGetopt::TOptsParseResult&)
{
    NYT::Initialize();
    NYT::SetLogger(NYT::CreateStdErrLogger(NYT::ILogger::INFO));
    NYT::NLogging::TLogger Logger("test");

    Config_.Validate();
    YT_LOG_INFO("Starting tester");

    auto testSpec = GenerateSystestSpec(Config_.TestConfig);

    auto client = NYT::CreateClientFromEnv();
    auto rpcClient = CreateRpcClient(Config_.NetworkConfig);

    // TODO(orlovorlov) upload test spec to cypress.

    SetSysOptions(Config_.TestConfig, rpcClient);

    TTestHome testHome(client, Config_.HomeConfig);
    testHome.Init();

    TValidator validator(
        Config_.Pool,
        Config_.ValidatorConfig,
        client,
        rpcClient,
        testHome);

    YT_LOG_INFO("Starting validator");
    validator.Start();

    TRunner runner(
        Config_.Pool,
        testSpec,
        client,
        rpcClient,
        Config_.RunnerThreads,
        testHome,
        validator);

    runner.Run();

    validator.Stop();
}

}  // namespace NYT::NTest
