#include "sequoia_reconstructor.h"

#include "mapper.h"
#include "record_consumer.h"
#include "reducer.h"

#include <library/cpp/yt/system/exit.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/library/program/helpers.h>

#include <yt/yt/library/fusion/service_directory.h>

namespace NYT::NSequoiaReconstructor {

using namespace NCellMaster;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

TBootstrapPtr InitializeMasterBootstrap(
    const std::string& masterConfigPath,
    const std::string& snapshotPath)
{
    TIFStream stream(masterConfigPath);
    auto masterConfigNode = ConvertToNode(&stream);

    auto masterConfig = New<TCellMasterProgramConfig>();
    masterConfig->SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);
    masterConfig->Load(masterConfigNode);

    ConfigureSingletons(masterConfig);

    TweakConfigForDryRun(masterConfig, /*skipTvmServiceEnvValidation*/ false);
    masterConfig->SetSingletonConfig(NLogging::TLogManagerConfig::CreateQuiet());

    auto bootstrap = CreateMasterBootstrap(
        masterConfig,
        masterConfigNode,
        NFusion::CreateServiceDirectory());
    DoNotOptimizeAway(bootstrap);

    NBus::TTcpDispatcher::Get()->DisableNetworking();

    bootstrap->Initialize();

    bootstrap->LoadSnapshot(
        TString(snapshotPath),
        /*dumpMode*/ ESerializationDumpMode::None,
        /*dumpScopeFilter*/ std::nullopt,
        /*checkInvariants*/ true);

    bootstrap->FinishRecoveryDryRun();

    return bootstrap;
}

void FinishRun(TBootstrapPtr bootstrap)
{
    bootstrap->FinishDryRun();
    AbortProcessSilently(EProcessExitCode::OK);
}

////////////////////////////////////////////////////////////////////////////////

void DoReconstructSequoia(TBootstrap* bootstrap, const TSequoiaReconstructorConfigPtr& config)
{
    TRecordsConsumer consumer(config);

    // TODO(grphil): Adapt for map and reduce
    if (!config->SingleCellSingleRun) {
        THROW_ERROR_EXCEPTION("separate map and reduce are not supported yet");
    }

    ExecuteSequoiaReconstructorMapStage(bootstrap, &consumer);

    ExecutePathToNodeReduce(consumer.PathToNodeChangeRecords, &consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void ReconstructSequoia(
    const TSequoiaReconstructorConfigPtr reconstructorConfig,
    const std::string &masterConfigPath,
    const std::string &snapshotPath)
{
    auto bootstrap = InitializeMasterBootstrap(masterConfigPath, snapshotPath);

    BIND(&DoReconstructSequoia, Unretained(bootstrap.get()), reconstructorConfig)
        .AsyncVia(bootstrap->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default))
        .Run()
        .Get()
        .ThrowOnError();

    FinishRun(bootstrap);
}

} // namespace NYT::NSequoiaReconstructor
