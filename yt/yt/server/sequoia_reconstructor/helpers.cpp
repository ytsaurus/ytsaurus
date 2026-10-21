#include "helpers.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/library/fusion/service_directory.h>

#include <yt/yt/library/program/helpers.h>

#include <library/cpp/yson/node/node_builder.h>
#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/yt/system/exit.h>

namespace NYT::NSequoiaReconstructor {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

namespace {

TBootstrapPtr InitializeMasterBootstrap(
    const std::string& masterConfigPath,
    const std::string& snapshotPath)
{
    TIFStream stream(masterConfigPath);
    auto masterConfigNode = NYTree::ConvertToNode(&stream);

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

    NBus::NTcp::TDispatcher::Get()->DisableNetworking();

    bootstrap->Initialize();

    bootstrap->LoadSnapshot(
        TString(snapshotPath),
        /*dumpMode*/ ESerializationDumpMode::None,
        /*dumpScopeFilter*/ std::nullopt,
        /*checkInvariants*/ true);

    bootstrap->FinishRecoveryDryRun();

    return bootstrap;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TPathToNodeChangeRecord::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("node_id", &TThis::NodeId);
    registrar.Parameter("transaction_id", &TThis::TransactionId);
    registrar.Parameter("transaction_ancestors", &TThis::TransactionAncestors);
}

////////////////////////////////////////////////////////////////////////////////

TPathToNodeChangeRecord TPathToNodeChangeRecord::FromNode(const TNode& node)
{
    return ConvertTo<TPathToNodeChangeRecord>(NYson::TYsonString(NodeToYsonString(node)));
}

////////////////////////////////////////////////////////////////////////////////

TNode TPathToNodeChangeRecord::ToNode() const
{
    TNode result;
    TNodeBuilder builder(&result);
    Serialize(*this, &builder);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void RunWithBootstrap(
    const std::string& masterConfigPath,
    const std::string& snapshotPath,
    const std::function<void(NCellMaster::TBootstrap*)>& callback,
    bool abortProcessAfterFinish)
{
    auto bootstrap = InitializeMasterBootstrap(masterConfigPath, snapshotPath);

    BIND(callback, Unretained(bootstrap.get()))
        .AsyncVia(bootstrap->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default))
        .Run()
        .BlockingGet()
        .ThrowOnError();

    bootstrap->FinishDryRun();

    if (abortProcessAfterFinish) {
        AbortProcessSilently(EProcessExitCode::OK);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor
