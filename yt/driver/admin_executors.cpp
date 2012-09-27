#include "admin_executors.h"

#include <ytlib/driver/driver.h>
#include <ytlib/logging/log_manager.h>

#include <ytlib/meta_state/meta_state_manager_proxy.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

TBuildSnapshotExecutor::TBuildSnapshotExecutor()
    : ReadOnlyArg("", "read_only", "set the master to read only mode", false)
{
    CmdLine.add(ReadOnlyArg);
}

EExitCode TBuildSnapshotExecutor::Execute(const std::vector<std::string>& args)
{
    // TODO(babenko): get rid of this copy-paste
    auto argsCopy = args;
    CmdLine.parse(argsCopy);

    InitConfig();

    NLog::TLogManager::Get()->Configure(Config->Logging);

    Driver = CreateDriver(Config);

    TMetaStateManagerProxy proxy(Driver->GetMasterChannel());
    proxy.SetDefaultTimeout(0); //infinity
    auto req = proxy.BuildSnapshot();
    req->set_set_read_only(ReadOnlyArg.getValue());
    auto rsp = req->Invoke().Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error building snapshot");

    i32 snapshotId = rsp->snapshot_id();
    printf("SnapshotId = %d\n", snapshotId);
    return EExitCode::OK;
}

Stroka TBuildSnapshotExecutor::GetCommandName() const
{
    return "build_snapshot";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
