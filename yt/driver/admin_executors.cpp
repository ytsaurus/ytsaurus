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
    : SetReadOnlyArg("", "set_read_only", "set the master to read only mode", false)
{
    CmdLine.add(SetReadOnlyArg);
}

EExitCode TBuildSnapshotExecutor::DoExecute()
{
    TMetaStateManagerProxy proxy(Driver->GetMasterChannel());
    proxy.SetDefaultTimeout(0); //infinity
    auto req = proxy.BuildSnapshot();
    req->set_set_read_only(SetReadOnlyArg.getValue());
    auto rsp = req->Invoke().Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error building snapshot");

    i32 snapshotId = rsp->snapshot_id();
    printf("Snapshot %d is built", snapshotId);

    return EExitCode::OK;
}

Stroka TBuildSnapshotExecutor::GetCommandName() const
{
    return "build_snapshot";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
