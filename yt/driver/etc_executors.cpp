#include "etc_executors.h"
#include "preprocess.h"

#include <ytlib/driver/driver.h>

#include <core/logging/log_manager.h>

#include <ytlib/meta_state/meta_state_manager_proxy.h>

#include <ytlib/object_client/object_service_proxy.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NMetaState;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TBuildSnapshotExecutor::TBuildSnapshotExecutor()
    : SetReadOnlyArg("", "set_read_only", "set the master to read only mode", false)
{
    CmdLine.add(SetReadOnlyArg);
}

EExitCode TBuildSnapshotExecutor::DoExecute()
{
    TMetaStateManagerProxy proxy(Driver->GetMasterChannel());
    proxy.SetDefaultTimeout(Null); // infinity
    auto req = proxy.BuildSnapshot();
    req->set_set_read_only(SetReadOnlyArg.getValue());

    auto rsp = req->Invoke().Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error building snapshot");

    int snapshotId = rsp->snapshot_id();
    printf("Snapshot %d is built\n", snapshotId);

    return EExitCode::OK;
}

Stroka TBuildSnapshotExecutor::GetCommandName() const
{
    return "build_snapshot";
}

////////////////////////////////////////////////////////////////////////////////

TGCCollectExecutor::TGCCollectExecutor()
{ }

EExitCode TGCCollectExecutor::DoExecute()
{
    TObjectServiceProxy proxy(Driver->GetMasterChannel());
    proxy.SetDefaultTimeout(Null); // infinity
    auto req = proxy.GCCollect();
    auto rsp = req->Invoke().Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error collecting garbage");
    return EExitCode::OK;
}

Stroka TGCCollectExecutor::GetCommandName() const
{
    return "gc_collect";
}

////////////////////////////////////////////////////////////////////////////////

TUpdateMembershipExecutor::TUpdateMembershipExecutor()
    : MemberArg("member", "member name (either a group or a user)", true, "", "STRING")
    , GroupArg("group", "group name", true, "", "STRING")
{
    CmdLine.add(MemberArg);
    CmdLine.add(GroupArg);
}

void TUpdateMembershipExecutor::BuildArgs(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("group").Value(GroupArg.getValue())
        .Item("member").Value(MemberArg.getValue());
    TRequestExecutor::BuildArgs(consumer);
}

Stroka TAddMemberExecutor::GetCommandName() const 
{
    return "add_member";
}

Stroka TRemoveMemberExecutor::GetCommandName() const 
{
    return "remove_member";
}

////////////////////////////////////////////////////////////////////////////////

TCheckPermissionExecutor::TCheckPermissionExecutor()
    : TTransactedExecutor(false)
    , UserArg("check_user", "user to check against", true, "", "STRING")
    , PermissionArg("permission", "permission to check", true, EPermission(), "PERMISSION")
    , PathArg("path", "path to check", true, TRichYPath(""), "YPATH")
{
    CmdLine.add(UserArg);
    CmdLine.add(PermissionArg);
    CmdLine.add(PathArg);
}

void TCheckPermissionExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("user").Value(UserArg.getValue())
        .Item("permission").Value(PermissionArg.getValue())
        .Item("path").Value(path);

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TCheckPermissionExecutor::GetCommandName() const
{
    return "check_permission";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
