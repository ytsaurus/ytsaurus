#include "etc_executors.h"
#include "preprocess.h"

#include <core/concurrency/scheduler.h>

#include <ytlib/driver/driver.h>

#include <ytlib/api/connection.h>
#include <ytlib/api/admin.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TBuildSnapshotExecutor::TBuildSnapshotExecutor()
    : CellIdArg("", "cell_id", "cell id where the snapshot must be built", false, NElection::TCellId(), "GUID")
    , SetReadOnlyArg("", "set_read_only", "set the master to read only mode", false)
{
    CmdLine.add(SetReadOnlyArg);
}

void TBuildSnapshotExecutor::DoExecute()
{
    auto admin = Driver->GetConnection()->CreateAdmin();
    TBuildSnapshotOptions options;
    options.CellId = CellIdArg.getValue();

    int snapshotId = WaitFor(admin->BuildSnapshot(options))
        .ValueOrThrow();

    printf("Snapshot %d is built\n", snapshotId);
}

Stroka TBuildSnapshotExecutor::GetCommandName() const
{
    return "build_snapshot";
}

////////////////////////////////////////////////////////////////////////////////

void TGCCollectExecutor::DoExecute()
{
    auto admin = Driver->GetConnection()->CreateAdmin();
    WaitFor(admin->GCCollect())
        .ThrowOnError();
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

void TUpdateMembershipExecutor::BuildParameters(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("group").Value(GroupArg.getValue())
        .Item("member").Value(MemberArg.getValue());
    TRequestExecutor::BuildParameters(consumer);
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

void TCheckPermissionExecutor::BuildParameters(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("user").Value(UserArg.getValue())
        .Item("permission").Value(PermissionArg.getValue())
        .Item("path").Value(path);

    TTransactedExecutor::BuildParameters(consumer);
}

Stroka TCheckPermissionExecutor::GetCommandName() const
{
    return "check_permission";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
