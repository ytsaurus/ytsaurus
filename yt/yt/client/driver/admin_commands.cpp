#include "admin_commands.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NDriver {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TBuildSnapshotCommand::TBuildSnapshotCommand()
{
    RegisterParameter("cell_id", Options.CellId);
    RegisterParameter("set_read_only", Options.SetReadOnly)
        .Optional();
    RegisterParameter("wait_for_snapshot_completion", Options.WaitForSnapshotCompletion)
        .Optional();
}

void TBuildSnapshotCommand::DoExecute(ICommandContextPtr context)
{
    auto snapshotId = WaitFor(context->GetClient()->BuildSnapshot(Options))
        .ValueOrThrow();
    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("snapshot_id").Value(snapshotId)
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

TBuildMasterSnapshotsCommand::TBuildMasterSnapshotsCommand()
{
    RegisterParameter("set_read_only", Options.SetReadOnly)
        .Optional();
    RegisterParameter("wait_for_snapshot_completion", Options.WaitForSnapshotCompletion)
        .Optional();
    RegisterParameter("retry", Options.Retry)
        .Optional();
}

void TBuildMasterSnapshotsCommand::DoExecute(ICommandContextPtr context)
{
    auto cellIdToSnapshotId = WaitFor(context->GetClient()->BuildMasterSnapshots(Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .DoListFor(cellIdToSnapshotId, [=] (TFluentList fluent, const auto& pair) {
            fluent
                .Item().BeginMap()
                    .Item("cell_id").Value(pair.first)
                    .Item("snapshot_id").Value(pair.second)
                .EndMap();
        })
    );
}

////////////////////////////////////////////////////////////////////////////////

TSwitchLeaderCommand::TSwitchLeaderCommand()
{
    RegisterParameter("cell_id", CellId_);
    RegisterParameter("new_leader_address", NewLeaderAddress_);
}

void TSwitchLeaderCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SwitchLeader(CellId_, NewLeaderAddress_))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TRepairExecNodeCommand::TRepairExecNodeCommand()
{
    RegisterParameter("address", Address_);
    RegisterParameter("locations", Options.Locations);
}

void TRepairExecNodeCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->RepairExecNode(Address_, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
