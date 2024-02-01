#include "background_activity_orchid.h"

namespace NYT::NTabletNode {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TTaskInfoBase& task, IYsonConsumer* consumer)
{
    BuildYsonMapFragmentFluently(consumer)
        .Item("trace_id").Value(task.TaskId)
        .Item("task_id").Value(task.TaskId)
        .Item("tablet_id").Value(task.TabletId)
        .Item("mount_revision").Value(task.MountRevision)
        .Item("table_path").Value(task.TablePath)
        .Item("tablet_cell_bundle").Value(task.TabletCellBundle)
        .DoIf(static_cast<bool>(task.StartTime), [&] (auto fluent) {
            fluent
                .Item("start_time").Value(task.StartTime)
                .Item("duration").Value((task.FinishTime ? task.FinishTime : Now()) - task.StartTime);
        })
        .DoIf(static_cast<bool>(task.FinishTime), [&] (auto fluent) {
            fluent.Item("finish_time").Value(task.FinishTime);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
