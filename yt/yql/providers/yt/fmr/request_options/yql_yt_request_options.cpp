#include "yql_yt_request_options.h"

namespace NYql {

TTask::TPtr MakeTask(ETaskType taskType, const TString& taskId, const TTaskParams& taskParams) {
    return MakeIntrusive<TTask>(taskType, taskId, taskParams);
}

TTaskState::TPtr MakeTaskState(ETaskStatus taskStatus, const TString& taskId) {
    return MakeIntrusive<TTaskState>(taskStatus, taskId);
}

} // namepsace NYql
