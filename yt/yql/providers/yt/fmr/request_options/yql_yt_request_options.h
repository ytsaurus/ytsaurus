#pragma once

#include <util/generic/string.h>
#include <vector>

namespace NYql {

enum class EOperationStatus {
    Accepted,
    InProgress,
    Failed,
    Completed,
    Aborted,
    NotFound
};

enum class ETaskStatus {
    Accepted,
    InProgress,
    Failed,
    Completed,
    Aborted
};

enum class ETaskType {
    Download,
    Upload,
    Merge
};

struct TStatistics {
};

struct TYtTableRef {
    TString Path;
    TString Cluster;
    TString TransactionId;
};

struct TFmrTableRef {
    TString TableId;
};

struct TTableRef {
    std::variant<TYtTableRef, TFmrTableRef> TableRef;
};

struct TUploadTaskParams {
    TFmrTableRef Input;
    TYtTableRef Output;
};

struct TDownloadTaskParams {
    TYtTableRef Input;
    TFmrTableRef Output;
};

struct TMergeTaskParams {
    std::vector<TTableRef> Input;
    TFmrTableRef Output;
};

using TTaskParams = std::variant<TUploadTaskParams, TDownloadTaskParams, TMergeTaskParams>;

struct TTask: public TThrRefBase {
    TTask(ETaskType taskType, const TString& taskId, const TTaskParams& taskParams, ui32 numRetries = 1)
        : TaskType(taskType), TaskId(taskId), TaskParams(taskParams), NumRetries(numRetries)
    {
    };

    ETaskType TaskType;
    TString TaskId;
    TTaskParams TaskParams;
    ui32 NumRetries; // Not supported yet

    using TPtr = TIntrusivePtr<TTask>;
};

struct TTaskState: public TThrRefBase {
    TTaskState(ETaskStatus taskStatus, const TString& taskId): TaskStatus(taskStatus), TaskId(taskId) {};

    ETaskStatus TaskStatus;
    TString TaskId;

    using TPtr = TIntrusivePtr<TTaskState>;
};

TTask::TPtr MakeTask(ETaskType taskType, const TString& taskId, const TTaskParams& taskParams);

TTaskState::TPtr MakeTaskState(ETaskStatus taskStatus, const TString& taskId);

} // namespace NYql
