#include "sql_library_meta_task.h"

#include <yql/essentials/utils/log/context.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/profile.h>

#include <yql/essentials/core/yql_library_compiler.h>
#include <yql/essentials/core/yql_user_data.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#include <yql/tools/yqlworker/interface/proto/task.pb.h>
#include <yql/tools/yqlworker/proto/sql_library_resolve_result.pb.h>
#include <yql/essentials/utils/signals/utils.h>
#include <yql/essentials/utils/debug_info.h>

using namespace NKikimr;
using namespace NMiniKQL;

namespace NYql {
struct TSqlLibraryMetaTask final : public IYqlRunnableTask {

    const TTaskDataPtr TaskData;
    const TTaskResultPtr TaskResult;
    const ITaskWatcherPtr TaskWatcher;
    const IMetricsRegistryPtr Metrics;
    const IModuleResolver::TPtr ModuleResolver;

    TSqlLibraryMetaTask(
        TTaskDataPtr taskData,
        ITaskWatcherPtr taskWatcher,
        IMetricsRegistryPtr metrics,
        IModuleResolver::TPtr moduleResolver)
        : TaskData(std::move(taskData))
        , TaskResult(new NProto::TTaskResult)
        , TaskWatcher(std::move(taskWatcher))
        , Metrics(std::move(metrics))
        , ModuleResolver(std::move(moduleResolver))
    {
        TaskWatcher->OnStart();
        SetTaskStatus(NProto::ETaskStatus::PENDING);
    }

    const NProto::TTaskResult& GetResult() const override {
        return *TaskResult;
    }

    ITaskWatcherPtr GetWatcher() const override {
        return TaskWatcher;
    }

    bool DoRun() override {
        const TString& taskId = TaskData->GetId();
        SetCurrentOperationId(taskId.data());
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(taskId);
        YQL_PROFILE_SCOPE(TRACE, "WholeOperation");

        NProto::ETaskStatus finishStatus = NProto::ETaskStatus::UNKNOWN_STATUS;
        try {
            YQL_LOG(INFO) << "Start processing task: " << TaskData->GetId();
            Metrics->IncCounter("action", NProto::ETaskAction_Name(NProto::ETaskAction::RUN));
            Metrics->IncCounter("syntax", NProto::ESyntax_Name(TaskData->GetSyntax()));

            DoFetchLibraryMeta();
            finishStatus = FinishedWithComplete();
        } catch (...) {
            TString errMsg = CurrentExceptionMessage();
            YQL_LOG(ERROR) << "Error while processing task " << TaskData->GetId() << ": " << errMsg;

            finishStatus = NProto::ETaskStatus::ERROR;
            TaskResult->SetStatus(finishStatus);
            TaskResult->SetRevision(TaskResult->GetRevision() + 1);
            AddFatalMessageToTaskResult(TaskResult.get(), errMsg);

            TaskWatcher->OnResultChange(TTaskResultChanges::MAX_VALUE, TaskResult);
        }

        Metrics->IncCounter("completion_status", NProto::ETaskStatus_Name(finishStatus));
        Metrics->Flush();

        TaskWatcher->OnFinish(finishStatus);
        return true;
    }

    void DoAbort() override {
        // not implemented
    }

private:
    void SetTaskStatus(NProto::ETaskStatus status) {
        TaskResult->SetStatus(status);
        TaskResult->SetRevision(TaskResult->GetRevision() + 1);

        TaskWatcher->OnResultChange(TTaskResultChanges::MAX_VALUE, TaskResult);
    }

    void DoFetchLibraryMeta() {
        TUserDataTable userData;

        if (TaskData->FilesSize() == 0) {
            ythrow yexception() << "No files";
        }

        NProto::TSqlLibraryResolveResult resultMsg;

        TExprContext librariesContext;
        const auto& files = TaskData->GetFiles();
        for (auto& f : files) {
            if (f.GetType() != NProto::TTaskFile::CONTENT) {
                ythrow yexception() << "Bad file type " << (int)f.GetType();
            }

            if (!f.GetName().EndsWith(".sql") && !f.GetName().EndsWith(".yql")) {
                ythrow yexception() << "File extension should be .yql or .sql, but have name " << f.GetName();
            }

            auto& entry = userData[TUserDataKey::File(f.GetName())];
            entry.Type = EUserDataType::RAW_INLINE_DATA;
            entry.Data = f.GetContent();
            entry.Usage.Set(EUserDataBlockUsage::Library, true);

            auto moduleMsg = resultMsg.AddModules();
            TString moduleName;
            std::vector<TString> exports;
            std::vector<TString> imports;
            if (!ModuleResolver->AddFromMemory(f.GetName(), f.GetContent(), librariesContext, 1, 0, {}, moduleName, &exports, &imports)) {
                ythrow yexception() << "Library " << f.GetName() << " compilation error: " << librariesContext.IssueManager.GetIssues().ToString();
            }

            moduleMsg->SetName(moduleName);
            for (auto e : exports) {
                auto exportMsg = moduleMsg->AddExports();
                exportMsg->SetName(e);
            }

            for (auto i : imports) {
                auto importMsg = moduleMsg->AddImports();
                importMsg->SetName(i);
            }
        }

        TString ysonResult = NYql::Proto2Yson(resultMsg);
        TaskResult->AddResults(ysonResult);
    }

    NProto::ETaskStatus FinishedWithComplete() {
        auto status = NProto::ETaskStatus::COMPLETED;
        TaskResult->SetStatus(status);
        TaskResult->SetRevision(TaskResult->GetRevision() + 1);

        TaskWatcher->OnResultChange(TTaskResultChanges::MAX_VALUE, TaskResult);
        return status;
    }
};

IYqlRunnableTaskPtr CreateSqlLibraryMetaTask(
        TTaskDataPtr taskData,
        ITaskWatcherPtr taskWatcher,
        IMetricsRegistryPtr metrics,
        IModuleResolver::TPtr moduleResolver) {
    return MakeIntrusive<TSqlLibraryMetaTask>(std::move(taskData), std::move(taskWatcher), std::move(metrics), std::move(moduleResolver));
}
}
