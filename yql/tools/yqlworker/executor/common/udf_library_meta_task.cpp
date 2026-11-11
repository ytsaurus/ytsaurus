#include "udf_library_meta_task.h"

#include <yql/essentials/utils/log/context.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/profile.h>
#include <yql/essentials/utils/signals/utils.h>

#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_utils.h>
#include <yql/essentials/core/yql_holding_file_storage.h>

#include <yql/essentials/core/yql_user_data.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/providers/common/udf_resolve/yql_outproc_udf_resolver.h>

#include <yql/tools/yqlworker/executor/common/credentials.h>
#include <yql/tools/yqlworker/interface/proto/task.pb.h>

#include <yql/essentials/utils/debug_info.h>

using namespace NKikimr;
using namespace NMiniKQL;

namespace NYql {
struct TUdfLibraryMetaTask final : public IYqlRunnableTask {

    const TTaskDataPtr TaskData;
    const TTaskResultPtr TaskResult;
    const ITaskWatcherPtr TaskWatcher;
    const IMetricsRegistryPtr Metrics;
    const IUdfResolver::TPtr UdfResolver;
    TFileStoragePtr FileStorage;
    IUrlPreprocessing::TPtr UrlPreprocessing;

    TUdfLibraryMetaTask(
        TTaskDataPtr taskData,
        ITaskWatcherPtr taskWatcher,
        IMetricsRegistryPtr metrics,
        IUdfResolver::TPtr udfResolver,
        TFileStoragePtr fileStorage,
        IUrlPreprocessing::TPtr urlPreprocessing)
        : TaskData(std::move(taskData))
        , TaskResult(new NProto::TTaskResult)
        , TaskWatcher(std::move(taskWatcher))
        , Metrics(std::move(metrics))
        , UdfResolver(std::move(udfResolver))
        , FileStorage(fileStorage)
        , UrlPreprocessing(std::move(urlPreprocessing))
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

            DoFetchUdfMeta();
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

    void DoFetchUdfMeta() {
        TVector<TUserDataBlock> blocks;
        blocks.reserve(TaskData->FilesSize());

        TCredentials::TPtr creds = MakeIntrusive<TCredentials>();
        if (TaskData->HasAuthData()) {
            ParseCredentials(TaskData->GetAuthData(), *creds);
        }

        if (TaskData->FilesSize() > 0) {
            const auto& files = TaskData->GetFiles();
            for (auto& f : files) {
                if (f.GetType() != NProto::TTaskFile::URL) {
                    continue;
                }

                auto [url, alias] = UrlPreprocessing->Preprocess(f.GetContent());

                TUserDataBlock& newBlock = blocks.emplace_back();
                newBlock.Type = EUserDataType::URL;
                newBlock.Data = url;
                newBlock.Usage.Set(EUserDataBlockUsage::Udf);

                TString tokenName;
                if (f.HasTokenName()) {
                    tokenName = f.GetTokenName();
                } else if (alias) {
                    tokenName = "default_" + alias;
                }

                if (tokenName) {
                    if (auto cred = creds->FindCredential(tokenName)) {
                        newBlock.UrlToken = cred->Content;
                    }
                }
            }
        }

        if (blocks.empty()) {
            ythrow yexception() << "No URL import detected";
        }
        THoldingFileStorage storage(FileStorage);
        NYql::TResolveResult outMsg = LoadRichMetadata(*UdfResolver, blocks, storage);
        TString ysonResult = NYql::Proto2Yson(outMsg);
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

IYqlRunnableTaskPtr CreateUdfLibraryMetaTask(
        TTaskDataPtr taskData,
        ITaskWatcherPtr taskWatcher,
        IMetricsRegistryPtr metrics,
        IUdfResolver::TPtr udfResolver,
        TFileStoragePtr fileStorage,
        IUrlPreprocessing::TPtr urlPreprocessing) {
    return MakeIntrusive<TUdfLibraryMetaTask>(std::move(taskData), std::move(taskWatcher), std::move(metrics), udfResolver, fileStorage, std::move(urlPreprocessing));
}
}
