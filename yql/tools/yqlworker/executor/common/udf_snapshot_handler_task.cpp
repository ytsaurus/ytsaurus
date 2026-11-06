#include "udf_snapshot_handler_task.h"

#include <yql/essentials/utils/log/context.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/profile.h>
#include <yql/essentials/utils/signals/utils.h>

#include <yql/tools/yqlworker/interface/proto/task.pb.h>
#include <yql/tools/yqlworker/proto/function_registry.pb.h>
#include <yql/essentials/utils/debug_info.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/yson/node/node_io.h>

namespace NYql {

static const TString SUCCESS_RESULT_VALUE = "true";
static const TString BLOCKING_MODE_KEY = "blocking";

struct TUdfSnapshotHandlerTask final : public IYqlRunnableTask {

    const TTaskDataPtr TaskData;
    const TTaskResultPtr TaskResult;
    const ITaskWatcherPtr TaskWatcher;
    const IMetricsRegistryPtr Metrics;
    const IDqControlFactoryPtr DqControlFactory;
    const TFileStoragePtr FileStorage;
    TAtomic Aborted = 0;

    TUdfSnapshotHandlerTask(
        TTaskDataPtr taskData,
        ITaskWatcherPtr taskWatcher,
        IMetricsRegistryPtr metrics,
        IDqControlFactoryPtr dqControlFactory,
        TFileStoragePtr fileStorage)
        : TaskData(std::move(taskData))
        , TaskResult(new NProto::TTaskResult)
        , TaskWatcher(std::move(taskWatcher))
        , Metrics(std::move(metrics))
        , DqControlFactory(std::move(dqControlFactory))
        , FileStorage(std::move(fileStorage))
    {
        TaskWatcher->OnStart();
        TaskResult->SetRevision(TaskResult->GetRevision() + 1);
        TaskResult->SetStatus(NProto::ETaskStatus::PENDING);
        TaskWatcher->OnResultChange(TTaskResultChanges::MAX_VALUE, TaskResult);
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

        NProto::ETaskStatus finishStatus;
        try {
            bool isBlocking = false;
            if (TaskData->HasAttributes()) {
                auto node = NYT::NodeFromYsonString(TaskData->GetAttributes());
                if (node.IsMap() && node.HasKey(BLOCKING_MODE_KEY) && node.At(BLOCKING_MODE_KEY).IsBool()) {
                    isBlocking = node.ChildAsBool(BLOCKING_MODE_KEY);
                }
            }
            YQL_LOG(INFO) << "Start processing task: " << TaskData->GetId() << " in " << (isBlocking ? "" : "non-") << "blocking mode";

            Metrics->IncCounter("action", NProto::ETaskAction_Name(NProto::ETaskAction::RUN));
            Metrics->IncCounter("syntax", NProto::ESyntax_Name(TaskData->GetSyntax()));

            finishStatus = DoWarmup(isBlocking);

            NJson::TJsonValue json(NJson::JSON_MAP);
            json["success"] = SUCCESS_RESULT_VALUE;
            TString ysonResult = NJson::WriteJson(json, false);
            TaskResult->AddResults(ysonResult);
        } catch (...) {
            TString errMsg = CurrentExceptionMessage();
            YQL_LOG(ERROR) << "Error while processing task " << TaskData->GetId() << ": " << errMsg;
            AddFatalMessageToTaskResult(TaskResult.get(), errMsg);
            finishStatus = NProto::ETaskStatus::ERROR;
        }
        TaskResult->SetRevision(TaskResult->GetRevision() + 1);
        TaskResult->SetStatus(finishStatus);

        Metrics->IncCounter("completion_status", NProto::ETaskStatus_Name(finishStatus));
        Metrics->Flush();

        TaskWatcher->OnResultChange(TTaskResultChanges::MAX_VALUE, TaskResult);
        TaskWatcher->OnFinish(finishStatus);
        return true;
    }

    void DoAbort() override {
        AtomicIncrement(Aborted);
    }

private:
    NProto::ETaskStatus DoWarmup(bool isBlocking) {
        if (!DqControlFactory) {
            return NProto::ETaskStatus::COMPLETED;
        }

        TVector<TFileLinkPtr> fileLinks;
        IDqControl::TFileMap additionalFiles;

        if (TaskData->HasFunctionRegistryData()) {
            NProto::TFunctionRegistryData registryData = GetRegistryData(*TaskData);
            auto filter = DqControlFactory->GetIndexedUdfFilter();
            auto doStrip = DqControlFactory->StripEnabled();
            for (auto &pkg : registryData.GetPackages()) {
                if (!filter.contains(pkg.GetName())) {
                    continue;
                }

                for (auto &p : pkg.GetResources()) {
                    TFileLinkPtr &link = fileLinks.emplace_back();
                    link = FileStorage->PutUrl(p.GetUrl(), "");
                    TString path = link->GetPath(), objectId = link->GetMd5();
                    auto size = link->GetSize();
                    TString contentMd5;
                    if (doStrip) {
                        TFileLinkPtr & newLink = fileLinks.emplace_back();
                        newLink = FileStorage->PutFileStripped(path, objectId);
                        path = newLink->GetPath();
                        objectId = objectId + DqStrippedSuffied();
                        size = newLink->GetSize();
                        contentMd5 = newLink->GetMd5();
                    }
                    additionalFiles.emplace(path, IDqControl::TAdditionalFile{
                        .ObjectId = objectId,
                        .ContentMd5 = contentMd5,
                    });
                    YQL_LOG(DEBUG)
                        << "Additional UDF to warmup. Name: " << pkg.GetName()
                        << " Path: " << path
                        << " ObjectId: " << objectId
                        << " Size: " << size;
                }
            }
        }

        // initiate warmup process
        auto dqControl = DqControlFactory->GetControl();
        auto isDqReady = dqControl->IsReady(additionalFiles);
        YQL_LOG(DEBUG) << "DQ warmup initiated, current status: " << isDqReady;
        if (!isBlocking) {
            return NProto::ETaskStatus::COMPLETED;
        }

        while (!isDqReady) {
            YQL_LOG(DEBUG) << "waiting DQ warmup";
            Sleep(TDuration::Seconds(3));
            if (AtomicGet(Aborted)) {
                YQL_LOG(DEBUG) << "aborted DQ warmup";
                return NProto::ETaskStatus::ABORTED;
            }
            isDqReady = dqControl->IsReady();
        }
        return NProto::ETaskStatus::COMPLETED;
    }

    NProto::TFunctionRegistryData GetRegistryData(const NProto::TTaskData& data) {
        TString content = data.GetFunctionRegistryData();
        TStringInput in(content);
        NProto::TFunctionRegistryData registryData;
        ParseFromTextFormat(in, registryData, EParseFromTextFormatOption::AllowUnknownField);
        return registryData;
    }
};

IYqlRunnableTaskPtr CreateUdfSnapshotHandlerTask(
    TTaskDataPtr taskData,
    ITaskWatcherPtr taskWatcher,
    IMetricsRegistryPtr metrics,
    IDqControlFactoryPtr dqControlFactory,
    TFileStoragePtr fileStorage)
{
    return MakeIntrusive<TUdfSnapshotHandlerTask>(
        std::move(taskData),
        std::move(taskWatcher),
        std::move(metrics),
        std::move(dqControlFactory),
        std::move(fileStorage));
}

} // namespace NYql
