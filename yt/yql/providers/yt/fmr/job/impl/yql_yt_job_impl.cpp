#include <library/cpp/threading/future/core/future.h>

#include <util/stream/file.h>

#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/table_data_service.h>
#include <yt/yql/providers/yt/fmr/yt_service/interface/yql_yt_yt_service.h>

#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

class TFmrJob: public IFmrJob {
public:

    TFmrJob(ITableDataService::TPtr tableDataService, IYtService::TPtr ytService, std::shared_ptr<std::atomic<bool>> cancelFlag)
        : TableDataService_(tableDataService), YtService_(ytService), CancelFlag_(cancelFlag)
    {
    }

    virtual TMaybe<TString> Download(const TDownloadTaskParams& params) override {
        const auto ytTable = params.Input;
        const auto cluster = params.Input.Cluster;
        const auto path = params.Input.Path;
        const auto tableId = params.Output.TableId;

        YQL_CLOG(DEBUG, FastMapReduce) << "Downloading " << cluster << '.' << path;

        TTempFileHandle tableFile = YtService_->Download(ytTable);
        TFileInput inputStream(tableFile.Name());
        TString tableContent = inputStream.ReadAll();

        TableDataService_->Put(tableId, tableContent).Wait();;

        return Nothing();
    }

    virtual TMaybe<TString> Upload(const TUploadTaskParams& params) override {
        const auto ytTable = params.Output;
        const auto cluster = params.Output.Cluster;
        const auto path = params.Output.Path;
        const auto tableId = params.Input.TableId;

        YQL_CLOG(DEBUG, FastMapReduce) << "Uploading " << cluster << '.' << path;

        TMaybe<TString> getResult = TableDataService_->Get(tableId).GetValueSync();

        if (!getResult) {

            YQL_CLOG(ERROR, FastMapReduce) << "Table " << tableId << " not found";
            return "Table not found";

        }

        TString tableContent = getResult.GetRef();
        TStringInput inputStream(tableContent);

        YtService_->Upload(ytTable, inputStream);

        return Nothing();
    }
private:
    ITableDataService::TPtr TableDataService_;
    IYtService::TPtr YtService_;
    std::shared_ptr<std::atomic<bool>> CancelFlag_;
};

IFmrJob::TPtr MakeFmrJob(ITableDataService::TPtr tableDataService, IYtService::TPtr ytService, std::shared_ptr<std::atomic<bool>> cancelFlag) {
    return MakeIntrusive<TFmrJob>(tableDataService, ytService, cancelFlag);
}

ETaskStatus RunJob(
    const TTaskParams& taskParams,
    ITableDataService::TPtr tableDataService,
    IYtService::TPtr ytService,
    std::shared_ptr<std::atomic<bool>> cancelFlag
) {
    IFmrJob::TPtr job = MakeFmrJob(tableDataService, ytService, cancelFlag);

    auto processTask = [job] (auto&& taskParams) {
        using T = std::decay_t<decltype(taskParams)>;

        if constexpr (std::is_same_v<T, TUploadTaskParams>) {
            return job->Upload(taskParams);
        } else if constexpr (std::is_same_v<T, TDownloadTaskParams>) {
            return job->Download(taskParams);
        } else if constexpr (std::is_same_v<T, TMergeTaskParams>) {
            TMaybe<TString> errMsg = "Not Defined";
            return errMsg;
        } else {
            throw std::runtime_error{"Unsupported task type"};
        }
    };

    TMaybe<TString> taskResult = std::visit(processTask, taskParams);

    if (taskResult.Defined()) {
        YQL_CLOG(ERROR, FastMapReduce) << "Task failed: " << taskResult.GetRef();
        return ETaskStatus::Failed;
    }

    return ETaskStatus::Completed;
};

} // namespace NYql
