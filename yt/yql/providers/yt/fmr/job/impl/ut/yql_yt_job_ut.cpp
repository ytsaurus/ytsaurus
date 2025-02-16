#include <library/cpp/testing/unittest/registar.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/table_data_service.h>
#include <yt/yql/providers/yt/fmr/yt_service/mock/yql_yt_yt_service_impl.h>

namespace NYql::NFmr {

Y_UNIT_TEST_SUITE(FmrJobTests) {
    Y_UNIT_TEST(DownloadTable) {
        TString tableContent =
        "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);
        IFmrJob::TPtr job = MakeFmrJob(tableDataServicePtr, ytService, cancelFlag);

        TYtTableRef input = TYtTableRef("test_cluster", "test_path");
        TFmrTableRef output = TFmrTableRef("test_table_id");
        TDownloadTaskParams params = TDownloadTaskParams(input, output);

        ytUploadedTablesMock->AddTable(input, tableContent);

        job->Download(params);

        UNIT_ASSERT_NO_DIFF(tableDataServicePtr->Get("test_table_id").GetValueSync().GetRef(), tableContent);
    }

    Y_UNIT_TEST(UploadTable) {
        TString ytTableContent =
        "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);
        IFmrJob::TPtr job = MakeFmrJob(tableDataServicePtr, ytService, cancelFlag);

        TYtTableRef output = TYtTableRef("test_cluster", "test_path");
        TFmrTableRef input = TFmrTableRef("test_table_id");
        TUploadTaskParams params = TUploadTaskParams(input, output);

        tableDataServicePtr->Put(input.TableId, ytTableContent);

        job->Upload(params);

        UNIT_ASSERT_NO_DIFF(ytUploadedTablesMock->GetTableContent(output), ytTableContent);
    }
}

Y_UNIT_TEST_SUITE(TaskRunTests) {
    Y_UNIT_TEST(RunDownloadTask) {
        TString ytTableContent =
        "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"}";

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        TYtTableRef input = TYtTableRef("test_cluster", "test_path");
        TFmrTableRef output = TFmrTableRef("test_table_id");

        ytUploadedTablesMock->AddTable(input, ytTableContent);
        TDownloadTaskParams params = TDownloadTaskParams(input, output);

        ETaskStatus status = RunJob(params, tableDataServicePtr, ytService, cancelFlag);

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Completed);
        UNIT_ASSERT_NO_DIFF(tableDataServicePtr->Get("test_table_id").GetValueSync().GetRef(), ytTableContent);
    }

    Y_UNIT_TEST(RunUploadTask) {
        TString ytTableContent =
        "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"}";

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        TFmrTableRef input = TFmrTableRef("test_table_id");
        TYtTableRef output = TYtTableRef("test_cluster", "test_path");

        TUploadTaskParams params = TUploadTaskParams(input, output);

        tableDataServicePtr->Put(input.TableId, ytTableContent);

        ETaskStatus status = RunJob(params, tableDataServicePtr, ytService, cancelFlag);

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Completed);
        UNIT_ASSERT_NO_DIFF(ytUploadedTablesMock->GetTableContent(output), ytTableContent);
    }

    Y_UNIT_TEST(RunUploadTaskWithNoTable) {
        TString ytTableContent =
        "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"}";

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        std::shared_ptr<std::atomic<bool>> cancelFlag = std::make_shared<std::atomic<bool>>(false);

        TFmrTableRef input = TFmrTableRef("test_table_id");
        TYtTableRef output = TYtTableRef("test_cluster", "test_path");

        TUploadTaskParams params = TUploadTaskParams(input, output);

        // No table in tableDataServicePtr
        // tableDataServicePtr->Put(input.TableId, ytTableContent);

        ETaskStatus status = RunJob(params, tableDataServicePtr, ytService, cancelFlag);

        UNIT_ASSERT_EQUAL(status, ETaskStatus::Failed);
        UNIT_ASSERT(ytUploadedTablesMock->IsEmpty());
    }
}

} // namespace NYql
