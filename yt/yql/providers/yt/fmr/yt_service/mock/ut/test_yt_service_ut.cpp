#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/yt_service/mock/yql_yt_yt_service_impl.h>
#include <yt/yql/providers/yt/fmr/yt_service/interface/yql_yt_yt_service.h>

using namespace NYql::NFmr;


Y_UNIT_TEST_SUITE(TLocalTableServiceTest)
{
    Y_UNIT_TEST(DownloadTable) {
        TString TableContent =
        "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";

        TYtTableRef ytTable = TYtTableRef{"test_path","test_cluster","test_transaction_id"};
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        ytUploadedTablesMock->AddTable(ytTable, TableContent);
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);

        TTempFileHandle tmpFile = ytService->Download(ytTable);

        TFileInput inputStream(tmpFile.Name());
        TString localContent = inputStream.ReadAll();

        UNIT_ASSERT_NO_DIFF(TableContent, localContent);
    }
    Y_UNIT_TEST(UploadTable) {
        TString TableContent =
        "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";

        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        TYtTableRef ytTable = TYtTableRef{"test_path","test_cluster","test_transaction_id"};
        TStringInput inputStream(TableContent);

        ytService->Upload(ytTable, inputStream);

        UNIT_ASSERT_NO_DIFF(TableContent, ytUploadedTablesMock->GetTableContent(ytTable));
        UNIT_ASSERT_EQUAL(ytUploadedTablesMock->UploadedTablesNum(), 1);
    }
}
