#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/yt_service/mock/yql_yt_yt_service_mock.h>
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

        TYtTableRef ytTable = TYtTableRef{"test_path","test_cluster"};
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        ytUploadedTablesMock->AddTable(ytTable, TableContent);
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);

        ui64 rowsCount;
        auto res = ytService->Download(ytTable, rowsCount);

        auto* err = std::get_if<TError>(&res);
        UNIT_ASSERT_C(!err,err->ErrorMessage);
        auto tmpFile = std::get_if<THolder<TTempFileHandle>>(&res);

        TString name = tmpFile->Get()->Name();
        TFileInput inputStream(name);
        TString localContent = inputStream.ReadAll();

        UNIT_ASSERT_NO_DIFF(TableContent, localContent);
        UNIT_ASSERT_EQUAL(rowsCount, 4);
    }
    Y_UNIT_TEST(DownloadNonExistentTable) {
        TYtTableRef ytTable = TYtTableRef{"test_path","test_cluster"};
        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        // No table
        // ytUploadedTablesMock->AddTable(ytTable, TableContent);
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);

        ui64 rowsCount;
        auto res = ytService->Download(ytTable, rowsCount);

        TError* err = std::get_if<TError>(&res);
        UNIT_ASSERT_C(err,"No error was returned");
    }
    Y_UNIT_TEST(UploadTable) {
        TString TableContent =
        "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";

        TYtUploadedTablesMock::TPtr ytUploadedTablesMock = MakeYtUploadedTablesMock();
        NYql::NFmr::IYtService::TPtr ytService = MakeYtServiceMock(ytUploadedTablesMock);
        TYtTableRef ytTable = TYtTableRef{"test_path","test_cluster"};
        TStringInput inputStream(TableContent);

        auto err = ytService->Upload(ytTable, inputStream);

        UNIT_ASSERT_C(!err,err->ErrorMessage);

        UNIT_ASSERT_NO_DIFF(TableContent, ytUploadedTablesMock->GetTableContent(ytTable));
        UNIT_ASSERT_EQUAL(ytUploadedTablesMock->UploadedTablesNum(), 1);
    }
}
