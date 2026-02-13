#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yt/yql/providers/yt/fmr/fmr_tool_lib/yql_yt_fmr_initializer.h>
#include <yt/yql/providers/yt/fmr/test_tools/table_data_service/yql_yt_table_data_service_helpers.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <yql/essentials/core/cbo/simple/cbo_simple.h>
#include <yql/essentials/core/facade/yql_facade.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/core/services/mounts/yql_mounts.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/stream/file.h>
#include <util/system/user.h>
#include <util/system/tempfile.h>

using namespace NYql;
using namespace NYql::NFmr;

constexpr TStringBuf InputData =
    "{\"key\"=\"075\";\"subkey\"=\".\";\"value\"=\"abc\"};\n"
    "{\"key\"=\"800\";\"subkey\"=\".\";\"value\"=\"ddd\"};\n"
    "{\"key\"=\"020\";\"subkey\"=\".\";\"value\"=\"q\"};\n"
    "{\"key\"=\"150\";\"subkey\"=\".\";\"value\"=\"qzz\"};\n"sv;

template <typename F>
TString WithTables(const F&& f) {
    static const TStringBuf KSV_ATTRS =
        "{\"_yql_row_spec\" = {\"Type\" = [\"StructType\";["
        "[\"key\";[\"DataType\";\"String\"]];"
        "[\"subkey\";[\"DataType\";\"String\"]];"
        "[\"value\";[\"DataType\";\"String\"]]"
        "]]}}"
        ;

    TTempFileHandle inputFile, outputFile;
    TTempFileHandle inputFileAttrs(inputFile.Name() + ".attr");

    inputFile.Write(InputData.data(), InputData.size());
    inputFile.FlushData();
    inputFileAttrs.Write(KSV_ATTRS.data(), KSV_ATTRS.size());
    inputFileAttrs.FlushData();

    THashMap<TString, TString> tables;
    tables["yt.plato.Input"] = inputFile.Name();
    tables["yt.plato.Output"] = outputFile.Name();
    f(tables);

    TFileInput outputFileStream(outputFile);
    NYT::TNode ysonSqlResult = NYT::NodeFromYsonStream(&outputFileStream, NYT::NYson::EYsonType::ListFragment);
    return NYT::NodeToCanonicalYsonString(ysonSqlResult);
}

struct TRunSettings {
    bool IsSql = true;
    bool IsPg = false;
    THashMap<TString, TString> Tables;
};

bool RunProgram(const TString& query, const TRunSettings& runSettings) {
    auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
    auto yqlNativeServices = NFile::TYtFileServices::Make(functionRegistry.Get(), runSettings.Tables, {}, "");
    auto ytGateway = CreateYtFileGateway(yqlNativeServices);

    auto fmrServices = MakeIntrusive<NFmr::TFmrServices>();
    fmrServices->FunctionRegistry = functionRegistry.Get();
    fmrServices->JobLauncher = MakeIntrusive<NFmr::TFmrUserJobLauncher>(NFmr::TFmrUserJobLauncherOptions{.RunInSeparateProcess = false});
    fmrServices->DisableLocalFmrWorker = false;
    fmrServices->YtJobService = NFmr::MakeFileYtJobService();
    fmrServices->YtCoordinatorService = NFmr::MakeFileYtCoordinatorService();
    fmrServices->NeedToTransformTmpTablePaths = false;

    TTempFileHandle discoveryFile;
    TPortManager pm;
    const ui16 port = pm.GetPort();
    SetupTableDataServiceDiscovery(discoveryFile, port);
    auto tableDataServiceServer = MakeTableDataServiceServer(port);
    fmrServices->TableDataServiceDiscoveryFilePath = discoveryFile.Name();

    TFileStorageConfig fsConfig;
    fsConfig.SetThreads(3);
    auto fileStorage = WithAsync(CreateFileStorage(fsConfig, {}));

    fmrServices->FileStorage = fileStorage;
    auto jobPreparer = NFmr::MakeFmrJobPreparer(fileStorage, discoveryFile.Name());
    fmrServices->JobPreparer = jobPreparer;

    auto [fmrGateway, worker] = InitializeFmrGateway(ytGateway, fmrServices);

    TVector<TDataProviderInitializer> dataProvidersInit;
    dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(fmrGateway, MakeSimpleCBOOptimizerFactory(), {}));

    TExprContext modulesCtx;
    IModuleResolver::TPtr moduleResolver;
    if (!GetYqlDefaultModuleResolver(modulesCtx, moduleResolver)) {
        Cerr << "Errors loading default YQL libraries:" << Endl;
        modulesCtx.IssueManager.GetIssues().PrintTo(Cerr);
        return false;
    }
    TExprContext::TFreezeGuard freezeGuard(modulesCtx);

    TProgramFactory factory(false, functionRegistry.Get(), 0ULL, dataProvidersInit, "ut");
    factory.SetModules(moduleResolver);

    TProgramPtr program = factory.Create("-stdin-", query, "", EHiddenMode::Disable);

    if (runSettings.IsSql || runSettings.IsPg) {
        NSQLTranslation::TTranslationSettings settings;
        settings.PgParser = runSettings.IsPg;
        settings.ClusterMapping["plato"] = TString(YtProviderName);

        if (!program->ParseSql(settings)) {
            program->PrintErrorsTo(Cerr);
            return false;
        }
    }

    if (!program->Compile(GetUsername())) {
        program->PrintErrorsTo(Cerr);
        return false;
    }

    TProgram::TStatus status = program->Run(GetUsername(), nullptr, nullptr, nullptr);
    if (status == TProgram::TStatus::Error) {
        program->PrintErrorsTo(Cerr);
    }
    return status == TProgram::TStatus::Ok;
}

Y_UNIT_TEST_SUITE(FastMapReduceTests) {
    Y_UNIT_TEST(InsertTmpTable) {
        auto query = "use plato; insert into Output with truncate select * from Input";
        TTempFileHandle outputFile;
        auto sqlQueryResult = WithTables([&](const auto& tables) {
            TRunSettings runSettings;
            runSettings.Tables = tables;
            UNIT_ASSERT(RunProgram(query, runSettings));
        });
        auto expected = NYT::NodeToCanonicalYsonString(NYT::NodeFromYsonString(InputData, NYT::NYson::EYsonType::ListFragment));
        UNIT_ASSERT_NO_DIFF(sqlQueryResult, expected);
    }

    Y_UNIT_TEST(InsertTmpTableFiltered) {
        auto query = "use plato; insert into Output with truncate select * from Input where Cast(key As Uint32) > 700";
        TTempFileHandle outputFile;
        auto sqlQueryResult = WithTables([&](const auto& tables) {
            TRunSettings runSettings;
            runSettings.Tables = tables;
            UNIT_ASSERT(RunProgram(query, runSettings));
        });
        TStringBuf filteredInputData = "{\"key\"=\"800\";\"subkey\"=\".\";\"value\"=\"ddd\"};\n"sv;
        auto expected = NYT::NodeToCanonicalYsonString(NYT::NodeFromYsonString(filteredInputData, NYT::NYson::EYsonType::ListFragment));
        UNIT_ASSERT_NO_DIFF(sqlQueryResult, expected);
    }

    Y_UNIT_TEST(SelectFixedColumn) {
        auto query = "use plato; insert into Output with truncate select key from Input where Cast(key As Uint32) > 700";
        TTempFileHandle outputFile;
        auto sqlQueryResult = WithTables([&](const auto& tables) {
            TRunSettings runSettings;
            runSettings.Tables = tables;
            UNIT_ASSERT(RunProgram(query, runSettings));
        });
        TStringBuf filteredInputData = "{\"key\"=\"800\";};\n"sv;
        auto expected = NYT::NodeToCanonicalYsonString(NYT::NodeFromYsonString(filteredInputData, NYT::NYson::EYsonType::ListFragment));
        UNIT_ASSERT_NO_DIFF(sqlQueryResult, expected);
    }
    Y_UNIT_TEST(AnonymousTables) {
        auto query = R"(
            use plato;
            PRAGMA DqEngine = 'disable';

            INSERT INTO @anon1 with truncate (
                SELECT * FROM `Input`
            );

            COMMIT;

            INSERT INTO @anon2 WITH truncate (
                SELECT * FROM @anon1
            );

            COMMIT;

            INSERT INTO `Output` WITH TRUNCATE (
                SELECT value || '_suffix' AS result FROM @anon2
            );
        )";
        TTempFileHandle outputFile;
        auto sqlQueryResult = WithTables([&](const auto& tables) {
            TRunSettings runSettings;
            runSettings.Tables = tables;
            UNIT_ASSERT(RunProgram(query, runSettings));
        });
        TString expected = "[{\"result\"=\"abc_suffix\"};{\"result\"=\"ddd_suffix\"};{\"result\"=\"q_suffix\"};{\"result\"=\"qzz_suffix\"}]";
        UNIT_ASSERT_NO_DIFF(sqlQueryResult, expected);
    }
}
