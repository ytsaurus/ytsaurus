#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <yt/yql/providers/yt/gateway/fmr/yql_yt_fmr.h>
#include <yql/essentials/core/cbo/simple/cbo_simple.h>
#include <yql/essentials/core/facade/yql_facade.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/core/services/mounts/yql_mounts.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/testing/unittest/registar.h>

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
    auto fmrGateway = CreateYtFmrGateway(ytGateway);

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

    TProgramFactory factory(true, functionRegistry.Get(), 0ULL, dataProvidersInit, "ut");
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
}
