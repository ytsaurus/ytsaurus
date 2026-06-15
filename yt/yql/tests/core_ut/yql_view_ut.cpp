#include <yql/essentials/core/cbo/simple/cbo_simple.h>
#include <yql/essentials/core/facade/yql_facade.h>
#include <yql/essentials/providers/common/gateway/yql_provider_gateway.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include <yql/essentials/core/services/mounts/yql_mounts.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <yt/yql/providers/yt/provider/yql_yt_provider.h>
#include <yt/yql/providers/yt/provider/yql_yt_forwarding_gateway.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_services.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/user.h>
#include <util/system/tempfile.h>

namespace NYql {

namespace {

class TFailingGateway: public TYtForwardingGatewayBase {
public:
    explicit TFailingGateway(IYtGateway::TPtr&& slave)
        : TYtForwardingGatewayBase(std::move(slave))
        , ClosePromise_(NThreading::NewPromise<void>())
    {
    }

    TFuture<TResOrPullResult> ResOrPull(const TExprNode::TPtr&, TExprContext&, TResOrPullOptions&&) override {
        // this will cause CompiledSql to be left in TTablesData in YtState (CleanupCompiledSQL will not be called)
        return NThreading::MakeFuture(NCommon::ResultFromError<TResOrPullResult>(TIssue(TPosition(), "Emulated evaluation failure")));
    }

    TFuture<void> CloseSession(TCloseSessionOptions&&) override {
        return ClosePromise_.GetFuture(); // stays pending: an in-flight async close
    }

private:
    NThreading::TPromise<void> ClosePromise_;
};

// A second data provider whose session close fails immediately. This is what makes
// WaitExceptionOrAll() in CloseLastSession() complete (and abandon the pending YT close)
// before the YT cleanup has run.
TDataProviderInitializer MakeFailingCloseProvider() {
    return [](const TString&, const TString&, const TGatewaysConfig*,
        const NKikimr::NMiniKQL::IFunctionRegistry*, TIntrusivePtr<IRandomProvider>,
        TIntrusivePtr<TTypeAnnotationContext>, const TOperationProgressWriter&,
        const TYqlOperationOptions&, THiddenQueryAborter, const TQContext&)
    {
        TDataProviderInfo info;
        info.Names.insert("dummy_failing_close");
        info.CloseSessionAsync = [](const TString&) {
            return NThreading::MakeErrorFuture<void>(
                std::make_exception_ptr(yexception() << "Emulated second-provider close failure"));
        };
        return info;
    };
}

void WriteViewInput(TTempFileHandle& inputFile, TTempFileHandle& attrFile) {
    static const TStringBuf KSV_ATTRS =
        "{\"_yql_row_spec\" = {\"Type\" = [\"StructType\";["
        "[\"key\";[\"DataType\";\"String\"]];"
        "[\"subkey\";[\"DataType\";\"String\"]];"
        "[\"value\";[\"DataType\";\"String\"]]"
        "]]};"
        "\"_yql_view_ksv\"=\"SELECT key AS k, subkey AS s, value AS v FROM self\"}"
        ;

    static const TStringBuf DATA =
        "{\"key\"=\"075\";\"subkey\"=\".\";\"value\"=\"abc\"};\n"
        "{\"key\"=\"800\";\"subkey\"=\".\";\"value\"=\"ddd\"};\n"sv
        ;

    inputFile.Write(DATA.data(), DATA.size());
    inputFile.FlushData();

    attrFile.Write(KSV_ATTRS.data(), KSV_ATTRS.size());
    attrFile.FlushData();
}

} // namespace

Y_UNIT_TEST_SUITE(View) {
    // YQLOVERYT-51: a failing query that reads a view, in a multi-provider setup where the
    // YT session close is asynchronous and another provider's close fails first.
    Y_UNIT_TEST(ViewCleanupRunsWhenCloseSessionAbandoned) {
        auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        auto cloned = functionRegistry->Clone();
        NKikimr::NMiniKQL::FillStaticModules(*cloned);
        functionRegistry = cloned;

        TTempFileHandle inputFile;
        TTempFileHandle attrFile(inputFile.Name() + ".attr");
        WriteViewInput(inputFile, attrFile);

        THashMap<TString, TString> tables;
        tables["yt.plato.Input"] = inputFile.Name();

        auto yqlNativeServices = NFile::TYtFileServices::Make(functionRegistry.Get(), tables, {}, "");
        // `gateway` (and the pending close promise it holds) outlives the program.
        IYtGateway::TPtr gateway = MakeIntrusive<TFailingGateway>(CreateYtFileGateway(yqlNativeServices));

        TVector<TDataProviderInitializer> dataProvidersInit;
        dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(gateway, MakeSimpleCBOOptimizerFactory(), {}));
        dataProvidersInit.push_back(MakeFailingCloseProvider());

        TExprContext modulesCtx;
        IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(GetYqlDefaultModuleResolver(modulesCtx, moduleResolver));
        TExprContext::TFreezeGuard freezeGuard(modulesCtx);

        TProgramFactory factory(true, functionRegistry.Get(), 0ULL, dataProvidersInit, "ut");
        factory.SetUdfResolver(NCommon::CreateSimpleUdfResolver(functionRegistry.Get()));
        factory.SetModules(moduleResolver);

        auto query = "$x = SELECT count(*) FROM plato.Input VIEW ksv; SELECT EvaluateExpr($x);";

        {
            TProgramPtr program = factory.Create("-stdin-", query);
            NSQLTranslation::TTranslationSettings settings;
            settings.ClusterMapping["plato"] = TString(YtProviderName);
            UNIT_ASSERT_C(program->ParseSql(settings), "parse failed");
            UNIT_ASSERT_C(program->Compile(GetUsername()), "compile failed");

            TProgram::TStatus status = program->Run(GetUsername());
            UNIT_ASSERT_VALUES_EQUAL(status, TProgram::TStatus::Error);
            // Destroying `program` here must NOT abort with
            // "Node ... not dead on destruction" (the YQLOVERYT-51 crash).
        }
    }
}

} // namespace NYql
