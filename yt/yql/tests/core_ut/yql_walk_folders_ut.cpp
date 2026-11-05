#include <yt/yql/providers/yt/provider/yql_yt_forwarding_gateway.h>
#include <yt/yql/providers/yt/provider/yql_yt_gateway.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <yt/yql/providers/yt/lib/ut_common/yql_ut_common.h>

#include <yql/essentials/core/facade/yql_facade.h>
#include <yql/essentials/core/cbo/simple/cbo_simple.h>
#include <yql/essentials/core/services/mounts/yql_mounts.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <library/cpp/threading/future/future.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/user.h>

#include <thread>

namespace NYql {

namespace {

class TDelayedFoldersGateway : public TYtForwardingGatewayBase {
public:
    TDelayedFoldersGateway(IYtGateway::TPtr&& inner)
        : TYtForwardingGatewayBase(std::move(inner))
    {}

    NThreading::TFuture<TBatchFolderResult> GetFolders(TBatchFolderOptions&& options) override {
        auto origFuture = Slave_->GetFolders(std::move(options));
        if (++Counter_ % 2 == 1) {
            return origFuture;
        }

        auto promise = NThreading::NewPromise<TBatchFolderResult>();
        origFuture.Subscribe([promise](const auto& f) {
            std::thread([promise, f]() mutable {
                Sleep(TDuration::MilliSeconds(100));
                promise.SetValue(f.GetValue());
            }).detach();
        });

        return promise.GetFuture();
    }

private:
    size_t Counter_ = 0;
};

bool RunWithDelayedFolders(const TString& query)
{
    auto funcReg = NKikimr::NMiniKQL::CreateFunctionRegistry(
        NKikimr::NMiniKQL::CreateBuiltinRegistry());
    auto cloned = funcReg->Clone();
    NKikimr::NMiniKQL::FillStaticModules(*cloned);

    auto yqlNativeServices = NFile::TYtFileServices::Make(cloned.Get(), {}, {}, "");
    auto mockGateway = MakeIntrusive<TDelayedFoldersGateway>(CreateYtFileGateway(yqlNativeServices));

    TVector<TDataProviderInitializer> dataProvidersInit;
    dataProvidersInit.push_back(
        GetYtNativeDataProviderInitializer(mockGateway, MakeSimpleCBOOptimizerFactory(), {}));

    TExprContext modulesCtx;
    IModuleResolver::TPtr moduleResolver;
    if (!GetYqlDefaultModuleResolver(modulesCtx, moduleResolver)) {
        return false;
    }
    TExprContext::TFreezeGuard freezeGuard(modulesCtx);

    TProgramFactory factory(true, cloned.Get(), 0ULL, dataProvidersInit, "ut");
    factory.SetUdfResolver(NCommon::CreateSimpleUdfResolver(cloned.Get()));
    factory.SetModules(moduleResolver);

    TProgramPtr program = factory.Create("-stdin-", query);

    NSQLTranslation::TTranslationSettings settings;
    settings.ClusterMapping["plato"] = TString(YtProviderName);
    if (!program->ParseSql(settings) || !program->Compile(GetUsername())) {
        program->PrintErrorsTo(Cerr);
        return false;
    }

    TProgram::TStatus status = program->Run(GetUsername());

    if (status == TProgram::TStatus::Error) {
        program->PrintErrorsTo(Cerr);
    }
    return status == TProgram::TStatus::Ok;
}

} // namespace

Y_UNIT_TEST_SUITE(WalkFoldersIODiscovery) {
    Y_UNIT_TEST(SeveralWalkFoldersWithDelayedGetFolders) {
        const TString query = R"(
            $postHandler = ($nodes, $state, $level) -> {
                return ListExtend($state, ListExtract($nodes, "Path"));
            };
            $res_1 = (SELECT State FROM plato.WalkFolders(`dir1`, $postHandler AS PostHandler));
            $res_2 = (SELECT State FROM plato.WalkFolders(`dir2`, $postHandler AS PostHandler));
            $res_3 = (SELECT State FROM plato.WalkFolders(`dir3`, $postHandler AS PostHandler));
            SELECT * FROM (SELECT State FROM $res_1 FLATTEN BY State)
            UNION ALL
            SELECT * FROM (SELECT State FROM $res_2 FLATTEN BY State)
            UNION ALL
            SELECT * FROM (SELECT State FROM $res_3 FLATTEN BY State);
        )";
        // Delay every second call to GetFolders()
        UNIT_ASSERT(RunWithDelayedFolders(query));
    }
}

} // namespace NYql
