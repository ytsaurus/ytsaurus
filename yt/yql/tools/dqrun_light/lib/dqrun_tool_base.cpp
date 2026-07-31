#include "dqrun_tool_base.h"

#include <yt/yql/providers/dq/gateway/yql_dq_gateway_factory.h>
#include <yt/yql/providers/dq/local_gateway/yql_dq_gateway_local.h>
#include <yt/yql/providers/yt/comp_nodes/dq/dq_yt_factory.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_comp_nodes.h>
#include <yt/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <yt/yql/providers/yt/mkql_dq/yql_yt_dq_transform.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider.h>

#include <yql/essentials/core/dq_integration/transform/yql_dq_task_transform.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/parser/pg_wrapper/interface/comp_factory.h>
#include <yql/essentials/providers/common/comp_nodes/yql_factory.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <contrib/ydb/library/yql/dq/actors/input_transforms/dq_input_transform_lookup_factory.h>
#include <contrib/ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <contrib/ydb/library/yql/dq/opt/dq_opt_join_cbo_factory.h>
#include <contrib/ydb/library/yql/dq/transform/yql_common_dq_transform.h>
#include <contrib/ydb/library/yql/providers/dq/helper/yql_dq_helper_impl.h>
#include <contrib/ydb/library/yql/providers/dq/interface/yql_dq_task_preprocessor.h>
#include <contrib/ydb/library/yql/providers/dq/provider/exec/yql_dq_exectransformer.h>
#include <contrib/ydb/library/yql/providers/dq/provider/yql_dq_provider.h>
#include <contrib/ydb/library/yql/providers/yt/actors/yql_yt_provider_factories.h>
#include <contrib/ydb/library/yql/providers/yt/dq_task_preprocessor/yql_yt_dq_task_preprocessor.h>
#include <contrib/ydb/library/yql/utils/bindings/utils.h>

#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/builder.h>

#ifdef PROFILE_MEMORY_ALLOCATIONS
#include <library/cpp/lfalloc/alloc_profiler/profiler.h>
#endif

namespace NYql {

TDqRunToolBase::TDqRunToolBase(TString name)
    : TYtRunTool(std::move(name))
{
    GetRunOptions().EnableCredentials = true;
    GetRunOptions().EnableQPlayer = true;
    GetRunOptions().ResultStream = &Cout;
    GetRunOptions().ResultsFormat = NYson::EYsonFormat::Text;
    GetRunOptions().CustomTests = true;
    GetRunOptions().Verbosity = TLOG_INFO;

    GetRunOptions().AddOptExtension([this](NLastGetopt::TOpts& opts) {

        opts.AddLongOption("dq-host", "Dq host")
            .RequiredArgument("HOST")
            .Handler1T<TString>([this](const TString& host) {
                DqHost_ = host;
            });
        opts.AddLongOption("dq-port", "Dq port")
            .RequiredArgument("PORT")
            .Handler1T<int>([this](int port) {
                DqPort_ = port;
            });
        opts.AddLongOption("analyze-query", "enable analyze query")
            .Optional()
            .NoArgument()
            .SetFlag(&AnalyzeQuery_);
        opts.AddLongOption("no-force-dq", "don't set force dq mode")
            .Optional()
            .NoArgument()
            .SetFlag(&NoForceDq_);
        opts.AddLongOption("enable-spilling", "Enable disk spilling")
            .NoArgument()
            .SetFlag(&EnableSpilling_);
        opts.AddLongOption("tmp-dir", "directory for temporary tables")
            .StoreResult<TString>(&TmpDir_);
        opts.AddLongOption('t', "table", "Table mapping").RequiredArgument("table@file")
            .KVHandler([&](TString name, TString path) {
                if (name.empty() || path.empty()) {
                    throw yexception() << "Incorrect table mapping, expected form table@file, e.g. yt.plato.Input@input.txt";
                }
                TablesMapping_[name] = path;
            }, '@');
        opts.AddLongOption('C', "cluster", "Cluster to service mapping").RequiredArgument("name@service")
            .KVHandler([&](TString cluster, TString provider) {
                if (cluster.empty() || provider.empty()) {
                    throw yexception() << "Incorrect service mapping, expected form cluster@provider, e.g. plato@yt";
                }
                AddClusterMapping(std::move(cluster), std::move(provider));
            }, '@');
        opts.AddLongOption("dq-threads", "DQ gateway threads")
            .Optional()
            .RequiredArgument("COUNT")
            .StoreResult(&DqThreads_);
        opts.AddLongOption("metrics", "Print execution metrics")
            .Optional()
            .OptionalArgument("FILE")
            .Handler1T<TString>([this](const TString& file) {
                if (file) {
                    MetricsStreamHolder_ = MakeHolder<TFileOutput>(file);
                    MetricsStream_ = MetricsStreamHolder_.Get();
                } else {
                    MetricsStream_ = &Cerr;
                }
            });
        opts.AddLongOption('E', "emulate-yt", "Emulate YT tables")
            .Optional()
            .NoArgument()
            .SetFlag(&EmulateYt_);

        opts.AddLongOption("bindings-file", "Bindings File")
            .Optional()
            .RequiredArgument("FILE")
            .Handler1T<TString>([this](const TString& file) {
                TFileInput input(file);
                LoadBindings(GetRunOptions().Bindings, input.ReadAll());
            });

        RegisterExtraOptions(opts);
    });

    GetRunOptions().AddOptHandler([this](const NLastGetopt::TOptsParseResult& res) {
        Y_UNUSED(res);

        if (EmulateYt_ && DqPort_) {
            throw yexception() << "Remote DQ instance cannot work with the emulated YT cluster";
        }
        if (EmulateYt_) {
            GetRunOptions().GatewayTypes.emplace(YtProviderName);
        }

        GetRunOptions().EnableResultPosition = !EmulateYt_;
        GetRunOptions().UseRepeatableRandomAndTimeProviders = EmulateYt_;

        if (!GetRunOptions().GatewaysConfig) {
            GetRunOptions().GatewaysConfig = MakeHolder<TGatewaysConfig>();
        }

        auto dqConfig = GetRunOptions().GatewaysConfig->MutableDq();

        if (AnalyzeQuery_) {
            auto* setting = dqConfig->AddDefaultSettings();
            setting->SetName("AnalyzeQuery");
            setting->SetValue("1");
        }

        if (EnableSpilling_) {
            auto* setting = dqConfig->AddDefaultSettings();
            setting->SetName("SpillingEngine");
            setting->SetValue("file");
        }
        if (EmulateYt_) {
            AddClusterMapping(TString{"plato"}, TString{YtProviderName});
            // Clusters from gateways are filled in YtRunLib
        }

        FillExtraClusterMappings();

        GetRunOptions().SqlFlags["DqEngineEnable"] = {};
        if (!AnalyzeQuery_ && !NoForceDq_) {
            GetRunOptions().SqlFlags["DqEngineForce"] = {};
        }

    });

    AddProviderFactory([this]() -> NYql::TDataProviderInitializer {
        auto compFactory = CreateCompNodeFactory();
        TIntrusivePtr<IDqGateway> dqGateway;
        if (DqPort_) {
            dqGateway = CreateDqGateway(DqHost_.GetOrElse("localhost"), *DqPort_);
        } else {
            std::function<NActors::IActor*(void)> metricsPusherFactory = {};
            dqGateway = CreateLocalDqGateway(GetFuncRegistry().Get(), compFactory, CreateDqTaskTransformFactory(), CreateDqTaskPreprocessorFactories(),
                 EnableSpilling_, CreateAsyncIoFactory(), DqThreads_, GetMetricsRegistry(), metricsPusherFactory);
        }

        return GetDqDataProviderInitializer(&CreateDqExecTransformer, dqGateway, compFactory, {}, GetFileStorage());
    });
}

void TDqRunToolBase::RegisterExtraOptions(NLastGetopt::TOpts& opts) {
    Y_UNUSED(opts);
}

void TDqRunToolBase::FillExtraClusterMappings() {
}

void TDqRunToolBase::RegisterExtraProviderFactories() {
}

void TDqRunToolBase::FillExtraCompNodeFactories(TVector<NKikimr::NMiniKQL::TComputationNodeFactory>& factories) {
    Y_UNUSED(factories);
}

void TDqRunToolBase::FillExtraDqTaskTransformFactories(TVector<TTaskTransformFactory>& factories) {
    Y_UNUSED(factories);
}

void TDqRunToolBase::RegisterExtraAsyncIoFactories(NYql::NDq::TDqAsyncIoFactory& factory) {
    Y_UNUSED(factory);
}

IYtGateway::TPtr TDqRunToolBase::CreateYtGateway() {
    if (EmulateYt_) {
        return CreateYtFileGateway(GetYtFileServices());
    }
    return TYtRunTool::CreateYtGateway();
}

IOptimizerFactory::TPtr TDqRunToolBase::CreateCboFactory() {
    return NDq::MakeCBOOptimizerFactory();
}

IDqHelper::TPtr TDqRunToolBase::CreateDqHelper() {
    return MakeDqHelper();
}

NFile::TYtFileServices::TPtr TDqRunToolBase::GetYtFileServices() {
    if (!YtFileServices_) {
        YtFileServices_ = NFile::TYtFileServices::Make(GetFuncRegistry().Get(), TablesMapping_, GetFileStorage(), TmpDir_, KeepTemp_);
    }
    return YtFileServices_;
}

IMetricsRegistryPtr TDqRunToolBase::GetMetricsRegistry() {
    if (!MetricsRegistry_) {
        MetricsRegistry_ = CreateMetricsRegistry(GetSensorsGroupFor(NSensorComponent::kDq));
    }
    return MetricsRegistry_;
}

NKikimr::NMiniKQL::TComputationNodeFactory TDqRunToolBase::CreateCompNodeFactory() {
    TVector<NKikimr::NMiniKQL::TComputationNodeFactory> factories = {
        GetCommonDqFactory(),
        NKikimr::NMiniKQL::GetYqlFactory(),
        GetPgFactory()
    };

    if (GetRunOptions().GatewaysConfig->HasYt() || EmulateYt_) {
        factories.push_back(GetDqYtFactory());
    }
    if (EmulateYt_) {
        factories.push_back(GetYtFileFactory(GetYtFileServices()));
    }
    FillExtraCompNodeFactories(factories);
    return NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory(factories);
}

TTaskTransformFactory TDqRunToolBase::CreateDqTaskTransformFactory() {
    TVector<TTaskTransformFactory> factories = {
        CreateCommonDqTaskTransformFactory()
    };

    if (GetRunOptions().GatewaysConfig->HasYt() || EmulateYt_) {
        factories.push_back(CreateYtDqTaskTransformFactory());
    }
    FillExtraDqTaskTransformFactories(factories);

    return CreateCompositeTaskTransformFactory(std::move(factories));
}

TDqTaskPreprocessorFactoryCollection TDqRunToolBase::CreateDqTaskPreprocessorFactories() {
    TDqTaskPreprocessorFactoryCollection factories;

    if (GetRunOptions().GatewaysConfig->HasYt() || EmulateYt_) {
        factories.push_back(NDq::CreateYtDqTaskPreprocessorFactory(EmulateYt_, GetFuncRegistry()));
    }
    return factories;
}

NDq::IDqAsyncIoFactory::TPtr TDqRunToolBase::CreateAsyncIoFactory() {
    auto factory = MakeIntrusive<NYql::NDq::TDqAsyncIoFactory>();
    RegisterDqInputTransformLookupActorFactory(*factory);
    if (EmulateYt_) {
        RegisterYtLookupActorFactory(*factory, GetYtFileServices(), *GetFuncRegistry());
    }
    RegisterExtraAsyncIoFactories(*factory);

    return factory;
}


TProgram::TStatus TDqRunToolBase::DoRunProgram(TProgramPtr program) {
#ifdef PROFILE_MEMORY_ALLOCATIONS
    NAllocProfiler::StartAllocationSampling(true);
#endif
    const TProgram::TStatus status = TYtRunTool::DoRunProgram(program);
#ifdef PROFILE_MEMORY_ALLOCATIONS
    NAllocProfiler::StopAllocationSampling(Cout);
#endif
    return status;
}

} // NYql
