#pragma once

#include <yt/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <yt/yql/tools/ytrun/lib/ytrun_lib.h>

#include <yql/essentials/core/cbo/cbo_optimizer_new.h>
#include <yql/essentials/core/dq_integration/transform/yql_dq_task_transform.h>
#include <yql/essentials/core/dq_integration/yql_dq_helper.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/providers/common/metrics/metrics_registry.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/tools/yql_facade_run/yql_facade_run.h>
#include <yql/essentials/utils/log/log_level.h>

#include <contrib/ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <contrib/ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <contrib/ydb/library/yql/providers/dq/interface/yql_dq_task_preprocessor.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NYql {

// Base class for DQ-based run tools (dqrun and dqrun_light).
// Contains the common option parsing, gateway/provider setup and helper
// methods shared between the "light" and the "full" variants. The "full"
// variant (dqrun) extends this class to add extra providers (S3, Ydb,
// ClickHouse, Solomon, ...).
class TDqRunToolBase: public TYtRunTool {
public:
    TDqRunToolBase(TString name);

protected:
    TProgram::TStatus DoRunProgram(TProgramPtr program) override;
    IOptimizerFactory::TPtr CreateCboFactory() override;
    IDqHelper::TPtr CreateDqHelper() override;
    IYtGateway::TPtr CreateYtGateway() override;

    NYql::NFile::TYtFileServices::TPtr GetYtFileServices();
    IMetricsRegistryPtr GetMetricsRegistry();
    NKikimr::NMiniKQL::TComputationNodeFactory CreateCompNodeFactory();
    NYql::TTaskTransformFactory CreateDqTaskTransformFactory();
    NYql::TDqTaskPreprocessorFactoryCollection CreateDqTaskPreprocessorFactories();
    NYql::NDq::IDqAsyncIoFactory::TPtr CreateAsyncIoFactory();

    // Hooks for the "full" variant to register extra providers and their
    // options / cluster mappings. The default implementations are no-ops.
    virtual void RegisterExtraOptions(NLastGetopt::TOpts& opts);
    virtual void FillExtraClusterMappings();
    virtual void RegisterExtraProviderFactories();
    virtual void FillExtraCompNodeFactories(TVector<NKikimr::NMiniKQL::TComputationNodeFactory>& factories);
    virtual void FillExtraDqTaskTransformFactories(TVector<TTaskTransformFactory>& factories);
    virtual void RegisterExtraAsyncIoFactories(NYql::NDq::TDqAsyncIoFactory& factory);

protected:
    bool AnalyzeQuery_ = false;
    bool NoForceDq_ = false;
    bool EmulateYt_ = false;
    TMaybe<TString> DqHost_;
    TMaybe<int> DqPort_;
    int DqThreads_ = 16;
    bool EnableSpilling_ = false;

    IOutputStream* MetricsStream_ = nullptr;
    THolder<IOutputStream> MetricsStreamHolder_;

    TString TmpDir_;
    THashMap<TString, TString> TablesMapping_;

    NFile::TYtFileServices::TPtr YtFileServices_;
    IMetricsRegistryPtr MetricsRegistry_;
};

}
