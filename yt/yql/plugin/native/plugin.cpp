#include "dq_manager.h"
#include "plugin.h"

#include "error_helpers.h"
#include "progress_merger.h"

#include <yt/yql/providers/yt/common/yql_names.h>
#include <yt/yql/providers/yt/comp_nodes/dq/dq_yt_factory.h>
#include <yt/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <yt/yql/providers/yt/lib/log/yt_logger.h>
#include <yt/yql/providers/yt/lib/res_pull/res_or_pull.h>
#include <yt/yql/providers/yt/lib/row_spec/yql_row_spec.h>
#include <yt/yql/providers/yt/lib/schema/schema.h>
#include <yt/yql/providers/yt/lib/skiff/yql_skiff_schema.h>
#include <yt/yql/providers/yt/lib/yt_download/yt_download.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider.h>

#include <yql/essentials/providers/common/codec/yql_codec_type_flags.h>
#include <yql/essentials/providers/common/codec/yql_codec.h>
#include <yql/essentials/providers/common/comp_nodes/yql_factory.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/udf_resolve/yql_simple_udf_resolver.h>

#include <contrib/ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <contrib/ydb/library/yql/providers/dq/provider/yql_dq_provider.h>
#include <contrib/ydb/library/yql/providers/dq/provider/yql_dq_state.h>
#include <contrib/ydb/library/yql/providers/dq/provider/exec/yql_dq_exectransformer.h>
#include <contrib/ydb/library/yql/providers/dq/helper/yql_dq_helper_impl.h>

#include <yql/essentials/ast/yql_expr.h>
#include <contrib/ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <contrib/ydb/library/yql/dq/opt/dq_opt_join_cbo_factory.h>
#include <yql/essentials/core/facade/yql_facade.h>
#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/core/file_storage/proto/file_storage.pb.h>
#include <yql/essentials/core/services/mounts/yql_mounts.h>
#include <yql/essentials/core/services/yql_transform_pipeline.h>
#include <yql/essentials/core/url_preprocessing/url_preprocessing.h>
#include <yql/essentials/core/yql_library_compiler.h>
#include <yql/essentials/core/yql_type_helpers.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/utils/backtrace/backtrace.h>
#include <yql/essentials/utils/log/log.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/program/config.h>
#include <yt/yt/library/program/helpers.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/parser.h>
#include <library/cpp/yson/writer.h>

#include <library/cpp/digest/md5/md5.h>

#include <library/cpp/resource/resource.h>

#include <util/folder/path.h>

#include <util/stream/file.h>

#include <util/string/builder.h>

#include <util/system/fs.h>

namespace NYT::NYqlPlugin {
namespace NNative {

using namespace NYson;
using namespace NKikimr::NMiniKQL;

static const TString YqlAgent = "yql_agent";

////////////////////////////////////////////////////////////////////////////////

std::optional<TString> MaybeToOptional(const TMaybe<TString>& maybeStr)
{
    if (!maybeStr) {
        return std::nullopt;
    }
    return *maybeStr;
};

////////////////////////////////////////////////////////////////////////////////

class TQueryPipelineConfigurator
    : public NYql::IPipelineConfigurator
    , public TRefCounted
{
public:
    struct TQueryPlan
    {
        std::optional<TString> Plan;
        YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, PlanSpinLock);
    };
    NYql::TProgramPtr Program_;
    mutable TQueryPlan Plan_;

    TQueryPipelineConfigurator(NYql::TProgramPtr program)
        : Program_(std::move(program))
    { }

    void AfterCreate(NYql::TTransformationPipeline* /*pipeline*/) const override
    { }

    void AfterTypeAnnotation(NYql::TTransformationPipeline* /*pipeline*/) const override
    { }

    void AfterOptimize(NYql::TTransformationPipeline* pipeline) const override
    {
        auto transformer = [this](NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext& /*ctx*/) {
            output = input;

            auto guard = WriterGuard(Plan_.PlanSpinLock);
            Plan_.Plan = MaybeToOptional(Program_->GetQueryPlan());

            return NYql::IGraphTransformer::TStatus::Ok;
        };

        pipeline->Add(NYql::CreateFunctorTransformer(transformer), "PlanOutput");
    }
};
DECLARE_REFCOUNTED_TYPE(TQueryPipelineConfigurator)
DEFINE_REFCOUNTED_TYPE(TQueryPipelineConfigurator)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicConfig
    : public TRefCounted
{
    NYql::TGatewaysConfig GatewaysConfig;
    THashMap<TString, TString> Clusters;
    std::optional<TString> DefaultCluster;
    ::TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FuncRegistry;
    NYql::TExprContext ExprContext;
    NYql::IModuleResolver::TPtr ModuleResolver;
};
DECLARE_REFCOUNTED_TYPE(TDynamicConfig)
DEFINE_REFCOUNTED_TYPE(TDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TActiveQuery
{
    NYql::TProgramPtr Program;
    bool Compiled = false;

    TProgressMerger ProgressMerger;
    TQueryPipelineConfiguratorPtr PipelineConfigurator;
    std::optional<TString> Plan;

    // Store shared data for TProgram after dyn config changing.
    TDynamicConfigPtr ProgramSharedData;
    NYql::TProgramFactoryPtr ProgramFactory;
};

////////////////////////////////////////////////////////////////////////////////

class TSkiffConverter
    : public ISkiffConverter
{
public:
    TString ConvertNodeToSkiff(
        const TDqStatePtr state,
        const IDataProvider::TFillSettings& fillSettings,
        const NYT::TNode& rowSpec,
        const NYT::TNode& item,
        const TVector<TString>& columns) override
    {
        TMemoryUsageInfo memInfo("DqResOrPull");
        TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), state->FunctionRegistry->SupportsSizedAllocators());
        THolderFactory holderFactory(alloc.Ref(), memInfo, state->FunctionRegistry);
        TTypeEnvironment env(alloc);
        NYql::NCommon::TCodecContext codecCtx(env, *state->FunctionRegistry, &holderFactory);

        auto skiffBuilder = MakeHolder<TSkiffExecuteResOrPull>(fillSettings.RowsLimitPerWrite, fillSettings.AllResultsBytesLimit, codecCtx, holderFactory, rowSpec, state->TypeCtx->OptLLVM.GetOrElse("OFF"), columns);
        if (item.IsList()) {
            skiffBuilder->SetListResult();
            for (auto& node : item.AsList()) {
                skiffBuilder->WriteNext(node);
            }
        } else {
            skiffBuilder->WriteNext(item);
        }

        return skiffBuilder->Finish();
    }

    TYtType ParseYTType(const TExprNode& node, TExprContext& ctx, const TMaybe<NYql::TColumnOrder>& columns) override
    {
        const auto sequenceItemType = GetSequenceItemType(node.Pos(), node.GetTypeAnn(), false, ctx);

        auto rowSpecInfo = MakeIntrusive<TYqlRowSpecInfo>();
        rowSpecInfo->SetType(sequenceItemType->Cast<TStructExprType>(), NTCF_ALL);
        rowSpecInfo->SetColumnOrder(columns);

        NYT::TNode tableSpec = NYT::TNode::CreateMap();
        rowSpecInfo->FillCodecNode(tableSpec[YqlRowSpecAttribute]);

        auto resultYTType = NodeToYsonString(RowSpecToYTSchema(tableSpec[YqlRowSpecAttribute], NTCF_ALL).ToNode());
        auto resultRowSpec = NYT::TNode::CreateMap()(TString{YqlIOSpecTables}, NYT::TNode::CreateList().Add(tableSpec));
        auto resultSkiffType = NodeToYsonString(TablesSpecToOutputSkiff(resultRowSpec));

        return {
            .Type = resultYTType,
            .SkiffType = resultSkiffType,
            .RowSpec = resultRowSpec
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYqlPlugin
    : public IYqlPlugin
{
public:
    TYqlPlugin(TYqlPluginOptions options)
        : DqManagerConfig_(options.DqManagerConfig ? NYTree::ConvertTo<TDqManagerConfigPtr>(options.DqManagerConfig) : nullptr)
    {
        try {
            auto singletonsConfig = NYTree::ConvertTo<TSingletonsConfigPtr>(options.SingletonsConfig);
            ConfigureSingletons(singletonsConfig);

            NYql::NLog::InitLogger(std::move(options.LogBackend));

            auto& logger = NYql::NLog::YqlLogger();

            logger.SetDefaultPriority(ELogPriority::TLOG_DEBUG);
            for (int i = 0; i < NYql::NLog::EComponentHelpers::ToInt(NYql::NLog::EComponent::MaxValue); ++i) {
                logger.SetComponentLevel((NYql::NLog::EComponent)i, NYql::NLog::ELevel::DEBUG);
            }

            NYql::SetYtLoggerGlobalBackend(NYT::ILogger::ELevel::DEBUG);
            if (NYT::TConfig::Get()->Prefix.empty()) {
                NYT::TConfig::Get()->Prefix = "//";
            }

            NYson::TProtobufWriterOptions protobufWriterOptions;
            protobufWriterOptions.ConvertSnakeToCamelCase = true;

            auto* gatewayDqConfig = GatewaysConfigInitial_.MutableDq();
            if (DqManagerConfig_) {
                gatewayDqConfig->ParseFromStringOrThrow(NYson::YsonStringToProto(
                    options.DqGatewayConfig,
                    NYson::ReflectProtobufMessageType<NYql::TDqGatewayConfig>(),
                    protobufWriterOptions));
            }

            auto* gatewayYtConfig = GatewaysConfigInitial_.MutableYt();
            gatewayYtConfig->ParseFromStringOrThrow(NYson::YsonStringToProto(
                options.GatewayConfig,
                NYson::ReflectProtobufMessageType<NYql::TYtGatewayConfig>(),
                protobufWriterOptions));

            NYql::TFileStorageConfig fileStorageConfig;
            fileStorageConfig.ParseFromStringOrThrow(NYson::YsonStringToProto(
                options.FileStorageConfig,
                NYson::ReflectProtobufMessageType<NYql::TFileStorageConfig>(),
                protobufWriterOptions));

            FileStorage_ = WithAsync(CreateFileStorage(fileStorageConfig, {MakeYtDownloader(fileStorageConfig)}));

            if (DqManagerConfig_) {
                DqManagerConfig_->FileStorage = FileStorage_;
                DqManager_ = New<TDqManager>(DqManagerConfig_);
            }

            LoadYqlDefaultMounts(UserDataTable_);

            const auto libraries = NYTree::ConvertTo<THashMap<TString, TString>>(options.Libraries);
            TVector<NYql::NUserData::TUserData> userData;
            userData.reserve(libraries.size());
            for (const auto& [module, path] : libraries) {
                userData.emplace_back(NYql::NUserData::EType::LIBRARY, NYql::NUserData::EDisposition::FILESYSTEM, path, path);
                Modules_[to_lower(module)] = path;

                auto& block = UserDataTable_[TUserDataKey::File(path)];
                block.Data = path;
                block.Type = EUserDataType::PATH;
                block.Usage.Set(EUserDataBlockUsage::Library, true);
            }

            NYql::NUserData::TUserData::UserDataToLibraries(userData, Modules_);

            OperationAttributes_ = options.OperationAttributes;

            DynamicConfig_ = CreateDynamicConfig(NYql::TGatewaysConfig(GatewaysConfigInitial_));

            if (options.YTTokenPath) {
                TFsPath path(options.YTTokenPath);
                YqlAgentToken_ = TIFStream(path).ReadAll();
            } else if (!NYT::TConfig::Get()->Token.empty()) {
                YqlAgentToken_ = NYT::TConfig::Get()->Token;
            }
            // do not use token from .yt/token or env in queries
            NYT::TConfig::Get()->Token = {};
        } catch (const std::exception& ex) {
            // NB: YQL_LOG may be not initialized yet (for example, during singletons config parse),
            // so we use std::cerr instead of it.
            std::cerr << "Unexpected exception while initializing YQL plugin: " << ex.what() << std::endl;
            exit(1);
        }
        YQL_LOG(INFO) << "YQL plugin initialized";
    }

    void Start() override
    {
        if (DqManager_) {
            DqManager_->Start();
        }
    }

    TClustersResult GuardedGetUsedClusters(
        TString queryText,
        TYsonString settings,
        std::vector<TQueryFile> files)
    {
        TDynamicConfigPtr dynamicConfig;
        {
            auto guard = ReaderGuard(DynamicConfigSpinLock);
            dynamicConfig = DynamicConfig_;
        }
        auto factory = CreateProgramFactory(*dynamicConfig);
        auto program = factory->Create("-memory-", queryText);

        program->AddCredentials({{"default_yt", NYql::TCredential("yt", "", YqlAgentToken_)}});
        program->SetOperationAttrsYson(PatchQueryAttributes(OperationAttributes_, settings));

        auto defaultQueryCluster = dynamicConfig->DefaultCluster;
        auto ysonSettings = NodeFromYsonString(settings.ToString()).AsMap();
        if (auto cluster = ysonSettings.FindPtr("cluster")) {
            defaultQueryCluster = cluster->AsString();
        }

        auto userDataTable = FilesToUserTable(files);
        program->AddUserDataTable(userDataTable);

        NSQLTranslation::TTranslationSettings sqlSettings;
        sqlSettings.ClusterMapping = dynamicConfig->Clusters;
        sqlSettings.ModuleMapping = Modules_;
        if (defaultQueryCluster) {
            sqlSettings.DefaultCluster = *defaultQueryCluster;
        }
        sqlSettings.SyntaxVersion = 1;
        sqlSettings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;

        if (!program->ParseSql(sqlSettings)) {
            return TClustersResult{
                .YsonError = IssuesToYtErrorYson(program->Issues()),
            };
        }

        if (!program->Compile(YqlAgent)) {
            return TClustersResult{
                .YsonError = IssuesToYtErrorYson(program->Issues()),
            };
        }

        auto usedClusters = program->GetUsedClusters();
        if (!usedClusters) {
            return TClustersResult{
                .YsonError = MessageToYtErrorYson("Can't get clusters from query"),
            };
        }

        if (defaultQueryCluster && !usedClusters->contains(*defaultQueryCluster)) {
            usedClusters->insert(*defaultQueryCluster);
        }

        std::vector<TString> clustersList(usedClusters->begin(), usedClusters->end());

        return TClustersResult{
            .Clusters = clustersList,
        };
    }

    TQueryResult GuardedRun(
        TQueryId queryId,
        TString user,
        TYsonString credentialsStr,
        TString queryText,
        TYsonString settings,
        std::vector<TQueryFile> files,
        int executeMode)
    {
        TDynamicConfigPtr dynamicConfig;
        {
            auto guard = ReaderGuard(DynamicConfigSpinLock);
            dynamicConfig = DynamicConfig_;
        }
        auto factory = CreateProgramFactory(*dynamicConfig);
        auto program = factory->Create("-memory-", queryText);
        auto pipelineConfigurator = New<TQueryPipelineConfigurator>(program);
        {
            auto guard = WriterGuard(ProgressSpinLock);
            auto& query = ActiveQueriesProgress_[queryId];
            query.Program = program;
            query.PipelineConfigurator = pipelineConfigurator;
            query.ProgramSharedData = dynamicConfig;
            query.ProgramFactory = factory;
        }

        TVector<std::pair<TString, NYql::TCredential>> credentials;
        const auto credentialsMap = NodeFromYsonString(credentialsStr.ToString()).AsMap();
        credentials.reserve(credentialsMap.size());
        for (const auto& item : credentialsMap) {
            credentials.emplace_back(item.first, NYql::TCredential {
                item.second.HasKey("category") ? item.second.ChildAsString("category") : "",
                item.second.HasKey("subcategory") ? item.second.ChildAsString("subcategory") : "",
                item.second.HasKey("content") ? item.second.ChildAsString("content") : ""
            });
        }
        program->AddCredentials(credentials);
        program->SetOperationAttrsYson(PatchQueryAttributes(OperationAttributes_, settings));

        auto defaultQueryCluster = dynamicConfig->DefaultCluster;
        auto settingsMap = NodeFromYsonString(settings.ToString()).AsMap();
        if (auto cluster = settingsMap.FindPtr("cluster")) {
            defaultQueryCluster = cluster->AsString();
        }

        auto userDataTable = FilesToUserTable(files);
        program->AddUserDataTable(userDataTable);

        program->SetProgressWriter([&] (const NYql::TOperationProgress& progress) {
            std::optional<TString> plan;
            {
                auto guard = ReaderGuard(pipelineConfigurator->Plan_.PlanSpinLock);
                plan.swap(pipelineConfigurator->Plan_.Plan);
            }

            auto guard = WriterGuard(ProgressSpinLock);
            ActiveQueriesProgress_[queryId].ProgressMerger.MergeWith(progress);
            if (plan) {
                ActiveQueriesProgress_[queryId].Plan.swap(plan);
            }
        });

        program->SetResultType(NYql::IDataProvider::EResultFormat::Skiff);

        NSQLTranslation::TTranslationSettings sqlSettings;
        sqlSettings.ClusterMapping = dynamicConfig->Clusters;
        sqlSettings.ModuleMapping = Modules_;
        if (defaultQueryCluster) {
            sqlSettings.DefaultCluster = *defaultQueryCluster;
        }
        sqlSettings.SyntaxVersion = 1;
        sqlSettings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;
        if (DqManager_) {
            sqlSettings.DqDefaultAuto = NSQLTranslation::ISqlFeaturePolicy::MakeAlwaysAllow();
        }

        if (!program->ParseSql(sqlSettings)) {
            return TQueryResult{
                .YsonError = IssuesToYtErrorYson(program->Issues()),
            };
        }

        if (!program->Compile(user)) {
            return TQueryResult{
                .YsonError = IssuesToYtErrorYson(program->Issues()),
            };
        }

        {
            auto guard = WriterGuard(ProgressSpinLock);
            ActiveQueriesProgress_[queryId].Compiled = true;
        }

        NYql::TProgram::TStatus status = NYql::TProgram::TStatus::Error;

        // NYT::NYqlClient::EExecuteMode (yt/yt/ytlib/yql_client/public.h)
        switch (executeMode) {
        case 0: // Validate.
            status = program->Validate(user, nullptr);
            break;
        case 1: // Optimize.
            status = program->OptimizeWithConfig(user, *pipelineConfigurator);
            break;
        case 2: // Run.
            status = program->RunWithConfig(user, *pipelineConfigurator);
            break;
        default: // Unknown.
            return TQueryResult{
                .YsonError = MessageToYtErrorYson(Format("Unknown execution mode: %v", executeMode)),
            };
        }

        if (status == NYql::TProgram::TStatus::Error) {
            return TQueryResult{
                .YsonError = IssuesToYtErrorYson(program->Issues()),
            };
        }

        TStringStream result;
        if (program->HasResults()) {
            ::NYson::TYsonWriter yson(&result, EYsonFormat::Binary);
            yson.OnBeginList();
            for (const auto& result : program->Results()) {
                yson.OnListItem();
                yson.OnRaw(result);
            }
            yson.OnEndList();
        }

        TString progress = ExtractQuery(queryId).value_or(TActiveQuery{}).ProgressMerger.ToYsonString();

        return {
            .YsonResult = result.Empty() ? std::nullopt : std::make_optional(result.Str()),
            .Plan = MaybeToOptional(program->GetQueryPlan()),
            .Statistics = MaybeToOptional(program->GetStatistics()),
            .Progress = progress,
            .TaskInfo = MaybeToOptional(program->GetTasksInfo()),
        };
    }

    TClustersResult GetUsedClusters(
        TString queryText,
        TYsonString settings,
        std::vector<TQueryFile> files) noexcept override
    {
        try {
            return GuardedGetUsedClusters(queryText, settings, files);
        } catch (const std::exception& ex) {
            return TClustersResult{
                .YsonError = MessageToYtErrorYson(ex.what()),
            };
        }
    }

    TQueryResult Run(
        TQueryId queryId,
        TString user,
        TYsonString credentials,
        TString queryText,
        TYsonString settings,
        std::vector<TQueryFile> files,
        int executeMode) noexcept override
    {
        auto finalCleaning = Finally([&] {
            ExtractQuery(queryId);
        });

        try {
            return GuardedRun(queryId, user, credentials, queryText, settings, files, executeMode);
        } catch (const std::exception& ex) {
            return TQueryResult{
                .YsonError = MessageToYtErrorYson(ex.what()),
            };
        }
    }

    TQueryResult GetProgress(TQueryId queryId) noexcept override
    {
        auto guard = ReaderGuard(ProgressSpinLock);
        if (ActiveQueriesProgress_.contains(queryId)) {
            TQueryResult result;
            if (ActiveQueriesProgress_[queryId].ProgressMerger.HasChangesSinceLastFlush()) {
                result.Plan = ActiveQueriesProgress_[queryId].Plan;
                result.Progress = ActiveQueriesProgress_[queryId].ProgressMerger.ToYsonString();
            }
            return result;
        } else {
            return TQueryResult{
                .YsonError = MessageToYtErrorYson(Format("No progress for queryId: %v", queryId)),
            };
        }
    }

    TAbortResult Abort(TQueryId queryId) noexcept override
    {
        NYql::TProgramPtr program;
        {
            auto guard = WriterGuard(ProgressSpinLock);
            if (!ActiveQueriesProgress_.contains(queryId)) {
                return TAbortResult{
                    .YsonError = MessageToYtErrorYson(Format("Query %v is not found", queryId)),
                };
            }
            if (!ActiveQueriesProgress_[queryId].Compiled) {
                return TAbortResult{
                    .YsonError = MessageToYtErrorYson(Format("Query %v is not compiled", queryId)),
                };
            }

            program = ActiveQueriesProgress_[queryId].Program;
        }

        try {
            program->Abort().GetValueSync();
        } catch (...) {
            return TAbortResult{
                .YsonError = MessageToYtErrorYson(Format("Failed to abort query %v: %v", queryId, CurrentExceptionMessage())),
            };
        }

        return {};
    }

    void OnDynamicConfigChanged(TYqlPluginDynamicConfig config) noexcept override
    {
        YQL_LOG(INFO) << "Dynamic config update started";

        NYson::TProtobufWriterOptions protobufWriterOptions;
        protobufWriterOptions.ConvertSnakeToCamelCase = true;

        NYql::TGatewaysConfig dynamicGatewaysConfig;
        dynamicGatewaysConfig.ParseFromStringOrThrow(NYson::YsonStringToProto(
            config.GatewaysConfig,
            NYson::ReflectProtobufMessageType<NYql::TGatewaysConfig>(),
            protobufWriterOptions));

        // Ignore TDqGatewayConfig without DqManagerConfig_.
        if (!DqManagerConfig_) {
            dynamicGatewaysConfig.ClearDq();
        }

        auto newGatewaysConfig = GatewaysConfigInitial_;
        newGatewaysConfig.MergeFrom(dynamicGatewaysConfig);

        auto dynamicConfig = CreateDynamicConfig(std::move(newGatewaysConfig));
        {
            auto guard = WriterGuard(DynamicConfigSpinLock);
            DynamicConfig_ = dynamicConfig;
        }
        YQL_LOG(INFO) << "Dynamic config update finished";
    }

private:
    const TDqManagerConfigPtr DqManagerConfig_;
    TDqManagerPtr DqManager_;
    NYql::TFileStoragePtr FileStorage_;
    TDynamicConfigPtr DynamicConfig_;
    NYql::TGatewaysConfig GatewaysConfigInitial_;
    THashMap<TString, TString> Modules_;
    TYsonString OperationAttributes_;
    TString YqlAgentToken_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ProgressSpinLock);
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, DynamicConfigSpinLock);
    THashMap<TQueryId, TActiveQuery> ActiveQueriesProgress_;
    TUserDataTable UserDataTable_;

    std::optional<TActiveQuery> ExtractQuery(TQueryId queryId) {
        // NB: TProgram destructor must be called without locking.
        std::optional<TActiveQuery> query;
        auto guard = WriterGuard(ProgressSpinLock);
        auto it = ActiveQueriesProgress_.find(queryId);
        if (it != ActiveQueriesProgress_.end()) {
            query = std::move(it->second);
            ActiveQueriesProgress_.erase(it);
        }
        return query;
    }

    static TString PatchQueryAttributes(TYsonString configAttributes, TYsonString querySettings)
    {
        auto querySettingsMap = NodeFromYsonString(querySettings.ToString());
        auto resultAttributesMap = NodeFromYsonString(configAttributes.ToString());

        for (const auto& item : querySettingsMap.AsMap()) {
            resultAttributesMap[item.first] = item.second;
        }

        return NodeToYsonString(resultAttributesMap);
    }

    static NYql::TUserDataTable FilesToUserTable(const std::vector<TQueryFile>& files)
    {
        NYql::TUserDataTable table;

        for (const auto& file : files) {
            NYql::TUserDataBlock& block = table[NYql::TUserDataKey::File(NYql::GetDefaultFilePrefix() + file.Name)];

            block.Data = file.Content;
            switch (file.Type) {
                case EQueryFileContentType::RawInlineData: {
                    block.Type = NYql::EUserDataType::RAW_INLINE_DATA;
                    break;
                }
                case EQueryFileContentType::Url: {
                    block.Type = NYql::EUserDataType::URL;
                    break;
                }
                default: {
                    ythrow yexception() << "Unexpected file content type";
                }
            }
        }

        return table;
    }

    TDynamicConfigPtr CreateDynamicConfig(NYql::TGatewaysConfig&& gatewaysConfig) const {
        YQL_LOG(DEBUG) << "Creating dynamic config";

        auto dynamicConfig = New<TDynamicConfig>();
        dynamicConfig->GatewaysConfig = std::move(gatewaysConfig);
        auto* gatewayYtConfig = dynamicConfig->GatewaysConfig.MutableYt();

        gatewayYtConfig->SetMrJobBinMd5(MD5::File(gatewayYtConfig->GetMrJobBin()));
        YQL_LOG(DEBUG) << "Creating dynamic config: SetMrJobBinMd5 ready";

        for (const auto& mapping : gatewayYtConfig->GetClusterMapping()) {
            dynamicConfig->Clusters.insert({mapping.name(), TString(NYql::YtProviderName)});
            if (mapping.GetDefault()) {
                dynamicConfig->DefaultCluster = mapping.name();
            }
        }
        YQL_LOG(DEBUG) << "Creating dynamic config: Clusters ready";

        dynamicConfig->FuncRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(
            NKikimr::NMiniKQL::CreateBuiltinRegistry())->Clone();

        const NKikimr::NMiniKQL::TUdfModuleRemappings emptyRemappings;
        dynamicConfig->FuncRegistry->SetBackTraceCallback(&NYql::NBacktrace::KikimrBackTrace);
        YQL_LOG(DEBUG) << "Creating dynamic config: SetBackTraceCallback ready";

        TVector<TString> udfPaths;
        NKikimr::NMiniKQL::FindUdfsInDir(gatewayYtConfig->GetMrJobUdfsDir(), &udfPaths);
        YQL_LOG(DEBUG) << "Creating dynamic config: FindUdfsInDir ready";

        for (const auto& path : udfPaths) {
            // Skip YQL plugin shared library itself, it is not a UDF.
            if (path.EndsWith("libyqlplugin.so")) {
                continue;
            }
            ui32 flags = 0;
            // System Python UDFs are not used locally so we only need types.
            if (path.Contains("systempython") && path.Contains(TString("udf") + MKQL_UDF_LIB_SUFFIX)) {
                flags |= NUdf::IRegistrator::TFlags::TypesOnly;
            }
            dynamicConfig->FuncRegistry->LoadUdfs(path, emptyRemappings, flags);
            if (DqManagerConfig_) {
                DqManagerConfig_->UdfsWithMd5.emplace(path, MD5::File(path));
            }
        }
        YQL_LOG(DEBUG) << "Creating dynamic config: LoadUdfs ready";

        gatewayYtConfig->ClearMrJobUdfsDir();

        NKikimr::NMiniKQL::TUdfModulePathsMap systemModules;
        for (const auto& m : dynamicConfig->FuncRegistry->GetAllModuleNames()) {
            TMaybe<TString> path = dynamicConfig->FuncRegistry->FindUdfPath(m);
            if (!path) {
                YQL_LOG(FATAL) << "Unable to detect UDF path for module " << m;
                exit(1);
            }
            systemModules.emplace(m, *path);
        }
        YQL_LOG(DEBUG) << "Creating dynamic config: FindUdfPath ready";

        dynamicConfig->FuncRegistry->SetSystemModulePaths(systemModules);
        YQL_LOG(DEBUG) << "Creating dynamic config: SetSystemModulePaths ready";

        TModulesTable modulesTable;
        if (!CompileLibraries(UserDataTable_, dynamicConfig->ExprContext, modulesTable, true)) {
            TStringStream err;
            dynamicConfig->ExprContext.IssueManager
                .GetIssues()
                .PrintTo(err);
            YQL_LOG(FATAL) << "Failed to compile modules:\n"
                           << err.Str();
            exit(1);
        }
        YQL_LOG(DEBUG) << "Creating dynamic config: CompileLibraries ready";

        dynamicConfig->ModuleResolver = std::make_shared<NYql::TModuleResolver>(std::move(modulesTable), dynamicConfig->ExprContext.NextUniqueId, dynamicConfig->Clusters, THashSet<TString>{});
        YQL_LOG(DEBUG) << "Creating dynamic config: ModuleResolver ready";

        YQL_LOG(DEBUG) << "Creating dynamic config: done";
        return std::move(dynamicConfig);
    }

    NYql::TProgramFactoryPtr CreateProgramFactory(TDynamicConfig& dynamicConfig) {
        YQL_LOG(DEBUG) << "Creating program factory";

        NYql::TYtNativeServices ytServices;
        ytServices.FunctionRegistry = dynamicConfig.FuncRegistry.Get();
        ytServices.FileStorage = FileStorage_;
        ytServices.Config = std::make_shared<NYql::TYtGatewayConfig>(*dynamicConfig.GatewaysConfig.MutableYt());

        TVector<NYql::TDataProviderInitializer> dataProvidersInit;
        if (DqManagerConfig_) {
            auto dqGateway = NYql::CreateDqGateway("localhost", DqManagerConfig_->GrpcPort);
            auto dqCompFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory({
                NYql::GetCommonDqFactory(),
                NYql::GetDqYtFactory(),
                NKikimr::NMiniKQL::GetYqlFactory(),
            });
            dataProvidersInit.push_back(GetDqDataProviderInitializer(NYql::CreateDqExecTransformerFactory(MakeIntrusive<TSkiffConverter>()), dqGateway, dqCompFactory, {}, FileStorage_));
        }

        auto ytNativeGateway = CreateYtNativeGateway(ytServices);
        dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(ytNativeGateway, NDq::MakeCBOOptimizerFactory(), MakeDqHelper()));
        YQL_LOG(DEBUG) << "Creating program factory: dataProvidersInit ready";

        auto factory = MakeIntrusive<NYql::TProgramFactory>(
            false, dynamicConfig.FuncRegistry.Get(), dynamicConfig.ExprContext.NextUniqueId, dataProvidersInit, "embedded");
        factory->AddUserDataTable(UserDataTable_);
        factory->SetCredentials(MakeIntrusive<NYql::TCredentials>());
        factory->SetModules(dynamicConfig.ModuleResolver);
        factory->SetUdfResolver(NYql::NCommon::CreateSimpleUdfResolver(dynamicConfig.FuncRegistry.Get(), FileStorage_));
        factory->SetGatewaysConfig(&dynamicConfig.GatewaysConfig);
        factory->SetFileStorage(FileStorage_);
        factory->SetUrlPreprocessing(MakeIntrusive<NYql::TUrlPreprocessing>(dynamicConfig.GatewaysConfig));

        YQL_LOG(DEBUG) << "Creating program factory: done";
        return std::move(factory);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IYqlPlugin> CreateYqlPlugin(TYqlPluginOptions options) noexcept
{
    return std::make_unique<NNative::TYqlPlugin>(std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
