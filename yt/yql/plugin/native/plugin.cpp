#include "dq_manager.h"
#include "dq_gateway_with_offloading.h"
#include "plugin.h"

#include "provider_load.h"
#include "secret_masker.h"

#include <yt/yql/plugin/lib/error_helpers.h>
#include <yt/yql/plugin/lib/progress_merger.h>

#include <yt/yql/providers/yt/common/yql_names.h>
#include <yt/yql/providers/yt/comp_nodes/dq/dq_yt_factory.h>
#include <yt/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <yt/yql/providers/yt/lib/access_provider/full/yt_access_provider.h>
#include <yt/yql/providers/yt/lib/log/yt_logger.h>
#include <yt/yql/providers/yt/lib/res_pull/res_or_pull.h>
#include <yt/yql/providers/yt/lib/row_spec/yql_row_spec.h>
#include <yt/yql/providers/yt/lib/schema/schema.h>
#include <yt/yql/providers/yt/lib/skiff/yql_skiff_schema.h>
#include <yt/yql/providers/yt/lib/tvm_client/full/tvm_client.h>
#include <yt/yql/providers/yt/lib/yt_download/yt_download.h>
#include <yt/yql/providers/yt/lib/yt_url_lister/yt_url_lister.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider.h>

#include <yql/essentials/parser/pg_wrapper/interface/comp_factory.h>

#include <yql/essentials/providers/common/codec/yql_codec_type_flags.h>
#include <yql/essentials/providers/common/codec/yql_codec.h>
#include <yql/essentials/providers/common/comp_nodes/yql_factory.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/udf_resolve/yql_simple_udf_resolver.h>

#include <contrib/ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <yt/yql/providers/dq/gateway/yql_dq_gateway_factory.h>
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
#include <yql/essentials/core/url_lister/url_lister_manager.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/utils/backtrace/backtrace.h>
#include <yql/essentials/utils/log/context.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/sql/v1/translation/sql.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/concurrency/coroutine.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/program/config.h>
#include <yt/yt/library/program/helpers.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>

#include <yt/yt/core/misc/fs.h>
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

#include <util/generic/ptr.h>
#include <util/system/env.h>
#include <util/system/fs.h>
#include <util/thread/pool.h>

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

TString CalculateMD5Checksum(const TString& filename)
{
    if (GetEnv("YT_LOCAL") == "1") {
        return MD5::Calc(filename);
    }

    return MD5::File(filename);
}

////////////////////////////////////////////////////////////////////////////////

struct TQueryRepresentations
    : public TRefCounted
{
    std::optional<TString> Plan;
    std::optional<TString> Ast;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ReprSpinLock);
};
DECLARE_REFCOUNTED_TYPE(TQueryRepresentations)
DEFINE_REFCOUNTED_TYPE(TQueryRepresentations)

////////////////////////////////////////////////////////////////////////////////

class TQueryPipelineConfigurator
    : public NYql::IPipelineConfigurator
    , public TRefCounted
{
public:
    const TQueryRepresentationsPtr Repr;

    TQueryPipelineConfigurator(NYql::TProgramPtr program)
        : Repr(New<TQueryRepresentations>())
        , Program_(std::move(program))
    { }

    void AfterCreate(NYql::TTransformationPipeline* /*pipeline*/) const override
    { }

    void AfterTypeAnnotation(NYql::TTransformationPipeline* /*pipeline*/) const override
    { }

    void AfterOptimize(NYql::TTransformationPipeline* pipeline) const override
    {
        auto transformer = [this](NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext& /*ctx*/) {
            output = input;

            auto guard = WriterGuard(Repr->ReprSpinLock);
            Repr->Plan = MaybeToOptional(Program_->GetQueryPlan());
            Repr->Ast = MaybeToOptional(Program_->GetQueryAst());

            return NYql::IGraphTransformer::TStatus::Ok;
        };

        pipeline->Add(NYql::CreateFunctorTransformer(transformer), "PlanAndAstOutput");
    }

private:
    NYql::TProgramPtr Program_;
};
DECLARE_REFCOUNTED_TYPE(TQueryPipelineConfigurator)
DEFINE_REFCOUNTED_TYPE(TQueryPipelineConfigurator)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicConfig
    : public TRefCounted
{
    NYql::TGatewaysConfig GatewaysConfig;
    THashMap<TString, TString> Clusters;
    THashMap<TString, TString> ClusterAddresses;
    std::optional<TString> DefaultCluster;
    NYql::TExprContext ExprContext;
    NYql::IModuleResolver::TPtr ModuleResolver;
};
DECLARE_REFCOUNTED_TYPE(TDynamicConfig)
DEFINE_REFCOUNTED_TYPE(TDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

enum class EAbortState
{
    Pending,
    Started,
    Finished
};

struct TActiveQuery
{
    // Store shared data for TProgram after dyn config changing.
    TDynamicConfigPtr ProgramSharedData;
    NYql::TProgramFactoryPtr ProgramFactory;

    NYql::TProgramPtr Program;

    bool Started = false;
    bool Compiled = false;
    bool ExecutionStarted = false;
    bool Finished = false;

    std::optional<EAbortState> AbortState;

    TProgressMerger ProgressMerger;
    TQueryPipelineConfiguratorPtr PipelineConfigurator;
    std::optional<TString> Plan;
    std::optional<TString> Ast;
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

namespace {

void MergeRepeatedFields(google::protobuf::Message& message);

void MergeRepeatedFields(google::protobuf::Message& message, const google::protobuf::FieldDescriptor& field)
{
    const auto fieldMessageType = field.message_type();
    if (!fieldMessageType) {
        // Plain field - nothing to merge.
        return;
    }

    const auto reflection = message.GetReflection();
    if (!field.is_repeated()) {
        if (reflection->HasField(message, &field)) {
            const auto submessage = reflection->MutableMessage(&message, &field);
            MergeRepeatedFields(*submessage);
        }
        return;
    }

    const auto nameField = fieldMessageType->FindFieldByName("Name");
    if (!nameField || nameField->type() != google::protobuf::FieldDescriptor::TYPE_STRING) {
        // Skip messages without 'string Name' field.
        return;
    }

    const int repeatedCount = reflection->FieldSize(message, &field);
    if (!repeatedCount) {
        return;
    }

    THashMap<TString, int> map;
    for (int i = 0; i < repeatedCount; ++i) {
        const auto& nextMessage = reflection->GetRepeatedMessage(message, &field, i);
        const TString name = nextMessage.GetReflection()->GetString(nextMessage, nameField);
        const auto it = map.find(name);
        if (it != map.end()) {
            const auto uniqueMessage = reflection->MutableRepeatedMessage(&message, &field, it->second);
            uniqueMessage->MergeFrom(nextMessage);
            // Could have more repeated submessages with 'Name', so we need to merge them too.
            MergeRepeatedFields(*uniqueMessage);
        } else {
            const int unique = map.ysize();
            if (unique != i) {
                // Move first values with unique names to the left (leave removed ones on the right).
                reflection->SwapElements(&message, &field, unique, i);
            }
            map[name] = unique;
        }
    }
    for (int i = map.ysize(); i < repeatedCount; ++i) {
        reflection->RemoveLast(&message, &field);
    }
}

void MergeRepeatedFields(google::protobuf::Message& message)
{
    const auto description = message.GetDescriptor();
    for (int i = 0; i < description->field_count(); ++i) {
        const auto field = description->field(i);
        MergeRepeatedFields(message, *field);
    }
}

void MergeGatewaysConfig(NYql::TGatewaysConfig& target, const NYql::TGatewaysConfig& source)
{
    target.MergeFrom(source);
    MergeRepeatedFields(target);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TYqlPlugin
    : public IYqlPlugin
{
public:
    TYqlPlugin(TYqlNativePluginOptions options)
        : DqManagerConfig_(options.DqManagerConfig ? NYTree::ConvertTo<TDqManagerConfigPtr>(options.DqManagerConfig) : nullptr)
        , StartDqManager_(options.StartDqManager)
    {
        try {
            NYql::NLog::InitLogger(std::move(options.LogBackend));

            auto& logger = NYql::NLog::YqlLogger();

            logger.SetDefaultPriority(ELogPriority::TLOG_DEBUG);
            for (int i = 0; i < NYql::NLog::TComponentHelpers::ToInt(NYql::NLog::EComponent::MaxValue); ++i) {
                logger.SetComponentLevel(NYql::NLog::EComponent(i), NYql::NLog::ELevel::DEBUG);
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

                DqGatewayOffloadThreadPool_ = MakeHolder<TThreadPool>();
            }

            auto* gatewayYtConfig = GatewaysConfigInitial_.MutableYt();
            gatewayYtConfig->ParseFromStringOrThrow(NYson::YsonStringToProto(
                options.GatewayConfig,
                NYson::ReflectProtobufMessageType<NYql::TYtGatewayConfig>(),
                protobufWriterOptions));

            auto* gatewayYtflowConfig = GatewaysConfigInitial_.MutableYtflow();
            gatewayYtflowConfig->ParseFromStringOrThrow(NYson::YsonStringToProto(
                options.YtflowGatewayConfig,
                NYson::ReflectProtobufMessageType<NYql::TYtflowGatewayConfig>(),
                protobufWriterOptions));

            auto* gatewayPqConfig = GatewaysConfigInitial_.MutablePq();
            gatewayPqConfig->ParseFromStringOrThrow(NYson::YsonStringToProto(
                options.PqGatewayConfig,
                NYson::ReflectProtobufMessageType<NYql::TPqGatewayConfig>(),
                protobufWriterOptions));

            auto* gatewaySolomonConfig = GatewaysConfigInitial_.MutableSolomon();
            gatewaySolomonConfig->ParseFromStringOrThrow(NYson::YsonStringToProto(
                options.SolomonGatewayConfig,
                NYson::ReflectProtobufMessageType<NYql::TSolomonGatewayConfig>(),
                protobufWriterOptions));

            NYql::TFileStorageConfig fileStorageConfig;
            fileStorageConfig.ParseFromStringOrThrow(NYson::YsonStringToProto(
                options.FileStorageConfig,
                NYson::ReflectProtobufMessageType<NYql::TFileStorageConfig>(),
                protobufWriterOptions));

            FileStorage_ = WithAsync(CreateFileStorage(fileStorageConfig, {MakeYtDownloader(fileStorageConfig)}));

            NYql::TYtTvmConfig tvmConfig;
            tvmConfig.ParseFromStringOrThrow(NYson::YsonStringToProto(
                options.TvmConfig,
                NYson::ReflectProtobufMessageType<NYql::TYtTvmConfig>(),
                protobufWriterOptions));

            TvmClient_ = CreateTvmClient(tvmConfig);

            NYql::TYtAccessProviderConfig ytAccessProviderConfig;
            ytAccessProviderConfig.ParseFromStringOrThrow(NYson::YsonStringToProto(
                options.YtAccessProviderConfig,
                NYson::ReflectProtobufMessageType<NYql::TYtAccessProviderConfig>(),
                protobufWriterOptions));

            YtAccessProvider_ = CreateYtAccessProvider(TvmClient_, ytAccessProviderConfig);

            FuncRegistry_ = NKikimr::NMiniKQL::CreateFunctionRegistry(
                NKikimr::NMiniKQL::CreateBuiltinRegistry())->Clone();
            const NKikimr::NMiniKQL::TUdfModuleRemappings emptyRemappings;
            FuncRegistry_->SetBackTraceCallback(&NYql::NBacktrace::KikimrBackTrace);
            TVector<TString> udfPaths;
            NKikimr::NMiniKQL::FindUdfsInDir(gatewayYtConfig->GetMrJobUdfsDir(), &udfPaths);
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
                FuncRegistry_->LoadUdfs(path, emptyRemappings, flags);
                if (DqManagerConfig_) {
                    DqManagerConfig_->UdfsWithMd5.emplace(path, CalculateMD5Checksum(path));
                }
            }
            gatewayYtConfig->ClearMrJobUdfsDir();
            NKikimr::NMiniKQL::TUdfModulePathsMap systemModules;
            for (const auto& m : FuncRegistry_->GetAllModuleNames()) {
                TMaybe<TString> path = FuncRegistry_->FindUdfPath(m);
                if (!path) {
                    YQL_LOG(FATAL) << "Unable to detect UDF path for module " << m;
                    exit(1);
                }
                systemModules.emplace(m, *path);
            }
            FuncRegistry_->SetSystemModulePaths(systemModules);

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

            DynamicConfig_.Store(CreateDynamicConfig(NYql::TGatewaysConfig(GatewaysConfigInitial_)));

            if (options.YTTokenPath) {
                TFsPath path(options.YTTokenPath);
                YqlAgentToken_ = TIFStream(path).ReadAll();
            } else if (!NYT::TConfig::Get()->Token.empty()) {
                YqlAgentToken_ = NYT::TConfig::Get()->Token;
            }

            // do not use token from .yt/token or env in queries
            NYT::TConfig::Get()->Token = {};

            TLangVersionBuffer buf;
            TStringBuf versionStringBuf;

            ParseLangVersion(options.MaxYqlLangVersion, MaxYqlLangVersionInitial_);
            MaxYqlLangVersion_ = MaxYqlLangVersionInitial_;
            YQL_LOG(INFO) << Format("Maximum supported YQL version is set (Version: %v)", options.MaxYqlLangVersion);

            DefaultYqlApiLangVersion_ = MinLangVersion;
            FormatLangVersion(DefaultYqlApiLangVersion_, buf, versionStringBuf);
            YQL_LOG(INFO) << Format("Default YQL version for API and CLI is set (Version: %v)", versionStringBuf);

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
        if (DqManager_ && StartDqManager_) {
            DqManager_->Start();
        }
        if (DqManager_) {
            // This pool is required for all DQ queries
            DqGatewayOffloadThreadPool_->Start(1);
        }
    }

    TClustersResult GuardedGetUsedClusters(
        TString queryText,
        TYsonString settings,
        std::vector<TQueryFile> files)
    {
        auto dynamicConfig = DynamicConfig_.Acquire();
        auto fictionalQueryId = TQueryId::Create();
        auto factory = CreateProgramFactory(fictionalQueryId, *dynamicConfig);
        auto program = factory->Create("-memory-", queryText, ToString(fictionalQueryId));

        program->AddCredentials({
            {"default_yt", NYql::TCredential("yt", "", YqlAgentToken_)},
            {"default_ytflow", NYql::TCredential("ytflow", "", YqlAgentToken_)}
        });

        program->SetOperationAttrsYson(PatchQueryAttributes(OperationAttributes_, settings));

        auto defaultQueryClusterForUserArtifacts = dynamicConfig->DefaultCluster;
        auto ysonSettings = NodeFromYsonString(settings.ToString()).AsMap();
        if (auto cluster = ysonSettings.FindPtr("cluster")) {
            defaultQueryClusterForUserArtifacts = cluster->AsString();
        }

        SetProgramYqlVersion(program, ysonSettings);

        auto userDataTable = FilesToUserTable(files);
        program->AddUserDataTable(userDataTable);

        NSQLTranslation::TTranslationSettings sqlSettings;
        sqlSettings.ClusterMapping = dynamicConfig->Clusters;
        sqlSettings.ModuleMapping = Modules_;
        if (defaultQueryClusterForUserArtifacts) {
            sqlSettings.DefaultCluster = *defaultQueryClusterForUserArtifacts;
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

        std::vector<std::pair<TString, TString>> clustersList;
        clustersList.reserve(usedClusters->size());

        THashMap<TString, TString> clusters;
        {
            auto dynamicConfig = DynamicConfig_.Acquire();
            clusters = dynamicConfig->ClusterAddresses;
        }

        // Default cluster for execution. It may differ from default cluster for user artifacts
        if (dynamicConfig->DefaultCluster) {
            usedClusters->insert(*dynamicConfig->DefaultCluster);
        }

        if (defaultQueryClusterForUserArtifacts) {
            // Default cluster must be first in list.
            usedClusters->erase(*defaultQueryClusterForUserArtifacts);
            clustersList.emplace_back(std::pair<TString, TString>{std::move(*defaultQueryClusterForUserArtifacts), clusters[*defaultQueryClusterForUserArtifacts]});
        }

        for (const auto& cluster : *usedClusters) {
            clustersList.emplace_back(std::pair<TString, TString>{cluster, clusters[cluster]});
        }

        return TClustersResult{
            .Clusters = std::move(clustersList),
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
        auto dynamicConfig = DynamicConfig_.Acquire();
        auto factory = CreateProgramFactory(queryId, *dynamicConfig);
        factory->SetUrlListerManager(MakeUrlListerManager({MakeYtUrlLister()}));
        auto [program, sqlSettings] = CreateProgramAndSqlSettingsFromParameters(
            queryId,
            queryText,
            settings,
            credentialsStr,
            dynamicConfig,
            factory);

        auto pipelineConfigurator = New<TQueryPipelineConfigurator>(program);
        {
            auto guard = WriterGuard(ProgressSpinLock_);

            auto& activeQuery = GetActiveQuery(queryId);
            YT_VERIFY(!activeQuery.Started);
            YT_VERIFY(!activeQuery.Program);

            activeQuery.ProgramSharedData = dynamicConfig;
            activeQuery.ProgramFactory = factory;
            activeQuery.Program = program;
            activeQuery.PipelineConfigurator = pipelineConfigurator;
            activeQuery.Started = true;
        }

        auto userDataTable = FilesToUserTable(files);
        program->AddUserDataTable(userDataTable);

        auto queryRepr = pipelineConfigurator->Repr;
        program->SetProgressWriter([queryRepr, queryId, this] (const NYql::TOperationProgress& progress) {
            std::optional<TString> plan;
            std::optional<TString> ast;
            {
                auto guard = ReaderGuard(queryRepr->ReprSpinLock);
                plan.swap(queryRepr->Plan);
                ast.swap(queryRepr->Ast);
            }

            auto guard = WriterGuard(ProgressSpinLock_);
            if (auto* activeQuery = GetActiveQueryPtr(queryId)) {
                activeQuery->ProgressMerger.MergeWith(progress);
                if (plan) {
                    activeQuery->Plan.swap(plan);
                }

                if (ast) {
                    activeQuery->Ast.swap(ast);
                }
            }
        });

        if (DqManager_) {
            sqlSettings.DqDefaultAuto = NSQLTranslation::ISqlFeaturePolicy::MakeAlwaysAllow();
        }

        program->SetOperationId(ToString(queryId));
        program->SetOperationUrl(sqlSettings.DefaultCluster);
        program->SetAuthenticatedUser(user);

        auto settingsMap = NodeFromYsonString(settings.ToString()).AsMap();
        if (auto parameters = settingsMap.FindPtr("declared_parameters")) {
            program->SetParametersYson(parameters->AsString());
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
            auto guard = WriterGuard(ProgressSpinLock_);

            auto& activeQuery = GetActiveQuery(queryId);
            activeQuery.Compiled = true;
        }

        ::NThreading::TFuture<NYql::TProgram::TStatus> futureStatus;

        // NYT::NYqlClient::EExecuteMode (yt/yt/ytlib/yql_client/public.h)
        switch (executeMode) {
        case 0: // Validate.
            futureStatus = program->ValidateAsync(user, nullptr);
            break;
        case 1: // Optimize.
            futureStatus = program->OptimizeAsyncWithConfig(user, *pipelineConfigurator);
            break;
        case 2: // Run.
            futureStatus = program->RunAsyncWithConfig(user, *pipelineConfigurator);
            break;
        default: // Unknown.
            return TQueryResult{
                .YsonError = MessageToYtErrorYson(
                    Format("Unknown execution mode: %v", executeMode)),
            };
        }

        std::optional<EAbortState> abortState;

        {
            auto guard = WriterGuard(ProgressSpinLock_);

            auto& activeQuery = GetActiveQuery(queryId);
            activeQuery.ExecutionStarted = true;
            abortState = activeQuery.AbortState;
        }

        if (abortState) {
            auto abortQueryResult = DoAbort(std::move(queryId), std::move(program));
            return TQueryResult{
                .YsonError = abortQueryResult.YsonError,
            };
        }

        auto status = WaitExecution(program, futureStatus);

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

        TString progress;

        {
            auto guard = ReaderGuard(ProgressSpinLock_);

            auto& activeQuery = GetActiveQuery(queryId);
            progress = activeQuery.ProgressMerger.ToYsonString();
        }

        YQL_LOG(DEBUG) << "Query " << ToString(queryId) << " finished successfully";

        return {
            .YsonResult = result.Empty() ? std::nullopt : std::make_optional(result.Str()),
            .Plan = MaybeToOptional(program->GetQueryPlan()),
            .Statistics = MaybeToOptional(program->GetStatistics()),
            .Progress = progress,
            .TaskInfo = MaybeToOptional(program->GetTasksInfo()),
            .Ast = MaybeToOptional(program->GetQueryAst()),
        };
    }

    TGetDeclaredParametersInfoResult GetDeclaredParametersInfo(
        TQueryId queryId,
        TString user,
        TString queryText,
        TYsonString settingsStr,
        TYsonString credentialsStr) override
    {
        auto dynamicConfig = DynamicConfig_.Acquire();
        auto factory = CreateProgramFactory(queryId, *dynamicConfig);
        auto [program, sqlSettings] = CreateProgramAndSqlSettingsFromParameters(
            queryId,
            queryText,
            settingsStr,
            credentialsStr,
            dynamicConfig,
            factory);

        if (!program->ParseSql(sqlSettings)) {
            ythrow yexception() << IssuesToYtErrorYson(program->Issues());
        }

        if (!program->Compile(user, true)) {
            ythrow yexception() << IssuesToYtErrorYson(program->Issues());
        }

        if (!program->ExtractQueryParametersMetadata()) {
            ythrow yexception() << IssuesToYtErrorYson(program->Issues());
        }

        return TGetDeclaredParametersInfoResult{
            .YsonParameters = program->GetExtractedQueryParametersMetadataYson(),
        };
    }

    TClustersResult GetUsedClusters(
        TQueryId /*queryId*/,
        TString queryText,
        TYsonString settings,
        std::vector<TQueryFile> files) noexcept override
    {
        TClustersResult result;

        auto coroutine = NConcurrency::TCoroutine<void()>(
            BIND([&](NConcurrency::TCoroutine<void()>& /*self*/){
                try {
                    result = GuardedGetUsedClusters(queryText, settings, files);
                } catch (const std::exception& ex) {
                    result = TClustersResult{
                        .YsonError = MessageToYtErrorYson(ex.what()),
                    };
                }
            }),
            NConcurrency::EExecutionStackKind::Large);

        coroutine.Run();
        YT_VERIFY(coroutine.IsCompleted());

        return result;
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
        TQueryResult result;

        auto coroutine = NConcurrency::TCoroutine<void()>(
            BIND([&](NConcurrency::TCoroutine<void()>& /*self*/){
                auto finalCleaning = Finally([&] {
                    auto guard = WriterGuard(ProgressSpinLock_);

                    auto& activeQuery = GetActiveQuery(queryId);
                    activeQuery.Finished = true;

                    YQL_LOG(DEBUG) << "Query " << ToString(queryId) << " finished";
                });

                try {
                    result = GuardedRun(queryId, user, credentials, queryText, settings, files, executeMode);
                } catch (const std::exception& ex) {
                    YQL_LOG(DEBUG) << "Query " << ToString(queryId) << " finished with errors";
                    result = TQueryResult{
                        .YsonError = MessageToYtErrorYson(ex.what()),
                    };
                }
            }),
            NConcurrency::EExecutionStackKind::Large);

        coroutine.Run();
        YT_VERIFY(coroutine.IsCompleted());

        return result;
    }

    TQueryResult GetProgress(TQueryId queryId) noexcept override
    {
        auto guard = ReaderGuard(ProgressSpinLock_);
        if (auto* activeQuery = GetActiveQueryPtr(queryId)) {
            TQueryResult result;
            if (activeQuery->ProgressMerger.HasChangesSinceLastFlush()) {
                result.Plan = activeQuery->Plan;
                result.Progress = activeQuery->ProgressMerger.ToYsonString();
                result.Ast = activeQuery->Ast;
            }
            return result;
        } else {
            return TQueryResult{
                .YsonError = MessageToYtErrorYson(Format("No progress for queryId: %v", queryId)),
            };
        }
    }

    /* TODO(ngc224): retry aborts from QT, it will allow to simplify current messy logic.
     *
     * Currently we have several tools to properly handle aborts:
     * - AbortState in active query allows to preserve aborts which happen at inappropriate time
     *   (before program's execution start - we cannot abort query's parsing / compilation)
     * - RegisterQuery injects query into active queries on yql agent's StartQuery start, thus
     *   allowing us to preserve also aborts before plugin's Run call
     *
     * To keep things not overly complicated, Abort call returns error whenever it cannot
     * start sync abort or guarantee that it already completed.
     *
     * Regardless of returned error, actual abort will wait until program's execution start,
     * and then it will be performed.
     */
    TAbortResult Abort(TQueryId queryId) noexcept override
    {
        NYql::TProgramPtr program;
        {
            auto guard = WriterGuard(ProgressSpinLock_);
            auto* activeQuery = GetActiveQueryPtr(queryId);

            if (!activeQuery) {
                return TAbortResult{
                    .YsonError = MessageToYtErrorYson(
                        Format("Query %v is not found", queryId)),
                };
            }

            if (activeQuery->AbortState) {
                if (auto abortResult = ValidateAbortState(*activeQuery, queryId)) {
                    return *abortResult;
                }
            }

            activeQuery->AbortState = EAbortState::Pending;

            if (!activeQuery->Started) {
                return TAbortResult{
                    .YsonError = MessageToYtErrorYson(
                        Format("Query %v is not started", queryId)),
                };
            }

            if (!activeQuery->Compiled) {
                return TAbortResult{
                    .YsonError = MessageToYtErrorYson(
                        Format("Query %v is not compiled", queryId)),
                };
            }

            if (!activeQuery->ExecutionStarted) {
                return TAbortResult{
                    .YsonError = MessageToYtErrorYson(
                        Format("Query %v execution is not started", queryId)),
                };
            }

            program = activeQuery->Program;
        }

        return DoAbort(std::move(queryId), std::move(program));
    }

    TAbortResult DoAbort(TQueryId queryId, NYql::TProgramPtr program)
    {
        // Method is called outside of guards, so it may be reached
        // simultaneously by Abort and Run branches, hence more locking
        // is needed..
        {
            auto guard = WriterGuard(ProgressSpinLock_);

            auto& activeQuery = GetActiveQuery(queryId);
            if (auto abortResult = ValidateAbortState(activeQuery, queryId)) {
                return *abortResult;
            }

            activeQuery.AbortState = EAbortState::Started;
        }

        try {
            YQL_LOG(DEBUG) << "Query " << ToString(queryId) << " is aborting";
            program->Abort().GetValueSync();
            YQL_LOG(DEBUG) << "Query " << ToString(queryId) << " is aborted";

            auto guard = WriterGuard(ProgressSpinLock_);

            // This section may be called after query unregistration,
            // hence additional check is required.
            if (auto* activeQuery = GetActiveQueryPtr(queryId)) {
                activeQuery->AbortState = EAbortState::Finished;
            }
        } catch (...) {
            return TAbortResult{
                .YsonError = MessageToYtErrorYson(
                    Format("Failed to abort query %v: %v", queryId, CurrentExceptionMessage())),
            };
        }

        return {};
    }

    void OnDynamicConfigChanged(TYqlPluginDynamicConfig config) noexcept override
    {
        YQL_LOG(INFO) << "Dynamic config update started";
        YQL_LOG(DEBUG) << __FUNCTION__ << ": config.GatewaysConfig = " << config.GatewaysConfig.AsStringBuf();

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
        MergeGatewaysConfig(newGatewaysConfig, dynamicGatewaysConfig);

        YQL_LOG(DEBUG) << __FUNCTION__ << ": GatewaysConfigInitial_ = " << GatewaysConfigInitial_.ShortDebugString();
        YQL_LOG(DEBUG) << __FUNCTION__ << ": dynamicGatewaysConfig = " << dynamicGatewaysConfig.ShortDebugString();
        YQL_LOG(DEBUG) << __FUNCTION__ << ": newGatewaysConfig = " << newGatewaysConfig.ShortDebugString();

        DynamicConfig_.Store(CreateDynamicConfig(std::move(newGatewaysConfig)));

        if (!config.MaxSupportedYqlVersion) {
            MaxYqlLangVersion_ = MaxYqlLangVersionInitial_;
        } else {
            const auto maxVersionStr = config.MaxSupportedYqlVersion.ToString();
            YQL_LOG(DEBUG) << __FUNCTION__ << ": config.MaxSupportedYqlVersion = " << maxVersionStr;
            TLangVersion maxVersion;
            if (ParseLangVersion(maxVersionStr, maxVersion)) {
                MaxYqlLangVersion_ = maxVersion;
            } else {
                YQL_LOG(DEBUG) << __FUNCTION__ << ": cannot parse config.MaxSupportedYqlVersion";
                MaxYqlLangVersion_ = MaxYqlLangVersionInitial_;
            }
        }
        YQL_LOG(INFO) << "Dynamic config update finished";
    }

    void RegisterQuery(TQueryId queryId) noexcept override
    {
        auto guard = WriterGuard(ProgressSpinLock_);
        // There should not be known query before registration.
        YT_VERIFY(!ActiveQueriesProgress_.contains(queryId));

        ActiveQueriesProgress_[queryId] = TActiveQuery{
            .Started = false,
        };
    }

    void UnregisterQuery(TQueryId queryId) noexcept override
    {
        // NB: TProgram destructor must be called without locking.
        std::optional<TActiveQuery> activeQuery;

        {
            auto guard = WriterGuard(ProgressSpinLock_);

            auto iterator = ActiveQueriesProgress_.find(queryId);
            YT_VERIFY(iterator != ActiveQueriesProgress_.end());

            activeQuery = std::move(iterator->second);
            ActiveQueriesProgress_.erase(iterator);

            YQL_LOG(DEBUG) << "Query " << ToString(queryId) << " is removed";
        }

        // Unregister may happen before query run (query is not started yet)
        // or after execution complete (successful or not).
        YT_VERIFY(!activeQuery->Started || activeQuery->Finished);
    }

private:
    const TDqManagerConfigPtr DqManagerConfig_;
    const bool StartDqManager_;
    TDqManagerPtr DqManager_;
    THolder<IThreadPool> DqGatewayOffloadThreadPool_;
    NYql::TFileStoragePtr FileStorage_;
    ::TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FuncRegistry_;
    TAtomicIntrusivePtr<TDynamicConfig> DynamicConfig_;
    NYql::TGatewaysConfig GatewaysConfigInitial_;
    THashMap<TString, TString> Modules_;
    TYsonString OperationAttributes_;
    TString YqlAgentToken_;
    NYql::ITvmClient::TPtr TvmClient_;
    NYql::IYtAccessProvider::TPtr YtAccessProvider_;

    std::atomic<TLangVersion> MaxYqlLangVersion_;
    TLangVersion MaxYqlLangVersionInitial_;
    TLangVersion DefaultYqlApiLangVersion_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ProgressSpinLock_);
    THashMap<TQueryId, TActiveQuery> ActiveQueriesProgress_;
    TUserDataTable UserDataTable_;

    // NOTE: must be called under locked ProgressSpinLock_
    TActiveQuery* GetActiveQueryPtr(TQueryId queryId)
    {
        auto iterator = ActiveQueriesProgress_.find(queryId);
        if (iterator != ActiveQueriesProgress_.end()) {
            return &iterator->second;
        }

        return nullptr;
    }

    // NOTE: must be called under locked ProgressSpinLock_
    TActiveQuery& GetActiveQuery(TQueryId queryId)
    {
        auto* activeQuery = GetActiveQueryPtr(queryId);
        YT_VERIFY(activeQuery);
        return *activeQuery;
    }

    std::optional<TAbortResult> ValidateAbortState(
        const TActiveQuery& activeQuery,
        const TQueryId& queryId)
    {
        YT_VERIFY(activeQuery.AbortState);

        switch (*activeQuery.AbortState) {
            case EAbortState::Started:
                return TAbortResult{
                    .YsonError = MessageToYtErrorYson(
                        Format("Query %v is already being aborted", queryId)),
                };

            case EAbortState::Finished:
                return TAbortResult{};

            case EAbortState::Pending:
                break;
        }

        return std::nullopt;
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
        YQL_LOG(DEBUG) << __FUNCTION__ << ": start";

        auto dynamicConfig = New<TDynamicConfig>();
        dynamicConfig->GatewaysConfig = std::move(gatewaysConfig);
        auto* gatewayYtConfig = dynamicConfig->GatewaysConfig.MutableYt();
        auto* gatewayPqConfig = dynamicConfig->GatewaysConfig.MutablePq();
        auto* gatewaySolomonConfig = dynamicConfig->GatewaysConfig.MutableSolomon();

        // Ignore MrJobUdfsDir in dynamic config (we won't reload udfs and won't restart DqManager_).
        gatewayYtConfig->ClearMrJobUdfsDir();
        YQL_LOG(DEBUG) << __FUNCTION__ << ": TDynamicConfig ready";

        gatewayYtConfig->SetMrJobBinMd5(CalculateMD5Checksum(gatewayYtConfig->GetMrJobBin()));
        YQL_LOG(DEBUG) << __FUNCTION__ << ": SetMrJobBinMd5 ready";

        for (const auto& mapping : gatewayYtConfig->GetClusterMapping()) {
            dynamicConfig->Clusters.insert({mapping.name(), TString(NYql::YtProviderName)});
            dynamicConfig->ClusterAddresses.insert({mapping.name(), mapping.cluster()});
            if (mapping.GetDefault()) {
                dynamicConfig->DefaultCluster = mapping.name();
            }
        }
        for (const auto& mapping : gatewayPqConfig->GetClusterMapping()) {
            dynamicConfig->Clusters.insert({mapping.name(), TString(NYql::PqProviderName)});
            dynamicConfig->ClusterAddresses.insert({mapping.name(), mapping.endpoint()});
        }
        for (const auto& mapping : gatewaySolomonConfig->GetClusterMapping()) {
            dynamicConfig->Clusters.insert({mapping.name(), TString(NYql::SolomonProviderName)});
            dynamicConfig->ClusterAddresses.insert({mapping.name(), mapping.cluster()});
        }
        YQL_LOG(DEBUG) << __FUNCTION__ << ": Clusters ready";

        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
        NSQLTranslationV1::TParsers parsers;
        parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
        parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();

        NSQLTranslation::TTranslators translators(
            nullptr,
            NSQLTranslationV1::MakeTranslator(lexers, parsers),
            NSQLTranslationPG::MakeTranslator()
        );

        TModulesTable modulesTable;
        if (!CompileLibraries(translators, UserDataTable_, dynamicConfig->ExprContext, modulesTable, true)) {
            TStringStream err;
            dynamicConfig->ExprContext.IssueManager
                .GetIssues()
                .PrintTo(err);
            YQL_LOG(FATAL) << "Failed to compile modules:\n" << err.Str();
            exit(1);
        }
        YQL_LOG(DEBUG) << __FUNCTION__ << ": CompileLibraries ready";

        dynamicConfig->ModuleResolver = std::make_shared<NYql::TModuleResolver>(
            translators,
            std::move(modulesTable),
            dynamicConfig->ExprContext.NextUniqueId,
            dynamicConfig->Clusters,
            NSQLTranslation::TExtendedSqlFlags{});

        YQL_LOG(DEBUG) << __FUNCTION__ << ": ModuleResolver ready";

        YQL_LOG(DEBUG) << __FUNCTION__ << ": done";
        return std::move(dynamicConfig);
    }

    NYql::TProgramFactoryPtr CreateProgramFactory(TQueryId queryId, TDynamicConfig& dynamicConfig) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(ToString(queryId));

        YQL_LOG(DEBUG) << __FUNCTION__ << ": start";

        NYql::TYtNativeServices ytServices;
        ytServices.FunctionRegistry = FuncRegistry_.Get();
        ytServices.FileStorage = FileStorage_;
        ytServices.Config = std::make_shared<NYql::TYtGatewayConfig>(dynamicConfig.GatewaysConfig.GetYt());
        ytServices.SecretMasker = CreateSecretMasker();
        ytServices.TvmClient = TvmClient_;
        ytServices.YtAccessProvider = YtAccessProvider_;

        TVector<NYql::TDataProviderInitializer> dataProvidersInit;
        if (DqManagerConfig_) {
            auto dqGateway = NYql::CreateDqGateway("localhost", DqManagerConfig_->GrpcPort);
            // NOTE: prevent deadlock upon thread joining
            // details in https://st.yandex-team.ru/YT-26302
            auto dqGatewayWithOffloading = CreateDqGatewayWithOffloading(
                std::move(dqGateway),
                DqGatewayOffloadThreadPool_.Get());

            auto dqCompFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory({
                NYql::GetCommonDqFactory(),
                NYql::GetDqYtFactory(),
                NKikimr::NMiniKQL::GetYqlFactory(),
                GetPgFactory()
            });

            dataProvidersInit.push_back(
                GetDqDataProviderInitializer(
                    NYql::CreateDqExecTransformerFactory(MakeIntrusive<TSkiffConverter>()),
                    std::move(dqGatewayWithOffloading),
                    std::move(dqCompFactory),
                    {},
                    FileStorage_));
        }

        auto ytNativeGateway = CreateYtNativeGateway(ytServices);
        dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(ytNativeGateway, NDq::MakeCBOOptimizerFactory(), MakeDqHelper()));

        ExtProviderSpecific(
            dynamicConfig.GatewaysConfig,
            FuncRegistry_.Get(),
            dataProvidersInit,
            FileStorage_);

        YQL_LOG(DEBUG) << __FUNCTION__ << ": dataProvidersInit ready";

        auto factory = MakeIntrusive<NYql::TProgramFactory>(
            false, FuncRegistry_.Get(), dynamicConfig.ExprContext.NextUniqueId, dataProvidersInit, "yql-agent");
        factory->AddUserDataTable(UserDataTable_);
        factory->SetCredentials(MakeIntrusive<NYql::TCredentials>());
        factory->SetModules(dynamicConfig.ModuleResolver);
        factory->SetUdfResolver(NYql::NCommon::CreateSimpleUdfResolver(FuncRegistry_.Get(), FileStorage_));
        factory->SetGatewaysConfig(&dynamicConfig.GatewaysConfig);
        factory->SetFileStorage(FileStorage_);
        factory->SetUrlPreprocessing(MakeIntrusive<NYql::TUrlPreprocessing>(dynamicConfig.GatewaysConfig));

        YQL_LOG(DEBUG) << __FUNCTION__ << ": done";
        return std::move(factory);
    }

    void SetProgramYqlVersion(TProgramPtr program, TNode::TMapType& settingsMap) {
        program->SetMaxLanguageVersion(MaxYqlLangVersion_);
        if (auto version = settingsMap.FindPtr("yql_version")) {
            TLangVersion parsedVersion;
            if (ParseLangVersion(version->AsString(), parsedVersion)) {
                program->SetLanguageVersion(parsedVersion);
            } else {
                ythrow yexception() << Format("Invalid YQL language version (Version: %v)", version->AsString());
            }
        } else {
            program->SetLanguageVersion(DefaultYqlApiLangVersion_);
        }
    }

    std::pair<TProgramPtr, NSQLTranslation::TTranslationSettings> CreateProgramAndSqlSettingsFromParameters(
        TQueryId queryId,
        const TString& queryText,
        const TYsonString& settingsStr,
        const TYsonString& credentialsStr,
        TDynamicConfigPtr dynamicConfig,
        TProgramFactoryPtr factory)
    {
        auto program = factory->Create("-memory-", queryText, ToString(queryId));

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

        program->SetOperationAttrsYson(PatchQueryAttributes(OperationAttributes_, settingsStr));

        auto defaultQueryCluster = dynamicConfig->DefaultCluster;
        auto settingsMap = NodeFromYsonString(settingsStr.ToString()).AsMap();
        if (auto cluster = settingsMap.FindPtr("cluster")) {
            defaultQueryCluster = cluster->AsString();
        }

        SetProgramYqlVersion(program, settingsMap);

        NSQLTranslation::TTranslationSettings sqlSettings;
        sqlSettings.ClusterMapping = dynamicConfig->Clusters;
        sqlSettings.ModuleMapping = Modules_;
        if (defaultQueryCluster) {
            sqlSettings.DefaultCluster = *defaultQueryCluster;
        }
        sqlSettings.SyntaxVersion = 1;
        sqlSettings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;

        return {program, sqlSettings};
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IYqlPlugin> CreateYqlPlugin(TYqlNativePluginOptions options) noexcept
{
    return std::make_unique<NNative::TYqlPlugin>(std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
