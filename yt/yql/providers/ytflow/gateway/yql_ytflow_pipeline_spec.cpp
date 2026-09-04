#include "yql_ytflow_pipeline_spec.h"
#include "yql_ytflow_mkql_compiler.h"
#include "yql_ytflow_schema.h"
#include "yql_ytflow_utils.h"

#include <library/cpp/yt/memory/new.h>
#include <library/cpp/yt/string/format.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_user_data_storage.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>
#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/schema/expr/yql_expr_schema.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/resources/public.h>

#include <yt/yql/providers/ytflow/common/yql_ytflow_constants.h>
#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>
#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_integration.h>
#include <yt/yql/providers/ytflow/integration/proto/yt.pb.h>
#include <yt/yql/providers/ytflow/integration/proto/pq.pb.h>
#include <yt/yql/providers/ytflow/integration/proto/solomon.pb.h>
#include <yt/yql/providers/ytflow/lambda_builder/yql_ytflow_lambda_builder.h>
#include <yt/yql/providers/ytflow/provider/yql_ytflow_constants.h>
#include <yt/yql/providers/ytflow/provider/yql_ytflow_utils.h>
#include <yt/yql/providers/ytflow/job/yql_ytflow_source_transformer.h>

#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <google/protobuf/any.pb.h>


namespace NYql::NYtflow::NPrivate {

using namespace NNodes;

namespace {

const NYT::NFlow::TResourceId FunctionRegistryResourceId("yql-function-registry");

constexpr TStringBuf ComputationPatternResourceAlias = "computation_pattern";
constexpr TStringBuf FunctionRegistryDependencyAlias = "function_registry";

constexpr const char* ComputationPatternResourceClassName =
    "NYql::NYtflow::TComputationPatternResource";
constexpr const char* FunctionRegistryResourceClassName =
    "NYql::NYtflow::TFunctionRegistryResource";

constexpr int ComputationPatternResourceRecipeVersion = 1;
constexpr int FunctionRegistryResourceRecipeVersion = 1;

TString PrettyPrintLambda(
    const TExprNode& lambdaNode,
    TExprContext& ctx)
{
    auto ast = ConvertToAst(lambdaNode, ctx, TConvertToAstSettings{
        .AllowFreeArgs = true,
    });

    YQL_ENSURE(ast.Root);

    TStringStream stream;
    ast.Root->PrettyPrintTo(
        stream,
        TAstPrintFlags::PerLine |
            TAstPrintFlags::ShortQuote |
            TAstPrintFlags::AdaptArbitraryContent);

    return stream.Str();
}

void AddLambdaFiles(
    TStringBuf parameterName,
    const TString& fileName,
    TCoLambda lambda,
    const TVector<TLambdaArgument>& arguments,
    TYtflowLambdaBuilder& lambdaBuilder,
    const NCommon::IMkqlCallableCompiler& compiler,
    NYT::NFlow::TComputationSpecPtr computationSpec,
    TBuildPipelineSpecContext& ctx)
{
    auto serializedNode = lambdaBuilder.BuildLambdaWithIO(
        compiler,
        lambda,
        arguments,
        ctx.ExprContext,
        ctx.RunOptions.Types()->LangVer,
        ctx.RunOptions.Types()->RuntimeSettings);

    computationSpec->Parameters->AddChild(
        parameterName, NYT::NYTree::ConvertToNode(fileName));

    ctx.Files.push_back({
        .Name = fileName,
        .Content = std::move(serializedNode),
        .Disposition = EFileDisposition::InlineData
    });

    ctx.Files.push_back({
        .Name = fileName + ".yqls",
        .Content = PrettyPrintLambda(lambda.Ref(), ctx.ExprContext),
        .Disposition = EFileDisposition::InlineData
    });
}

} // anonymous namespace

namespace {

void EnsureFunctionRegistryResource(
    NYT::NFlow::TComputationSpecPtr computationSpec,
    NYT::NFlow::TPipelineSpecPtr pipelineSpec,
    TBuildPipelineSpecContext& ctx)
{
    const auto functionRegistryIt = pipelineSpec->Resources.find(FunctionRegistryResourceId);
    if (functionRegistryIt == pipelineSpec->Resources.end()) {
        auto functionRegistrySpec = NYT::New<NYT::NFlow::TResourceSpec>();
        functionRegistrySpec->ResourceClassName = FunctionRegistryResourceClassName;
        functionRegistrySpec->Parameters->AddChild(
            "recipe_version",
            NYT::NYTree::ConvertToNode(FunctionRegistryResourceRecipeVersion));
        functionRegistrySpec->Parameters->AddChild(
            "udf_paths",
            NYT::NYTree::ConvertToNode(BuildPipelineUdfPaths(ctx.UserDataBlocks)));

        pipelineSpec->Resources.emplace(
            FunctionRegistryResourceId,
            std::move(functionRegistrySpec));
    } else {
        YQL_ENSURE(
            functionRegistryIt->second->ResourceClassName == FunctionRegistryResourceClassName,
            "Resource ID collision: " << FunctionRegistryResourceId);
    }

    const auto requirementIt = computationSpec->RequiredResourceIds.find(
        FunctionRegistryResourceId);
    if (requirementIt == computationSpec->RequiredResourceIds.end()) {
        auto requirement = NYT::New<NYT::NFlow::TResourceDescription>();
        requirement->Alias = NYT::NFlow::TResourceId(FunctionRegistryDependencyAlias);
        requirement->Worker = true;
        requirement->Controller = false;
        computationSpec->RequiredResourceIds.emplace(
            FunctionRegistryResourceId,
            std::move(requirement));
    } else {
        YQL_ENSURE(
            requirementIt->second->Alias ==
                NYT::NFlow::TResourceId(FunctionRegistryDependencyAlias) &&
            requirementIt->second->Worker &&
            !requirementIt->second->Controller,
            "Required resource ID collision: " << FunctionRegistryResourceId);
    }
}

void AddComputationPatternResourceForLambda(
    const TString& computationName,
    TStringBuf lambdaFileParameterName,
    TStringBuf resourceIdComponent,
    TStringBuf computationPatternResourceAlias,
    NYT::NFlow::TComputationSpecPtr computationSpec,
    NYT::NFlow::TPipelineSpecPtr pipelineSpec,
    TBuildPipelineSpecContext& ctx)
{
    EnsureFunctionRegistryResource(computationSpec, pipelineSpec, ctx);

    const auto lambdaFile = computationSpec->Parameters->GetChildValueOrThrow<TString>(
        lambdaFileParameterName);
    const NYT::NFlow::TResourceId computationPatternResourceId(
        TStringBuilder()
            << computationName
            << "-" << resourceIdComponent << "-computation-pattern");

    auto functionRegistryDependency = NYT::New<NYT::NFlow::TResourceDescription>();
    functionRegistryDependency->Alias =
        NYT::NFlow::TResourceId(FunctionRegistryDependencyAlias);
    functionRegistryDependency->Worker = true;
    functionRegistryDependency->Controller = false;

    auto computationPatternSpec = NYT::New<NYT::NFlow::TResourceSpec>();
    computationPatternSpec->ResourceClassName = ComputationPatternResourceClassName;
    computationPatternSpec->Parameters->AddChild(
        "recipe_version",
        NYT::NYTree::ConvertToNode(ComputationPatternResourceRecipeVersion));
    computationPatternSpec->Parameters->AddChild(
        "lambda_file",
        NYT::NYTree::ConvertToNode(lambdaFile));
    computationPatternSpec->Parameters->AddChild(
        "lang_version",
        NYT::NYTree::ConvertToNode(ctx.RunOptions.Types()->LangVer));
    computationPatternSpec->Parameters->AddChild(
        "opt_llvm",
        NYT::NYTree::ConvertToNode(TString("OFF")));
    computationPatternSpec->Parameters->AddChild(
        "runtime_settings",
        NYT::NYTree::ConvertToNode(SerializeRuntimeSettingsToString(
            *ctx.RunOptions.Types()->RuntimeSettings)));
    computationPatternSpec->Dependencies.emplace(
        FunctionRegistryResourceId,
        std::move(functionRegistryDependency));

    auto [patternIterator, patternEmplaced] = pipelineSpec->Resources.emplace(
        computationPatternResourceId,
        std::move(computationPatternSpec));
    YQL_ENSURE(patternEmplaced, "Duplicate computation pattern resource: "
        << patternIterator->first);

    auto computationPatternRequirement = NYT::New<NYT::NFlow::TResourceDescription>();
    computationPatternRequirement->Alias =
        NYT::NFlow::TResourceId(computationPatternResourceAlias);
    computationPatternRequirement->Worker = true;
    computationPatternRequirement->Controller = false;

    auto [requirementIterator, requirementEmplaced] = computationSpec->RequiredResourceIds.emplace(
        computationPatternResourceId,
        std::move(computationPatternRequirement));
    YQL_ENSURE(requirementEmplaced, "Duplicate required resource: "
        << requirementIterator->first);
}

} // anonymous namespace

void AddComputationPatternResource(
    const TString& computationName,
    NYT::NFlow::TComputationSpecPtr computationSpec,
    NYT::NFlow::TPipelineSpecPtr pipelineSpec,
    TBuildPipelineSpecContext& ctx)
{
    if (!ctx.EnableComputationPatternResources) {
        return;
    }

    AddComputationPatternResourceForLambda(
        computationName,
        "lambda_file",
        "lambda",
        ComputationPatternResourceAlias,
        computationSpec,
        pipelineSpec,
        ctx);
}

void AddHoppingComputationPatternResources(
    const TString& computationName,
    NYT::NFlow::TComputationSpecPtr computationSpec,
    NYT::NFlow::TPipelineSpecPtr pipelineSpec,
    TBuildPipelineSpecContext& ctx)
{
    if (!ctx.EnableComputationPatternResources) {
        return;
    }

    AddComputationPatternResourceForLambda(
        computationName,
        "update_state_lambda_file",
        "update_state",
        UpdateStateComputationPatternResourceAlias,
        computationSpec,
        pipelineSpec,
        ctx);
    AddComputationPatternResourceForLambda(
        computationName,
        "postprocess_lambda_file",
        "postprocess",
        PostprocessComputationPatternResourceAlias,
        computationSpec,
        pipelineSpec,
        ctx);
}

////////////////////////////////////////////////////////////////////////////////

TVector<TString> BuildPipelineUdfPaths(const TUserDataTable& userDataBlocks)
{
    TVector<TString> udfPaths;
    for (const auto& [userDataKey, userDataBlock] : userDataBlocks) {
        if (userDataBlock.Usage.Test(EUserDataBlockUsage::Udf)) {
            udfPaths.push_back(TUserDataStorage::MakeRelativeName(userDataKey.Alias()));
        }
    }

    SortUnique(udfPaths);

    return udfPaths;
}
// duplicate identifiers to break dependencies from non opensource code
// TODO(ngc224): eliminate duplication
static const NYT::NFlow::TResourceId YdbDriverDefaultResourceId = "YdbDriver";
static const NYT::NFlow::TResourceId MoniumDriverDefaultResourceId = "MoniumDriver";

////////////////////////////////////////////////////////////////////////////////

TString GetToken(const TString& tokenName, const THashMap<TString, TString>& secureParams)
{
    if (!tokenName.empty()) {
        auto tokenIterator = secureParams.find(tokenName);
        YQL_ENSURE(tokenIterator != secureParams.end());

        const auto& token = tokenIterator->second;
        YQL_ENSURE(IsStructuredTokenJson(token));

        auto parser = NYql::CreateStructuredTokenParser(token);
        YQL_ENSURE(parser.HasIAMToken(), "Unexpected token type");

        return parser.GetIAMToken();
    }
    return TString();
}

////////////////////////////////////////////////////////////////////////////////

void ProcessSource(
    TExprBase source,
    TString /*computationName*/,
    NYT::NFlow::TComputationSpecPtr computationSpec,
    NYT::NFlow::TPipelineSpecPtr pipelineSpec,
    TRequestedCredentials& requestedCredentials,
    TBuildPipelineSpecContext& ctx)
{
    if (auto maybeOutput = source.Maybe<TYtflowOutput>()) {
        auto outputIndex = FromString<ui32>(maybeOutput.Cast().OutputIndex().Value());

        auto streamName = maybeOutput
            .Operation().Sinks().Item(outputIndex)
            .Cast<TYtflowSinkBase>().Name().StringValue();

        YQL_ENSURE(streamName, "Unnamed source");

        computationSpec->InputStreamIds.insert(NYT::NFlow::TStreamId(std::move(streamName)));
    } else if (auto maybePersistentSource = source.Maybe<TYtflowPersistentSource>()) {
        auto streamName = maybePersistentSource.Cast().Name().StringValue();
        YQL_ENSURE(streamName, "Unnamed source");

        auto sourceSpec = NYT::New<NYT::NFlow::TSourceSpec>();

        auto input = maybePersistentSource.Cast().Input();
        auto providerInput = input.Cast<TYtflowReadWrap>().Input();

        auto* ytflowIntegration = GetYtflowIntegration(
            providerInput.Ref(),
            *ctx.RunOptions.Types());

        YQL_ENSURE(ytflowIntegration);

        ::google::protobuf::Any settings;
        ytflowIntegration->FillSourceSettings(providerInput.Ref(), settings, ctx.ExprContext);
        YQL_ENSURE(settings.Is<NProto::TQYTSourceMessage>() || settings.Is<NProto::TPQSourceMessage>());

        auto resourceDescription = NYT::New<NYT::NFlow::TResourceDescription>();
        resourceDescription->Controller = true;
        resourceDescription->Worker = true;

        computationSpec->RequiredResourceIds[NYT::NFlow::YTClientFactoryDefaultResourceId] =
            resourceDescription;

        const auto& config = ctx.RunOptions.Config();
        auto finiteStreams = config->_FiniteStreams.Get();
        YQL_ENSURE(finiteStreams, "Ytflow._FiniteStreams system setting is not set");

        auto& parameters = sourceSpec->Parameters;
        parameters->AddChild(
            "finite", NYT::NYTree::ConvertToNode(*finiteStreams));

        if (settings.Is<NProto::TQYTSourceMessage>()) {
            sourceSpec->SourceClassName = "NYT::NFlow::TQueueSource";

            NProto::TQYTSourceMessage qytSourceSettings;
            settings.UnpackTo(&qytSourceSettings);

            YQL_ENSURE(
                ctx.ConfigClusters,
                "Ytflow cluster mapping is not configured");
            const auto& configClusters = *ctx.ConfigClusters;

            auto queueRichPath = NYT::NYPath::TRichYPath(
                CanonizeYtPath(qytSourceSettings.GetPath(), *config));
            queueRichPath.SetCluster(configClusters.GetRealName(
                qytSourceSettings.GetCluster()));

            parameters->AddChild(
                "queue_path", NYT::NYTree::ConvertToNode(queueRichPath));

            auto consumerRichPath = MakeYtConsumerRichPath(
                *config,
                configClusters);

            parameters->AddChild(
                "consumer_path", NYT::NYTree::ConvertToNode(consumerRichPath));

            parameters->AddChild(
                "source_type", NYT::NYTree::ConvertToNode(ESourceType::YT));
        } else if (settings.Is<NProto::TPQSourceMessage>()) {
            sourceSpec->SourceClassName = "NYT::NFlow::TLogbrokerSource";

            auto resourceSpec = NYT::New<NYT::NFlow::TResourceSpec>();
            resourceSpec->ResourceClassName = "NYT::NFlow::TYdbDriver";
            pipelineSpec->Resources[YdbDriverDefaultResourceId] = std::move(resourceSpec);

            computationSpec->RequiredResourceIds[YdbDriverDefaultResourceId] =
                resourceDescription;

            NProto::TPQSourceMessage pqSourceMessage;
            settings.UnpackTo(&pqSourceMessage);

            auto maybeConsumerPath = config->LogbrokerConsumerPath.Get();
            YQL_ENSURE(maybeConsumerPath, "Ytflow.LogbrokerConsumerPath pragma is not set");

            auto& parameters = sourceSpec->Parameters;

            parameters->AddChild(
                "endpoints", NYT::NYTree::ConvertToNode(pqSourceMessage.GetEndpoints()));

            if (pqSourceMessage.GetClusterType() != TPqClusterConfig::CT_PERS_QUEUE || pqSourceMessage.GetDatabase() != "/Root") {
                parameters->AddChild(
                    "database", NYT::NYTree::ConvertToNode(pqSourceMessage.GetDatabase()));
            }

            parameters->AddChild(
                "topic", NYT::NYTree::ConvertToNode(pqSourceMessage.GetTopicPath()));

            parameters->AddChild(
                "consumer", NYT::NYTree::ConvertToNode(maybeConsumerPath.GetRef()));

            parameters->AddChild(
                "source_type", NYT::NYTree::ConvertToNode(ESourceType::Logbroker));

            if (auto maybeToken = input.Cast<TYtflowReadWrap>().Token()) {
                auto tokenName = maybeToken.Cast().Name().StringValue();
                auto token = GetToken(tokenName, ctx.SecureParams);
                YQL_ENSURE(requestedCredentials.YdbToken.empty() || requestedCredentials.YdbToken == token);
                requestedCredentials.YdbToken = token;
            }
        } else {
            ythrow yexception() << "Unknown message type: " << settings.GetTypeName();
        }

        auto [iterator, emplaced] = computationSpec->SourceStreams.emplace(
            streamName, std::move(sourceSpec));

        YQL_ENSURE(emplaced, "Duplicate source stream: " << streamName);
    } else {
        ythrow yexception() << "Unsupported source callable: " << source.Ref().Content();
    }
}

void ProcessSink(
    TExprBase sink,
    TString /*computationName*/,
    NYT::NFlow::TComputationSpecPtr computationSpec,
    NYT::NFlow::TPipelineSpecPtr pipelineSpec,
    THashMap<TString, TString>& outputIndicesByOutputStreamId,
    TRequestedCredentials& requestedCredentials,
    TBuildPipelineSpecContext& ctx)
{
    auto sinkBase = sink.Cast<TYtflowSinkBase>();

    auto streamName = sinkBase.Name().StringValue();
    YQL_ENSURE(streamName, "Unnamed sink");

    const TTypeAnnotationNode* rowType = nullptr;

    auto streamSpec = NYT::New<NYT::NFlow::TStreamSpec>();
    if (auto maybeIntermediateSink = sinkBase.Maybe<TYtflowIntermediateSink>()) {
        rowType = maybeIntermediateSink.Cast().RowType().Ref()
            .GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        streamSpec->Schema = BuildTableSchema(rowType);
    } else if (auto maybePersistentSink = sinkBase.Maybe<TYtflowPersistentSink>()) {
        auto sinkSpec = NYT::New<NYT::NFlow::TSinkSpec>();

        sinkSpec->InputStreamIds.emplace(NYT::NFlow::TStreamId(streamName));

        auto resourceDescription = NYT::New<NYT::NFlow::TResourceDescription>();
        resourceDescription->Controller = true;
        resourceDescription->Worker = true;

        computationSpec->RequiredResourceIds[NYT::NFlow::YTClientFactoryDefaultResourceId] =
            std::move(resourceDescription);

        auto input = maybePersistentSink.Cast().Input();
        auto providerInput = input.Cast<TYtflowWriteWrap>().Input();

        auto* ytflowIntegration = GetYtflowIntegration(
            providerInput.Ref(),
            *ctx.RunOptions.Types());

        YQL_ENSURE(ytflowIntegration);

        ::google::protobuf::Any settings;
        ytflowIntegration->FillSinkSettings(providerInput.Ref(), settings, ctx.ExprContext);

        YQL_ENSURE(settings.Is<NProto::TQYTSinkMessage>() ||
            settings.Is<NProto::TPQSinkMessage>() ||
            settings.Is<NProto::TSolomonSinkMessage>());
        const auto& config = ctx.RunOptions.Config();
        if (settings.Is<NProto::TQYTSinkMessage>()) {
            sinkSpec->SinkClassName = "NYT::NFlow::TSyncQueueSink";

            NProto::TQYTSinkMessage qytSinkSettings;
            settings.UnpackTo(&qytSinkSettings);

            YQL_ENSURE(
                ctx.ConfigClusters,
                "Ytflow cluster mapping is not configured");
            const auto& configClusters = *ctx.ConfigClusters;

            auto queueRichPath = NYT::NYPath::TRichYPath(
                CanonizeYtPath(qytSinkSettings.GetPath(), *config));

            auto cluster = configClusters.GetRealName(
                qytSinkSettings.GetCluster());
            queueRichPath.SetCluster(cluster);

            auto& parameters = sinkSpec->Parameters;
            parameters->AddChild(
                "queue_path", NYT::NYTree::ConvertToNode(queueRichPath));

            auto producerPath = config->GetYtProducerPath();
            auto producerRichPath = CanonizeYtRichPath(
                std::move(producerPath), *config);

            if (auto producerCluster = producerRichPath.GetCluster()) {
                producerRichPath.SetCluster(
                    configClusters.GetRealName(TString(*producerCluster)));
            } else {
                producerRichPath.SetCluster(cluster);
            }

            parameters->AddChild(
                "producer_path", NYT::NYTree::ConvertToNode(producerRichPath));

            computationSpec->Sinks.emplace(NYT::NFlow::TSinkId(streamName), std::move(sinkSpec));

            rowType = sink.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
            streamSpec->Schema = ConvertToQueueWriteSchema(BuildTableSchema(rowType));
        } else if (settings.Is<NProto::TPQSinkMessage>()) {
            sinkSpec->SinkClassName = "NYT::NFlow::TLogbrokerSink";

            auto resourceDescription = NYT::New<NYT::NFlow::TResourceDescription>();
            resourceDescription->Controller = true;
            resourceDescription->Worker = true;

            auto resourceSpec = NYT::New<NYT::NFlow::TResourceSpec>();
            resourceSpec->ResourceClassName = "NYT::NFlow::TYdbDriver";
            pipelineSpec->Resources[YdbDriverDefaultResourceId] = std::move(resourceSpec);

            computationSpec->RequiredResourceIds[YdbDriverDefaultResourceId] =
                resourceDescription;

            NProto::TPQSinkMessage pqSinkMessage;
            settings.UnpackTo(&pqSinkMessage);

            auto& parameters = sinkSpec->Parameters;

            parameters->AddChild(
                "logbroker", NYT::NYTree::ConvertToNode(pqSinkMessage.GetEndpoint()));

            if (pqSinkMessage.GetClusterType() != TPqClusterConfig::CT_PERS_QUEUE || pqSinkMessage.GetDatabase() != "/Root") {
                parameters->AddChild(
                    "database", NYT::NYTree::ConvertToNode(pqSinkMessage.GetDatabase()));
            }

            parameters->AddChild(
                "topic", NYT::NYTree::ConvertToNode(pqSinkMessage.GetTopicPath()));

            rowType = sink.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
            auto schema = ConvertToQueueWriteSchema(BuildTableSchema(rowType));
            YQL_ENSURE(schema->GetColumnCount() == 1);

            parameters->AddChild(
                "payload_column", NYT::NYTree::ConvertToNode(schema->Columns()[0].Name()));

            if (auto codec = config->LogbrokerWriteCompressionCodec.Get()) {
                parameters->AddChild(
                    "codec", NYT::NYTree::ConvertToNode(codec.GetRef()));
            }

            if (auto compressionLevel = config->LogbrokerWriteCompressionLevel.Get()) {
                parameters->AddChild("compression_level", NYT::NYTree::ConvertToNode(compressionLevel.GetRef()));
            }

            streamSpec->Schema = std::move(schema);

            if (auto maybeToken = input.Cast<TYtflowWriteWrap>().Token()) {
                auto tokenName = maybeToken.Cast().Name().StringValue();
                auto token = GetToken(tokenName, ctx.SecureParams);
                YQL_ENSURE(requestedCredentials.YdbToken.empty() || requestedCredentials.YdbToken == token);
                requestedCredentials.YdbToken = token;
            }
        } else if (settings.Is<NProto::TSolomonSinkMessage>()) {
            // Wire the legacy `TSolomonSinkMessage` to the new gRPC monium sink
            // class.  Unpack the proto first so we can read Endpoint below.
            NProto::TSolomonSinkMessage solomonSinkMessage;
            settings.UnpackTo(&solomonSinkMessage);

            sinkSpec->SinkClassName = "NYT::NFlow::TMoniumSink";

            auto resourceSpec = NYT::New<NYT::NFlow::TResourceSpec>();
            resourceSpec->ResourceClassName = "NYT::NFlow::TMoniumDriver";

            // TODO: drop this workaround after upstream fix in solomon ytflow integration
            auto endpoint = solomonSinkMessage.GetEndpoint();
            if (endpoint.StartsWith("http://")) {
                endpoint = endpoint.substr(7);
            } else if (endpoint.StartsWith("https://")) {
                endpoint = endpoint.substr(8);
            }

            auto& resourceParameters = resourceSpec->Parameters;

            resourceParameters->AddChild(
                "endpoint", NYT::NYTree::ConvertToNode(endpoint));

            auto moniumDriverSecure = config->_MoniumDriverSecure.Get();
            YQL_ENSURE(moniumDriverSecure, "Ytflow._MoniumDriverSecure system setting is not set");

            resourceParameters->AddChild(
                "secure", NYT::NYTree::ConvertToNode(moniumDriverSecure.GetRef()));

            resourceParameters->AddChild(
                "auth_mode", NYT::NYTree::ConvertToNode("OAuthEnv"));

            if (auto codec = config->SolomonWriteCompressionCodec.Get()) {
                resourceParameters->AddChild(
                    "compression_algorithm", NYT::NYTree::ConvertToNode(codec.GetRef()));
            }

            pipelineSpec->Resources[MoniumDriverDefaultResourceId] = std::move(resourceSpec);

            auto resourceDescription = NYT::New<NYT::NFlow::TResourceDescription>();
            resourceDescription->Controller = true;
            resourceDescription->Worker = true;

            computationSpec->RequiredResourceIds[MoniumDriverDefaultResourceId] = resourceDescription;

            auto& parameters = sinkSpec->Parameters;

            parameters->AddChild(
                "project", NYT::NYTree::ConvertToNode(solomonSinkMessage.GetProject()));

            parameters->AddChild(
                "cluster", NYT::NYTree::ConvertToNode(solomonSinkMessage.GetCluster()));

            parameters->AddChild(
                "service", NYT::NYTree::ConvertToNode(solomonSinkMessage.GetService()));

            parameters->AddChild(
                "timestamp_column", NYT::NYTree::ConvertToNode(solomonSinkMessage.GetMetricTimestampColumn()));

            auto metrics = NYT::NYTree::GetEphemeralNodeFactory()->CreateList();
            for (const auto& metricParameters : solomonSinkMessage.GetMetrics()) {
                auto metric = NYT::NYTree::GetEphemeralNodeFactory()->CreateMap();
                metric->AddChild(
                    "metric_value_column", NYT::NYTree::ConvertToNode(metricParameters.GetMetricValueColumn()));

                metric->AddChild(
                    "metric_type", NYT::NYTree::ConvertToNode(metricParameters.GetMetricType()));

                metric->AddChild(
                    "labels", NYT::NYTree::ConvertToNode(solomonSinkMessage.GetLabelColumns()));

                metrics->AddChild(metric);
            }

            parameters->AddChild("metrics", metrics);

            auto metricNameLabel = config->MoniumMetricNameLabel.Get();
            if (!metricNameLabel) {
                metricNameLabel = config->SolomonMetricNameLabel.Get();
            }

            if (metricNameLabel) {
                parameters->AddChild(
                    "metric_name_label", NYT::NYTree::ConvertToNode(metricNameLabel.GetRef()));
            }

            auto skipMetricsWithNullTimestamp = config->MoniumSkipMetricsWithNullTimestamp.Get();
            if (!skipMetricsWithNullTimestamp) {
                skipMetricsWithNullTimestamp = config->SolomonSkipMetricsWithNullTimestamp.Get();
            }

            if (skipMetricsWithNullTimestamp) {
                parameters->AddChild(
                    "skip_metrics_with_null_timestamp",
                    NYT::NYTree::ConvertToNode(skipMetricsWithNullTimestamp.GetRef()));
            }

            parameters->AddChild("skip_null_labels", NYT::NYTree::ConvertToNode(false));

            if (auto rpcTimeout = config->_RpcTimeout.Get()) {
                parameters->AddChild(
                    "write_timeout", NYT::NYTree::ConvertToNode(rpcTimeout.GetRef()));
            }

            rowType = sink.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
            auto schema = ConvertToQueueWriteSchema(BuildTableSchema(rowType));
            streamSpec->Schema = std::move(schema);

            if (auto maybeToken = input.Cast<TYtflowWriteWrap>().Token()) {
                auto tokenName = maybeToken.Cast().Name().StringValue();
                auto token = GetToken(tokenName, ctx.SecureParams);
                YQL_ENSURE(requestedCredentials.MoniumToken.empty() || requestedCredentials.MoniumToken == token);
                requestedCredentials.MoniumToken = token;
            }
        } else {
            ythrow yexception() << "Unknown message type: " << settings.GetTypeName();
        }
        computationSpec->Sinks.emplace(NYT::NFlow::TSinkId(streamName), std::move(sinkSpec));
    } else {
        ythrow yexception() << "Unsupported sink callable: " << sink.Ref().Content();
    }

    auto [iterator, emplaced] = pipelineSpec->Streams.emplace(
        NYT::NFlow::TStreamId(streamName), std::move(streamSpec)
    );

    YQL_ENSURE(emplaced, "Duplicate stream: " << streamName);

    computationSpec->OutputStreamIds.insert(NYT::NFlow::TStreamId(streamName));
    outputIndicesByOutputStreamId.emplace(std::move(streamName), sinkBase.OutputIndex());
}

NYT::NTableClient::TTableSchemaPtr AppendInputMessageIdColumn(
    NYT::NTableClient::TTableSchemaPtr schema)
{
    auto columnSchemas = schema->Columns();
    columnSchemas.push_back(NYT::NTableClient::TColumnSchema(
        "$input_message_id", NYT::NTableClient::ESimpleLogicalValueType::String));

    return NYT::New<NYT::NTableClient::TTableSchema>(
        std::move(columnSchemas));
}

void ProcessOpBase(
    TYtflowOpBase /*opBase*/,
    TString /*computationName*/,
    NYT::NFlow::TComputationSpecPtr computationSpec,
    NYT::NFlow::TPipelineSpecPtr /*pipelineSpec*/,
    const THashMap<TString, TString>& outputIndicesByOutputStreamId,
    TBuildPipelineSpecContext& ctx)
{
    auto& parameters = computationSpec->Parameters;

    {
        TVector<TString> udfPaths;
        for (const auto& [userDataKey, userDataBlock]: ctx.UserDataBlocks) {
            if (userDataBlock.Usage.Test(EUserDataBlockUsage::Udf)) {
                udfPaths.push_back(
                    NYql::TUserDataStorage::MakeRelativeName(
                        userDataKey.Alias()));
            }
        }

        parameters->AddChild(
            "udf_paths", NYT::NYTree::ConvertToNode(udfPaths));
    }

    parameters->AddChild("output_indices_by_output_stream_id",
        NYT::NYTree::ConvertToNode(outputIndicesByOutputStreamId));

    parameters->AddChild("lang_version",
        NYT::NYTree::ConvertToNode(ctx.RunOptions.Types()->LangVer));

    parameters->AddChild(
        "opt_llvm",
        NYT::NYTree::ConvertToNode(TString("OFF")));

    parameters->AddChild(
        "runtime_settings",
        NYT::NYTree::ConvertToNode(SerializeRuntimeSettingsToString(
            *ctx.RunOptions.Types()->RuntimeSettings)));
}

void ProcessMapBase(
    TYtflowMapBase map,
    TString computationName,
    NYT::NFlow::TComputationSpecPtr computationSpec,
    NYT::NFlow::TPipelineSpecPtr pipelineSpec,
    const THashMap<TString, TString>& outputIndicesByOutputStreamId,
    TBuildPipelineSpecContext& ctx)
{
    ProcessOpBase(
        map,
        computationName,
        computationSpec,
        pipelineSpec,
        outputIndicesByOutputStreamId,
        ctx);

    auto& parameters = computationSpec->Parameters;

    if (HasSetting(map.Settings().Ref(), "inject_input_message_id")) {
        parameters->AddChild(
            "inject_input_message_id",
            NYT::NYTree::ConvertToNode(true));

        for (const auto& [streamId, outputIndex] : outputIndicesByOutputStreamId) {
            auto& streamSpec = pipelineSpec->Streams.at(
                NYT::NFlow::TStreamId(streamId));

            streamSpec->Schema = AppendInputMessageIdColumn(streamSpec->Schema);
        }
    }

    if (HasSetting(map.Settings().Ref(), EXTEND_SETTING)) {
        parameters->AddChild(
            TString(EXTEND_SETTING),
            NYT::NYTree::ConvertToNode(true));
    }

    {
        NKikimr::NMiniKQL::TScopedAlloc scopedAlloc(__LOCATION__);
        TYtflowLambdaBuilder lambdaBuilder(
            ctx.FunctionRegistry,
            scopedAlloc,
            /*env*/ nullptr,
            /*randomProvider*/ {},
            /*timeProvider*/ {},
            /*jobStats*/ nullptr,
            /*counters*/ nullptr,
            /*secureParamsProvider*/ nullptr,
            /*logProvider*/ nullptr,
            ctx.RunOptions.Types()->LangVer,
            ctx.RunOptions.Types()->RuntimeSettings);

        NYql::NCommon::TMkqlCommonCallableCompiler compiler;
        RegisterYtflowMkqlCompiler(
            compiler, *ctx.RunOptions.Types(), *ctx.RunOptions.Config());

        AddLambdaFiles(
            "lambda_file",
            computationName + "_lambda",
            map.Lambda(),
            {{"YtflowInputStream", ETypeAnnotationKind::Stream}},
            lambdaBuilder,
            compiler,
            computationSpec,
            ctx);
    }
}

void ProcessSourceMap(
    TYtflowSourceMap sourceMap,
    TString computationName,
    NYT::NFlow::TComputationSpecPtr computationSpec,
    NYT::NFlow::TPipelineSpecPtr pipelineSpec,
    const THashMap<TString, TString>& outputIndicesByOutputStreamId,
    TBuildPipelineSpecContext& ctx)
{
    ProcessMapBase(
        sourceMap,
        computationName,
        computationSpec,
        pipelineSpec,
        outputIndicesByOutputStreamId,
        ctx);

    computationSpec->ComputationClassName = "NYql::NYtflow::TSourceMap";

    auto& parameters = computationSpec->Parameters;

    {
        const auto& sources = sourceMap.Sources();
        YQL_ENSURE(sources.Size() == 1);
        const auto& source = sources.Item(0).Ref();

        auto schema = BuildTableSchema(
            source.GetTypeAnn()->Cast<TListExprType>()->GetItemType());

        parameters->AddChild(
            "source_schema", NYT::NYTree::ConvertToNode(schema));
    }
}

void ProcessGroupBySchema(
    TYtflowOpBase opBase,
    NYT::NFlow::TComputationSpecPtr computationSpec,
    const TVector<TString>& groupByColumns)
{
    YQL_ENSURE(!groupByColumns.empty());

    {
        const auto& sources = opBase.Sources();
        YQL_ENSURE(sources.Size() >= 1);
        const auto& source = sources.Item(0).Ref();

        auto schema = BuildTableSchema(
            source.GetTypeAnn()->Cast<TListExprType>()->GetItemType());

        for (const auto& column : groupByColumns) {
            if (column == "$input_message_id") {
                schema = AppendInputMessageIdColumn(schema);
                break;
            }
        }

        std::vector<NYT::NTableClient::TColumnSchema> groupByColumnSchemas;
        std::vector<std::string> hashExpressionParts;

        for (const auto& column : groupByColumns) {
            const auto& columnSchema = schema->GetColumnOrThrow(column);

            hashExpressionParts.push_back(NYT::Format("[%v]", columnSchema.Name()));
            groupByColumnSchemas.push_back(columnSchema);
        }

        groupByColumnSchemas.insert(
            groupByColumnSchemas.begin(),
            NYT::NTableClient::TColumnSchema(
                "hash", NYT::NTableClient::ESimpleLogicalValueType::Uint64)
            .SetRequired(true)
            .SetExpression(
                NYT::Format("farm_hash(%v)", JoinSeq(",", hashExpressionParts))));

        auto groupBySchema = NYT::New<NYT::NTableClient::TTableSchema>(
            std::move(groupByColumnSchemas));

        computationSpec->GroupBySchema = std::move(groupBySchema);
    }
}

void ProcessTransformBase(
    TYtflowOpBase opBase,
    TString /*computationName*/,
    NYT::NFlow::TComputationSpecPtr computationSpec,
    NYT::NFlow::TPipelineSpecPtr /*pipelineSpec*/,
    const TVector<TString>& groupByColumns,
    TBuildPipelineSpecContext& /*ctx*/)
{
    auto& parameters = computationSpec->Parameters;

    parameters->AddChild(
        "processing_mode", NYT::NYTree::ConvertToNode("exactly_once"));

    ProcessGroupBySchema(opBase, computationSpec, groupByColumns);
}

void ProcessTransformMap(
    TYtflowTransformMap transformMap,
    TString computationName,
    NYT::NFlow::TComputationSpecPtr computationSpec,
    NYT::NFlow::TPipelineSpecPtr pipelineSpec,
    const THashMap<TString, TString>& outputIndicesByOutputStreamId,
    TBuildPipelineSpecContext& ctx)
{
    ProcessMapBase(
        transformMap,
        computationName,
        computationSpec,
        pipelineSpec,
        outputIndicesByOutputStreamId,
        ctx);

    ProcessTransformBase(
        transformMap,
        computationName,
        computationSpec,
        pipelineSpec,
        ParseTupleOfAtoms(
            transformMap.GroupByColumns().Ref()),
        ctx);

    computationSpec->ComputationClassName = "NYql::NYtflow::TTransformMap";
}

void ProcessSwiftMap(
    TYtflowSwiftMap swiftMap,
    TString computationName,
    NYT::NFlow::TComputationSpecPtr computationSpec,
    NYT::NFlow::TPipelineSpecPtr pipelineSpec,
    const THashMap<TString, TString>& outputIndicesByOutputStreamId,
    TBuildPipelineSpecContext& ctx)
{
    ProcessMapBase(
        swiftMap,
        computationName,
        computationSpec,
        pipelineSpec,
        outputIndicesByOutputStreamId,
        ctx);

    ProcessGroupBySchema(
        swiftMap,
        computationSpec,
        ParseTupleOfAtoms(swiftMap.GroupByColumns().Ref()));

    computationSpec->ComputationClassName = "NYql::NYtflow::TSwiftMap";
}

void ProcessHoppingAggregate(
    TYtflowHoppingAggregate hoppingAggregate,
    TString computationName,
    NYT::NFlow::TComputationSpecPtr computationSpec,
    NYT::NFlow::TPipelineSpecPtr pipelineSpec,
    const THashMap<TString, TString>& outputIndicesByOutputStreamId,
    TBuildPipelineSpecContext& ctx)
{
    ProcessOpBase(
        hoppingAggregate,
        computationName,
        computationSpec,
        pipelineSpec,
        outputIndicesByOutputStreamId,
        ctx);

    auto keys = ParseTupleOfAtoms(
        hoppingAggregate.Keys().Ref());

    ProcessTransformBase(
        hoppingAggregate,
        computationName,
        computationSpec,
        pipelineSpec,
        keys,
        ctx);

    computationSpec->ComputationClassName = "NYql::NYtflow::THoppingAggregate";

    computationSpec->TimerStreams["timer"] = NYT::New<NYT::NFlow::TTimerSpec>();

    auto& parameters = computationSpec->Parameters;

    parameters->AddChild(
        "interval", NYT::NYTree::ConvertToNode(
            FromString<ui64>(hoppingAggregate.Interval().Value())));

    parameters->AddChild(
        "delay", NYT::NYTree::ConvertToNode(
            FromString<ui64>(hoppingAggregate.Delay().Value())));

    {
        NKikimr::NMiniKQL::TScopedAlloc scopedAlloc(__LOCATION__);
        TYtflowLambdaBuilder lambdaBuilder(
            ctx.FunctionRegistry,
            scopedAlloc,
            /*env*/ nullptr,
            /*randomProvider*/ {},
            /*timeProvider*/ {},
            /*jobStats*/ nullptr,
            /*counters*/ nullptr,
            /*secureParamsProvider*/ nullptr,
            /*logProvider*/ nullptr,
            ctx.RunOptions.Types()->LangVer,
            ctx.RunOptions.Types()->RuntimeSettings);

        NYql::NCommon::TMkqlCommonCallableCompiler compiler;
        RegisterYtflowMkqlCompiler(
            compiler, *ctx.RunOptions.Types(), *ctx.RunOptions.Config());

        AddLambdaFiles(
            "update_state_lambda_file",
            computationName + "_update_state_lambda",
            hoppingAggregate.UpdateStateLambda(),
            {
                {"YtflowInputStream", ETypeAnnotationKind::Stream},
                {"YtflowInputState", ETypeAnnotationKind::List}
            },
            lambdaBuilder,
            compiler,
            computationSpec,
            ctx);

        AddLambdaFiles(
            "postprocess_lambda_file",
            computationName + "_postprocess_lambda",
            hoppingAggregate.PostprocessLambda(),
            {
                {"YtflowInputKey", Nothing()},
                {"YtflowInputState", ETypeAnnotationKind::List},
                {"YtflowInputMaxHopStartTime", ETypeAnnotationKind::Data}
            },
            lambdaBuilder,
            compiler,
            computationSpec,
            ctx);
    }

    AddHoppingComputationPatternResources(
        computationName,
        computationSpec,
        pipelineSpec,
        ctx);
}

} // namespace NYql::NYtflow::NPrivate

namespace NYql::NYtflow {

using namespace NNodes;

TBuildPipelineSpecContext::TBuildPipelineSpecContext(
    NPrepare::TContext& prepareCtx,
    THashMap<TStringBuf, ui32>& computationCounters,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const TUserDataTable& userDataBlocks,
    const THashMap<TString, TString>& secureParams)

    : TContext(prepareCtx)
    , ComputationCounters(computationCounters)
    , FunctionRegistry(functionRegistry)
    , UserDataBlocks(userDataBlocks)
    , SecureParams(secureParams)
    , EnableComputationPatternResources(
        prepareCtx.RunOptions.Config()->GetEnableComputationPatternResources())
{ }

TBuildPipelineSpecResult BuildPipelineSpec(
    TExprNode::TPtr node, TBuildPipelineSpecContext& ctx)
{
    TVector<TYtflowOpBase> operations;

    NPrivate::VisitExprCurrentEpoch(node, [&](const TExprNode::TPtr& child) {
        if (auto operation = TMaybeNode<TYtflowOpBase>(child)) {
            operations.push_back(operation.Cast());
        }

        return true;
    });

    auto pipelineSpec = NYT::New<NYT::NFlow::TPipelineSpec>();

    auto resourceSpec = NYT::New<NYT::NFlow::TResourceSpec>();
    resourceSpec->ResourceClassName = "NYT::NFlow::TYTClientFactory";
    pipelineSpec->Resources[NYT::NFlow::YTClientFactoryDefaultResourceId] = std::move(resourceSpec);

    TRequestedCredentials requestedCredentials;

    for (const auto& operation: operations) {
        auto operationType = operation.Ref().Content();
        operationType.ChopSuffix("!");

        auto computationSpec = NYT::New<NYT::NFlow::TComputationSpec>();
        auto computationName = TStringBuilder()
            << "computation_" << operationType
            << "_" << ctx.ComputationCounters[operationType]++;

        for (const auto& source: operation.Sources()) {
            NPrivate::ProcessSource(
                source, computationName, computationSpec,
                pipelineSpec, requestedCredentials, ctx);
        }

        THashMap<TString, TString> outputIndicesByOutputStreamId;
        for (const auto& sink: operation.Sinks()) {
            NPrivate::ProcessSink(
                sink, computationName, computationSpec,
                pipelineSpec, outputIndicesByOutputStreamId, requestedCredentials, ctx);
        }

        bool supportsComputationPatternResource = false;
        if (auto maybeSourceMap = operation.Maybe<TYtflowSourceMap>()) {
            NPrivate::ProcessSourceMap(
                maybeSourceMap.Cast(),
                computationName,
                computationSpec,
                pipelineSpec,
                outputIndicesByOutputStreamId,
                ctx);
            supportsComputationPatternResource = true;
        } else if (auto maybeTransformMap = operation.Maybe<TYtflowTransformMap>()) {
            NPrivate::ProcessTransformMap(
                maybeTransformMap.Cast(),
                computationName,
                computationSpec,
                pipelineSpec,
                outputIndicesByOutputStreamId,
                ctx);
            supportsComputationPatternResource = true;
        } else if (auto maybeSwiftMap = operation.Maybe<TYtflowSwiftMap>()) {
            NPrivate::ProcessSwiftMap(
                maybeSwiftMap.Cast(),
                computationName,
                computationSpec,
                pipelineSpec,
                outputIndicesByOutputStreamId,
                ctx);
            supportsComputationPatternResource = true;
        } else if (auto maybeHoppingAggregate = operation.Maybe<TYtflowHoppingAggregate>()) {
            NPrivate::ProcessHoppingAggregate(
                maybeHoppingAggregate.Cast(),
                computationName,
                computationSpec,
                pipelineSpec,
                outputIndicesByOutputStreamId,
                ctx);
        } else {
            YQL_ENSURE(
                false,
                "Unsupported operation: " << operation.Ref().Content());
        }

        if (supportsComputationPatternResource) {
            NPrivate::AddComputationPatternResource(
                computationName,
                computationSpec,
                pipelineSpec,
                ctx);
        }

        auto [computationIterator, computationEmplaced] = pipelineSpec->Computations.emplace(
            NYT::NFlow::TComputationId(computationName),
            std::move(computationSpec));
        YQL_ENSURE(computationEmplaced, "Duplicate computation: "
            << computationIterator->first);
    }

    return TBuildPipelineSpecResult{
        .PipelineSpec = std::move(pipelineSpec),
        .RequestedCredentials = std::move(requestedCredentials),
        .Files = std::move(ctx.Files)
    };
}

} // namespace NYql::NYtflow
