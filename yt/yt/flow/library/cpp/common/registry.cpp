#include "registry.h"

#include "computation.h"
#include "computation_controller.h"
#include "process_function.h"
#include "resource.h"
#include "sink.h"
#include "sink_controller.h"
#include "source.h"
#include "source_controller.h"
#include "spec.h"
#include "yson_message.h"

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/traverse.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <library/cpp/string_utils/levenshtein_diff/levenshtein_diff.h>

#include <util/charset/wide.h>

namespace NYT::NFlow {

using namespace NLogging;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TRegistry::TRegistry()
    : Logger(TLogger("FlowRegistry"))
{ }

TRegistry* TRegistry::Get()
{
    return Singleton<TRegistry>();
}

IComputationPtr TRegistry::CreateComputation(
    const TComputationContextPtr& context,
    const TDynamicComputationContextPtr& dynamicContext)
{
    return GetComputationDescriptor(context->ComputationSpec->ComputationClassName).Factory(context, dynamicContext);
}

IComputationControllerPtr TRegistry::CreateComputationController(
    const TComputationControllerContextPtr& context,
    const TDynamicComputationControllerContextPtr& dynamicContext)
{
    return GetComputationDescriptor(context->ComputationSpec->ComputationClassName).ControllerFactory(context, dynamicContext);
}

NYTree::TYsonStructPtr TRegistry::ParseComputationParameters(const TComputationSpecPtr& spec)
{
    auto parameters = GetComputationDescriptor(spec->ComputationClassName).ParametersFactory();
    parameters->Load(spec->Parameters);
    return parameters;
}

NYTree::TYsonStructPtr TRegistry::ParseDynamicComputationParameters(
    const TComputationSpecPtr& spec,
    const TDynamicComputationSpecPtr& dynamicSpec)
{
    auto dynamicParameters = GetComputationDescriptor(spec->ComputationClassName).DynamicParametersFactory();
    dynamicParameters->Load(dynamicSpec->Parameters);
    return dynamicParameters;
}

NYTree::TYsonStructPtr TRegistry::ParseDynamicComputationPartitionSpec(
    const TComputationSpecPtr& spec,
    const NYTree::IMapNodePtr& dynamicPartitionSpec)
{
    auto parsedDynamicPartitionSpec = GetComputationDescriptor(spec->ComputationClassName).DynamicPartitionSpecFactory();
    parsedDynamicPartitionSpec->Load(dynamicPartitionSpec);
    return parsedDynamicPartitionSpec;
}

ISourcePtr TRegistry::CreateSource(
    const TSourceContextPtr& context,
    const TDynamicSourceContextPtr& dynamicContext)
{
    return GetSourceDescriptor(context->SourceSpec->SourceClassName).Factory(context, dynamicContext);
}

ISourceControllerPtr TRegistry::CreateSourceController(
    const TSourceControllerContextPtr& context,
    const TDynamicSourceControllerContextPtr& dynamicContext)
{
    return GetSourceDescriptor(context->SourceSpec->SourceClassName).ControllerFactory(context, dynamicContext);
}

NYTree::TYsonStructPtr TRegistry::ParseSourceParameters(const TSourceSpecPtr& spec)
{
    auto parameters = GetSourceDescriptor(spec->SourceClassName).ParametersFactory();
    parameters->Load(spec->Parameters);
    return parameters;
}

NYTree::TYsonStructPtr TRegistry::ParseDynamicSourceParameters(
    const TSourceSpecPtr& spec,
    const TDynamicSourceSpecPtr& dynamicSpec)
{
    auto dynamicParameters = GetSourceDescriptor(spec->SourceClassName).DynamicParametersFactory();
    dynamicParameters->Load(dynamicSpec->Parameters);
    return dynamicParameters;
}

NYTree::TYsonStructPtr TRegistry::ParseDynamicSourcePartitionSpec(
    const TSourceSpecPtr& spec,
    const NYTree::IMapNodePtr& dynamicPartitionSpec)
{
    auto parsedDynamicPartitionSpec = GetSourceDescriptor(spec->SourceClassName).DynamicPartitionSpecFactory();
    parsedDynamicPartitionSpec->Load(dynamicPartitionSpec);
    return parsedDynamicPartitionSpec;
}

ISinkPtr TRegistry::CreateSink(
    const TSinkContextPtr& context,
    const TDynamicSinkContextPtr& dynamicContext)
{
    return GetSinkDescriptor(context->SinkSpec->SinkClassName).Factory(context, dynamicContext);
}

ISinkControllerPtr TRegistry::CreateSinkController(
    const TSinkControllerContextPtr& context,
    const TDynamicSinkControllerContextPtr& dynamicContext)
{
    return GetSinkDescriptor(context->SinkSpec->SinkClassName).ControllerFactory(context, dynamicContext);
}

NYTree::TYsonStructPtr TRegistry::ParseSinkParameters(const TSinkSpecPtr& spec)
{
    auto parameters = GetSinkDescriptor(spec->SinkClassName).ParametersFactory();
    parameters->Load(spec->Parameters);
    return parameters;
}

NYTree::TYsonStructPtr TRegistry::ParseDynamicSinkParameters(
    const TSinkSpecPtr& spec,
    const TDynamicSinkSpecPtr& dynamicSpec)
{
    auto dynamicParameters = GetSinkDescriptor(spec->SinkClassName).DynamicParametersFactory();
    dynamicParameters->Load(dynamicSpec->Parameters);
    return dynamicParameters;
}

IResourcePtr TRegistry::CreateResource(
    const TResourceContextPtr& context,
    const TDynamicResourceContextPtr& dynamicContext)
{
    return GetResourceDescriptor(context->ResourceSpec->ResourceClassName).Factory(context, dynamicContext);
}

TYsonMessagePtr TRegistry::CreateYsonMessage(TStringBuf name)
{
    return GetYsonMessageDescriptor(name).Factory();
}

NYTree::TYsonStructPtr TRegistry::ParseResourceParameters(const TResourceSpecPtr& spec)
{
    auto parameters = GetResourceDescriptor(spec->ResourceClassName).ParametersFactory();
    parameters->Load(spec->Parameters);
    return parameters;
}

NYTree::TYsonStructPtr TRegistry::ParseResourceDynamicParameters(
    const TResourceSpecPtr& spec,
    const TDynamicResourceSpecPtr& dynamicSpec)
{
    auto dynamicParameters = GetResourceDescriptor(spec->ResourceClassName).DynamicParametersFactory();
    if (dynamicParameters && dynamicSpec->Parameters) {
        dynamicParameters->Load(dynamicSpec->Parameters);
    }
    return dynamicParameters;
}

IExternalStateManagerPtr TRegistry::CreateExternalStateManager(
    const TExternalStateManagerContextPtr& context,
    const TDynamicExternalStateManagerContextPtr& dynamicContext)
{
    return GetExternalStateManagerDescriptor(context->ExternalStateManagerSpec->ExternalStateManagerClassName)
        .Factory(context, dynamicContext);
}

NYTree::TYsonStructPtr TRegistry::ParseExternalStateManagerParameters(
    const TExternalStateManagerSpecPtr& spec)
{
    auto parameters = GetExternalStateManagerDescriptor(spec->ExternalStateManagerClassName).ParametersFactory();
    if (spec->Parameters) {
        parameters->Load(spec->Parameters);
    }
    return parameters;
}

NYTree::TYsonStructPtr TRegistry::ParseDynamicExternalStateManagerParameters(
    const TExternalStateManagerSpecPtr& spec,
    const TDynamicExternalStateManagerSpecPtr& dynamicSpec)
{
    auto dynamicParameters = GetExternalStateManagerDescriptor(spec->ExternalStateManagerClassName).DynamicParametersFactory();
    if (dynamicSpec && dynamicSpec->Parameters) {
        dynamicParameters->Load(dynamicSpec->Parameters);
    }
    return dynamicParameters;
}

IExternalStateJoinerPtr TRegistry::CreateExternalStateJoiner(
    const TExternalStateJoinerContextPtr& context,
    const TDynamicExternalStateJoinerContextPtr& dynamicContext)
{
    return GetExternalStateJoinerDescriptor(context->ExternalStateJoinerSpec->ExternalStateJoinerClassName)
        .Factory(context, dynamicContext);
}

NYTree::TYsonStructPtr TRegistry::ParseExternalStateJoinerParameters(
    const TExternalStateJoinerSpecPtr& spec)
{
    auto parameters = GetExternalStateJoinerDescriptor(spec->ExternalStateJoinerClassName).ParametersFactory();
    if (spec->Parameters) {
        parameters->Load(spec->Parameters);
    }
    return parameters;
}

NYTree::TYsonStructPtr TRegistry::ParseDynamicExternalStateJoinerParameters(
    const TExternalStateJoinerSpecPtr& spec,
    const TDynamicExternalStateJoinerSpecPtr& dynamicSpec)
{
    auto dynamicParameters = GetExternalStateJoinerDescriptor(spec->ExternalStateJoinerClassName).DynamicParametersFactory();
    if (dynamicSpec && dynamicSpec->Parameters) {
        dynamicParameters->Load(dynamicSpec->Parameters);
    }
    return dynamicParameters;
}

namespace {

std::vector<TRichYPath> ReadRichYPaths(const INodePtr& node)
{
    if (node->GetType() == ENodeType::List) {
        return ConvertTo<std::vector<TRichYPath>>(node);
    }
    return {ConvertTo<TRichYPath>(node)};
}

} // namespace

std::vector<TYTPathClaim> TRegistry::CollectYTPathClaims(const NYTree::TYsonStructPtr& parameters) const
{
    auto parametersNode = ConvertToNode(parameters);

    std::vector<TYTPathClaim> claims;
    parameters->GetMeta()->Traverse([&] (const TYsonStructTraverseContext& context) {
        auto ownership = context.Parameter->FindOption<EYTPathOwnership>();
        if (!ownership) {
            return;
        }
        auto fieldNode = FindNodeByYPath(parametersNode, context.Path);
        if (!fieldNode) {
            return;
        }
        for (const auto& richPath : ReadRichYPaths(fieldNode)) {
            auto normalized = richPath.Normalize();
            // An unset path-typed field (e.g. an embedded spec the entity never
            // configures) owns no YT node and must not be claimed.
            if (normalized.GetPath().empty()) {
                continue;
            }
            claims.push_back(TYTPathClaim{
                .Cluster = normalized.GetCluster().value_or(""),
                .Path = normalized.GetPath(),
                .Ownership = *ownership,
                .ParameterKey = TString(context.Key),
            });
        }
    });
    return claims;
}

void TRegistry::RegisterPayloadMigrationFunction(TStringBuf name, TPayloadMigrationFunction func)
{
    EmplaceOrCrash(
        TypeNameToPayloadMigrationFunction_,
        name,
        std::move(func));
}

TPayloadMigrationFunction TRegistry::GetPayloadMigrationFunction(TStringBuf name) const
{
    auto it = TypeNameToPayloadMigrationFunction_.find(name);
    THROW_ERROR_EXCEPTION_IF(it.IsEnd(),
        "No payload migration function %Qv is registered",
        name);
    return it->second;
}

void TRegistry::ValidateComputationSpec(const TComputationSpecPtr& spec) const
{
    {
        const auto& typeName = spec->ComputationClassName;
        if (!TypeNameToComputationDescriptor_.contains(typeName)) {
            THROW_ERROR_EXCEPTION("No computation class %Qv is registered", typeName);
        }
    }
    GetComputationDescriptor(spec->ComputationClassName).ValidateSpec(*spec);
    for (const auto& [sourceId, sourceSpec] : spec->SourceStreams) {
        ValidateSourceSpec(sourceSpec);
    }
    for (const auto& [sinkId, sinkSpec] : spec->Sinks) {
        ValidateSinkSpec(sinkSpec);
    }
    for (const auto& [name, externalStateSpec] : spec->ExternalStateManagers) {
        ValidateExternalStateManagerSpec(externalStateSpec);
        THROW_ERROR_EXCEPTION_IF(spec->ExternalStateJoiners.contains(name),
            "External state name %Qv is bound to both a manager and a joiner; names must be unique across these namespaces",
            name);
    }
    for (const auto& [_, externalStateSpec] : spec->ExternalStateJoiners) {
        ValidateExternalStateJoinerSpec(externalStateSpec);
    }
}

void TRegistry::ValidateSourceSpec(const TSourceSpecPtr& spec) const
{
    const auto& typeName = spec->SourceClassName;
    if (!TypeNameToSourceDescriptor_.contains(typeName)) {
        THROW_ERROR_EXCEPTION("No source class %Qv is registered", typeName);
    }
    GetSourceDescriptor(typeName).ValidateSpec(*spec);
}

void TRegistry::ValidateSinkSpec(const TSinkSpecPtr& spec) const
{
    const auto& typeName = spec->SinkClassName;
    if (!TypeNameToSinkDescriptor_.contains(typeName)) {
        THROW_ERROR_EXCEPTION("No sink class %Qv is registered", typeName);
    }
    GetSinkDescriptor(typeName).ValidateSpec(*spec);
}

void TRegistry::ValidateResourceSpec(const TResourceSpecPtr& spec) const
{
    const auto& typeName = spec->ResourceClassName;
    if (!TypeNameToResourceDescriptor_.contains(typeName)) {
        THROW_ERROR_EXCEPTION("No resource %Qv is registered", typeName);
    }
    GetResourceDescriptor(typeName).ValidateSpec(*spec);
}

void TRegistry::ValidateStreamSpec(const TStreamSpecPtr& spec) const
{
    const auto& typeName = spec->ClassName;
    if (typeName && !TypeNameToYsonMessageDescriptor_.contains(*typeName)) {
        THROW_ERROR_EXCEPTION("No yson message class %Qv is registered", typeName);
    }
}

void TRegistry::ValidateExternalStateManagerSpec(const TExternalStateManagerSpecPtr& spec) const
{
    if (!TypeNameToExternalStateManagerDescriptor_.contains(spec->ExternalStateManagerClassName)) {
        THROW_ERROR_EXCEPTION("No external state manager class %Qv is registered",
            spec->ExternalStateManagerClassName);
    }
    GetExternalStateManagerDescriptor(spec->ExternalStateManagerClassName).ValidateSpec(*spec);
}

bool TRegistry::IsExternalStateJoinerVisitorDriven(const std::string& typeName) const
{
    auto it = TypeNameToExternalStateJoinerDescriptor_.find(typeName);
    return it != TypeNameToExternalStateJoinerDescriptor_.end() && it->second.VisitorDriven;
}

void TRegistry::ValidateExternalStateJoinerSpec(const TExternalStateJoinerSpecPtr& spec) const
{
    if (!TypeNameToExternalStateJoinerDescriptor_.contains(spec->ExternalStateJoinerClassName)) {
        THROW_ERROR_EXCEPTION("No external state joiner class %Qv is registered",
            spec->ExternalStateJoinerClassName);
    }
    GetExternalStateJoinerDescriptor(spec->ExternalStateJoinerClassName).ValidateSpec(*spec);
}

void TRegistry::ValidateYsonMessageType(TStringBuf name, const TYsonMessagePtr& ysonMessage) const
{
    const auto& descriptor = GetYsonMessageDescriptor(name);
    if (ysonMessage->GetMeta() != descriptor.Meta) {
        THROW_ERROR_EXCEPTION("Unexpected class of yson message")
            << TErrorAttribute("expected_class_name", name);
    }
}

std::vector<std::string> TRegistry::GetComputationTypeNames() const
{
    return GetKeys(TypeNameToComputationDescriptor_);
}

std::vector<std::string> TRegistry::GetSourceTypeNames() const
{
    return GetKeys(TypeNameToSourceDescriptor_);
}

std::vector<std::string> TRegistry::GetSinkTypeNames() const
{
    return GetKeys(TypeNameToSinkDescriptor_);
}

std::vector<std::string> TRegistry::GetResourceTypeNames() const
{
    return GetKeys(TypeNameToResourceDescriptor_);
}

TRegistry::TParameterFactories TRegistry::GetComputationParameterFactories(TStringBuf typeName) const
{
    auto descriptor = GetComputationDescriptor(typeName);
    return {descriptor.ParametersFactory,
        descriptor.DynamicParametersFactory};
}

TRegistry::TParameterFactories TRegistry::GetSourceParameterFactories(TStringBuf typeName) const
{
    auto descriptor = GetSourceDescriptor(typeName);
    return {descriptor.ParametersFactory,
        descriptor.DynamicParametersFactory};
}

TRegistry::TParameterFactories TRegistry::GetSinkParameterFactories(TStringBuf typeName) const
{
    auto descriptor = GetSinkDescriptor(typeName);
    return {descriptor.ParametersFactory,
        descriptor.DynamicParametersFactory};
}

std::vector<std::string> TRegistry::GetPayloadMigrationFunctionNames() const
{
    return GetKeys(TypeNameToPayloadMigrationFunction_);
}

std::vector<std::string> TRegistry::GetYsonMessageTypeNames() const
{
    return GetKeys(TypeNameToYsonMessageDescriptor_);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void ValidateParameters(
    TStringBuf specType,
    TYPath path,
    auto factory,
    NYTree::INodePtr parametersNode,
    std::vector<TError>* errors,
    NYTree::IMapNodePtr unrecognizedOutput)
{
    try {
        auto parameters = factory();
        parameters->SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);
        parameters->Load(parametersNode, true, true, [&] {
            return YPathJoin(specType, path);
        });
        auto parametersUnrecognized = parameters->GetRecursiveUnrecognized();
        if (!parametersUnrecognized->GetKeys().empty()) {
            SetNodeByYPath(unrecognizedOutput, path, parametersUnrecognized, /*force*/ true);
        }
    } catch (const std::exception& exception) {
        errors->push_back(TError(exception));
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

namespace {

NYTree::INodePtr ProcessingFunctionParametersNodeOrEmpty(const NYTree::IMapNodePtr& node)
{
    return node ? NYTree::INodePtr(node) : NYTree::GetEphemeralNodeFactory()->CreateMap();
}

} // namespace

const TRegistry::TProcessFunctionDescriptor* TRegistry::FindProcessFunctionDescriptor(TStringBuf typeName) const
{
    auto it = TypeNameToProcessFunctionDescriptor_.find(typeName);
    return it == TypeNameToProcessFunctionDescriptor_.end() ? nullptr : &it->second;
}

IProcessFunctionBasePtr TRegistry::CreateProcessFunction(const std::string& name) const
{
    const auto* descriptor = FindProcessFunctionDescriptor(name);
    THROW_ERROR_EXCEPTION_UNLESS(descriptor, "Unknown processing function %Qv", name);
    return descriptor->Factory();
}

ISyncProcessFunction* TRegistry::ViewProcessFunctionAsSync(const std::string& name, const IProcessFunctionBasePtr& function) const
{
    const auto* descriptor = FindProcessFunctionDescriptor(name);
    THROW_ERROR_EXCEPTION_UNLESS(descriptor, "Unknown processing function %Qv", name);
    return descriptor->SyncView(function.Get());
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TError> TRegistry::ValidatePipelineSpecParseability(const NYTree::IMapNodePtr& specNode) const
{
    std::vector<TError> errors;

    auto pipelineSpec = New<TPipelineSpec>();
    pipelineSpec->SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);
    try {
        pipelineSpec->Load(specNode, true, true, [] {
            return "static_pipeline_spec";
        });
    } catch (const std::exception& exception) {
        errors.push_back(TError(exception));
        return errors;
    }
    auto unrecognized = ConvertTo<IMapNodePtr>(pipelineSpec->GetRecursiveUnrecognized());

    for (const auto& [computationName, computationSpec] : pipelineSpec->Computations) {
        try {
            ValidateComputationSpec(computationSpec);
            ValidateParameters(
                "static_pipeline_spec",
                Format("/computations/%v/parameters", computationName),
                GetComputationDescriptor(computationSpec->ComputationClassName).ParametersFactory,
                computationSpec->Parameters,
                &errors,
                unrecognized);
            for (const auto& [streamId, sourceSpec] : computationSpec->SourceStreams) {
                ValidateParameters(
                    "static_pipeline_spec",
                    Format("/computations/%v/source_streams/%v/parameters", computationName, streamId),
                    GetSourceDescriptor(sourceSpec->SourceClassName).ParametersFactory,
                    sourceSpec->Parameters,
                    &errors,
                    unrecognized);
            }
            for (const auto& [sinkId, sinkSpec] : computationSpec->Sinks) {
                ValidateParameters(
                    "static_pipeline_spec",
                    Format("/computations/%v/sinks/%v/parameters", computationName, sinkId),
                    GetSinkDescriptor(sinkSpec->SinkClassName).ParametersFactory,
                    sinkSpec->Parameters,
                    &errors,
                    unrecognized);
            }
            if (GetComputationDescriptor(computationSpec->ComputationClassName).RequiresProcessingFunction &&
                !computationSpec->ProcessingFunction)
            {
                errors.push_back(TError("Computation %Qv must specify a \"processing_function\"", computationName));
            }
            if (computationSpec->ProcessingFunction) {
                const auto* functionDescriptor = FindProcessFunctionDescriptor(*computationSpec->ProcessingFunction);
                if (!functionDescriptor) {
                    errors.push_back(TError("Unknown processing function %Qv in computation %Qv",
                        *computationSpec->ProcessingFunction,
                        computationName));
                } else {
                    ValidateParameters(
                        "static_pipeline_spec",
                        Format("/computations/%v/processing_function_parameters", computationName),
                        functionDescriptor->StaticParametersFactory,
                        ProcessingFunctionParametersNodeOrEmpty(computationSpec->ProcessingFunctionParameters),
                        &errors,
                        unrecognized);
                    if (functionDescriptor->OverridesSync &&
                        !GetComputationDescriptor(computationSpec->ComputationClassName).InvokesProcessFunctionSync)
                    {
                        errors.push_back(TError(
                            "Processing function %Qv requires a sync phase, but computation %Qv hosts it on %Qv, "
                            "which runs none",
                            *computationSpec->ProcessingFunction,
                            computationName,
                            computationSpec->ComputationClassName));
                    }
                }
            }
        } catch (const std::exception& exception) {
            errors.push_back(TError(exception));
        }
    }

    if (!unrecognized->GetKeys().empty()) {
        errors.push_back(TError("Static spec has unrecognized fields") << TErrorAttribute("unrecognized_fields", unrecognized));
    }

    return errors;
}

namespace {

template <typename TEntityId, typename TContainer>
std::vector<TEntityId> GetClosestNames(TEntityId name, const TContainer& existingNames)
{
    std::vector<std::pair<size_t, TEntityId>> candidates;
    auto wideName = ::UTF8ToWide(name.Underlying());
    for (const auto& candidateName : existingNames) {
        size_t distance = NLevenshtein::Distance(wideName, ::UTF8ToWide(candidateName.Underlying()));
        candidates.emplace_back(distance, candidateName);
    }
    Sort(candidates);

    constexpr size_t MaxCandidatesSize = 5;
    return GetIths<1>(candidates, MaxCandidatesSize);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::vector<TError> TRegistry::ValidateDynamicPipelineSpecParseability(const TPipelineSpecPtr& pipelineSpec, const NYTree::IMapNodePtr& dynamicSpecNode) const
{
    std::vector<TError> errors;

    auto dynamicPipelineSpec = New<TDynamicPipelineSpec>();
    dynamicPipelineSpec->SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);
    try {
        dynamicPipelineSpec->Load(dynamicSpecNode, true, true, [] {
            return "dynamic_pipeline_spec";
        });
    } catch (const std::exception& exception) {
        errors.push_back(TError(exception));
        return errors;
    }
    auto unrecognized = ConvertTo<IMapNodePtr>(dynamicPipelineSpec->GetRecursiveUnrecognized());

    // Validate that all computations in dynamic spec exist in static spec.
    for (const auto& [computationName, dynamicComputationSpec] : dynamicPipelineSpec->Computations) {
        if (!pipelineSpec->Computations.contains(computationName)) {
            errors.push_back(
                TError(
                    "Dynamic spec contains computation %Qv that does not exist in static spec. Closest existing names: %v",
                    computationName,
                    GetClosestNames(computationName, GetKeys(pipelineSpec->Computations))));
        }
    }

    for (const auto& [computationName, computationSpec] : pipelineSpec->Computations) {
        try {
            auto dynamicComputationSpecIt = dynamicPipelineSpec->Computations.find(computationName);
            if (dynamicComputationSpecIt == dynamicPipelineSpec->Computations.end()) {
                continue;
            }
            auto& dynamicComputationSpec = dynamicComputationSpecIt->second;

            ValidateParameters(
                "dynamic_pipeline_spec",
                Format("/computations/%v/parameters", computationName),
                GetComputationDescriptor(computationSpec->ComputationClassName).DynamicParametersFactory,
                dynamicComputationSpec->Parameters,
                &errors,
                unrecognized);

            if (computationSpec->ProcessingFunction) {
                if (const auto* functionDescriptor = FindProcessFunctionDescriptor(*computationSpec->ProcessingFunction)) {
                    ValidateParameters(
                        "dynamic_pipeline_spec",
                        Format("/computations/%v/processing_function_parameters", computationName),
                        functionDescriptor->DynamicParametersFactory,
                        ProcessingFunctionParametersNodeOrEmpty(dynamicComputationSpec->ProcessingFunctionParameters),
                        &errors,
                        unrecognized);
                }
            }

            // Validate that all source_streams in dynamic spec exist in static spec.
            for (const auto& [streamId, dynamicSourceSpec] : dynamicComputationSpec->SourceStreams) {
                if (!computationSpec->SourceStreams.contains(streamId)) {
                    errors.push_back(TError(
                        "Dynamic spec contains source_stream %Qv in computation %Qv "
                        "that does not exist in static spec. Closest existing names: %v",
                        streamId,
                        computationName,
                        GetClosestNames(streamId, GetKeys(computationSpec->SourceStreams))));
                }
            }

            // Validate that all sinks in dynamic spec exist in static spec.
            for (const auto& [sinkId, dynamicSinkSpec] : dynamicComputationSpec->Sinks) {
                if (!computationSpec->Sinks.contains(sinkId)) {
                    errors.push_back(TError(
                        "Dynamic spec contains sink %Qv in computation %Qv "
                        "that does not exist in static spec. Closest existing names: %v",
                        sinkId,
                        computationName,
                        GetClosestNames(sinkId, GetKeys(computationSpec->Sinks))));
                }
            }

            for (const auto& [streamId, sourceSpec] : computationSpec->SourceStreams) {
                auto dynamicSourceSpecIt = dynamicComputationSpec->SourceStreams.find(streamId);
                if (dynamicSourceSpecIt == dynamicComputationSpec->SourceStreams.end()) {
                    continue;
                }
                auto& dynamicSourceSpec = dynamicSourceSpecIt->second;
                ValidateParameters(
                    "dynamic_pipeline_spec",
                    Format("/computations/%v/source_streams/%v/parameters", computationName, streamId),
                    GetSourceDescriptor(sourceSpec->SourceClassName).DynamicParametersFactory,
                    dynamicSourceSpec->Parameters,
                    &errors,
                    unrecognized);
            }
            for (const auto& [sinkId, sinkSpec] : computationSpec->Sinks) {
                auto dynamicSinkSpecIt = dynamicComputationSpec->Sinks.find(sinkId);
                if (dynamicSinkSpecIt == dynamicComputationSpec->Sinks.end()) {
                    continue;
                }
                auto& dynamicSinkSpec = dynamicSinkSpecIt->second;
                ValidateParameters(
                    "dynamic_pipeline_spec",
                    Format("/computations/%v/sinks/%v/parameters", computationName, sinkId),
                    GetSinkDescriptor(sinkSpec->SinkClassName).DynamicParametersFactory,
                    dynamicSinkSpec->Parameters,
                    &errors,
                    unrecognized);
            }
        } catch (const std::exception& exception) {
            errors.push_back(TError(exception));
        }
    }

    // Validate that all resources in dynamic spec exist in static spec.
    for (const auto& [resourceId, dynamicResourceSpec] : dynamicPipelineSpec->Resources) {
        if (!pipelineSpec->Resources.contains(resourceId)) {
            errors.push_back(
                TError(
                    "Dynamic spec contains resource %Qv that does not exist in static spec. Closest existing names: %v",
                    resourceId,
                    GetClosestNames(resourceId, GetKeys(pipelineSpec->Resources))));
        }
    }

    // Validate resource dynamic parameters.
    for (const auto& [resourceId, resourceSpec] : pipelineSpec->Resources) {
        try {
            auto dynamicResourceSpecIt = dynamicPipelineSpec->Resources.find(resourceId);
            if (dynamicResourceSpecIt == dynamicPipelineSpec->Resources.end()) {
                continue;
            }
            auto& dynamicResourceSpec = dynamicResourceSpecIt->second;
            ValidateParameters(
                "dynamic_pipeline_spec",
                Format("/resources/%v/parameters", resourceId),
                GetResourceDescriptor(resourceSpec->ResourceClassName).DynamicParametersFactory,
                dynamicResourceSpec->Parameters,
                &errors,
                unrecognized);
        } catch (const std::exception& exception) {
            errors.push_back(TError(exception));
        }
    }

    if (!unrecognized->GetKeys().empty()) {
        errors.push_back(TError("Dynamic spec has unrecognized fields") << TErrorAttribute("unrecognized_fields", unrecognized));
    }

    return errors;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

const auto& GetDescriptor(TStringBuf mapType, auto& descriptorMap, TStringBuf typeName)
{
    auto it = descriptorMap.find(typeName);
    if (it == descriptorMap.end()) {
        constexpr int MaxCandidatesSize = 5;

        // [{notEndsWithTypeName, distance, candidateTypeName} ...].
        // notEndsWithTypeName is for prioritizing of types with forgotten namespace.
        std::vector<std::tuple<bool, size_t, std::string>> candidates;
        auto wideTypeName = ::UTF8ToWide(typeName);
        for (const auto& candidateTypeName : GetKeys(descriptorMap)) {
            size_t distance = NLevenshtein::Distance(wideTypeName, ::UTF8ToWide(candidateTypeName));
            candidates.emplace_back(!candidateTypeName.ends_with(typeName), distance, candidateTypeName);
        }
        Sort(candidates);

        THROW_ERROR_EXCEPTION(
            "No %Qv class %Qv is registered. "
            "Check if you write YT_FLOW_DEFINE_* in source file and mark this file GLOBAL in ya.make (so file cannot be excluded by linker). "
            "Closest registered names: %v",
            mapType,
            typeName,
            ConvertToYsonString(GetIths<2>(candidates, MaxCandidatesSize), NYson::EYsonFormat::Text));
    }

    return it->second;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

const TRegistry::TComputationDescriptor& TRegistry::GetComputationDescriptor(TStringBuf typeName) const
{
    return GetDescriptor("computation", TypeNameToComputationDescriptor_, typeName);
}

const TRegistry::TSourceDescriptor& TRegistry::GetSourceDescriptor(TStringBuf typeName) const
{
    return GetDescriptor("source", TypeNameToSourceDescriptor_, typeName);
}

const TRegistry::TSinkDescriptor& TRegistry::GetSinkDescriptor(TStringBuf typeName) const
{
    return GetDescriptor("sink", TypeNameToSinkDescriptor_, typeName);
}

const TRegistry::TResourceDescriptor& TRegistry::GetResourceDescriptor(TStringBuf typeName) const
{
    return GetDescriptor("resource", TypeNameToResourceDescriptor_, typeName);
}

const TRegistry::TExternalStateManagerDescriptor& TRegistry::GetExternalStateManagerDescriptor(TStringBuf typeName) const
{
    return GetDescriptor("external_state_manager", TypeNameToExternalStateManagerDescriptor_, typeName);
}

const TRegistry::TExternalStateJoinerDescriptor& TRegistry::GetExternalStateJoinerDescriptor(TStringBuf typeName) const
{
    return GetDescriptor("external_state_joiner", TypeNameToExternalStateJoinerDescriptor_, typeName);
}

const TRegistry::TYsonMessageDescriptor& TRegistry::GetYsonMessageDescriptor(TStringBuf typeName) const
{
    return GetDescriptor("yson_message", TypeNameToYsonMessageDescriptor_, typeName);
}

////////////////////////////////////////////////////////////////////////////////

IDescribeTraitsPtr TRegistry::CreateComputationDescribeTraits(TStringBuf typeName, const TDescribeTraitsContext& context) const
{
    auto it = TypeNameToComputationDescriptor_.find(typeName);
    return it != TypeNameToComputationDescriptor_.end() ? it->second.DescribeTraitsFactory(context) : nullptr;
}

IDescribeTraitsPtr TRegistry::CreateSourceDescribeTraits(TStringBuf typeName, const TDescribeTraitsContext& context) const
{
    auto it = TypeNameToSourceDescriptor_.find(typeName);
    return it != TypeNameToSourceDescriptor_.end() ? it->second.DescribeTraitsFactory(context) : nullptr;
}

IDescribeTraitsPtr TRegistry::CreateSinkDescribeTraits(TStringBuf typeName, const TDescribeTraitsContext& context) const
{
    auto it = TypeNameToSinkDescriptor_.find(typeName);
    return it != TypeNameToSinkDescriptor_.end() ? it->second.DescribeTraitsFactory(context) : nullptr;
}

IDescribeTraitsPtr TRegistry::CreateExternalStateManagerDescribeTraits(TStringBuf typeName, const TDescribeTraitsContext& context) const
{
    auto it = TypeNameToExternalStateManagerDescriptor_.find(typeName);
    return it != TypeNameToExternalStateManagerDescriptor_.end() ? it->second.DescribeTraitsFactory(context) : nullptr;
}

IDescribeTraitsPtr TRegistry::CreateExternalStateJoinerDescribeTraits(TStringBuf typeName, const TDescribeTraitsContext& context) const
{
    auto it = TypeNameToExternalStateJoinerDescriptor_.find(typeName);
    return it != TypeNameToExternalStateJoinerDescriptor_.end() ? it->second.DescribeTraitsFactory(context) : nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
