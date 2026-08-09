#include "companion_model.h"

#include <yt/yt/flow/library/cpp/companion/proto/companion_service.pb.h>

#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <util/generic/map.h>

namespace NYT::NFlow::NCompanion {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TStreamSpecsPtr CreateLocalStreamSpecs(
    const THashMap<TStreamId, NTableClient::TTableSchemaPtr>& sourceStreamsSchemas,
    const THashSet<TStreamId>& outputStreamIds,
    const TStreamSpecsPtr& streamSpecs)
{
    auto streamSpecsMap = THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>>();
    // StreamSpecId for the current batch.
    i64 localStreamSpecId = 0;

    // Process source streams.
    for (const auto& [streamId, payloadSchema] : sourceStreamsSchemas) {
        auto streamSpec = New<TStreamSpec>();
        streamSpec->Schema = payloadSchema;
        streamSpecsMap[streamId].emplace(TStreamSpecId(localStreamSpecId++), std::move(streamSpec));
    }

    // Process output streams.
    for (const auto& streamId : outputStreamIds) {
        auto currentStreamSpecId = streamSpecs->GetLastSpecId(streamId);
        auto streamSpec = streamSpecs->GetSpec(currentStreamSpecId);
        streamSpecsMap[streamId].emplace(TStreamSpecId(localStreamSpecId++), std::move(streamSpec));
    }

    return New<TStreamSpecs>(std::move(streamSpecsMap));
}

////////////////////////////////////////////////////////////////////////////////

void TCompanionState::Register(TRegistrar registrar)
{
    registrar.Parameter("payload", &TThis::Payload)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TCompanionResourceInstanceReference::Register(TRegistrar registrar)
{
    registrar.Parameter("resource_id", &TThis::ResourceId);
    registrar.Parameter("incarnation_id", &TThis::IncarnationId);
    registrar.Parameter("configuration_generation", &TThis::ConfigurationGeneration);
    registrar.Parameter("alias", &TThis::Alias)
        .Default();
    registrar.UnrecognizedStrategy(NYTree::EUnrecognizedStrategy::Throw);
}

void ToProto(
    NProto::NCompanion::TCompanionResourceInstanceReference* protoReference,
    const TCompanionResourceInstanceReference& reference)
{
    protoReference->set_resource_id(ToProto<TProtobufString>(reference.ResourceId));
    ToProto(protoReference->mutable_incarnation_id(), reference.IncarnationId);
    protoReference->set_configuration_generation(reference.ConfigurationGeneration);
    if (reference.Alias) {
        protoReference->set_alias(ToProto<TProtobufString>(*reference.Alias));
    }
}

void FromProto(
    TCompanionResourceInstanceReference* reference,
    const NProto::NCompanion::TCompanionResourceInstanceReference& protoReference)
{
    reference->ResourceId = TResourceId(protoReference.resource_id());
    FromProto(&reference->IncarnationId, protoReference.incarnation_id());
    reference->ConfigurationGeneration = protoReference.configuration_generation();
    reference->Alias = protoReference.has_alias()
        ? std::make_optional(TResourceId(protoReference.alias()))
        : std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

TStreamSpecsPtr TCompanionProcessRequest::GetMessageStreamSpecs() const
{
    return OverrideStreamSpecs ? OverrideStreamSpecs : JobStreamSpecs;
}

////////////////////////////////////////////////////////////////////////////////

void TInitResourceCommandArg::Register(TRegistrar registrar)
{
    registrar.Parameter("spec", &TThis::Spec);
    registrar.Parameter("dynamic_spec", &TThis::DynamicSpec);
    registrar.Parameter("incarnation_id", &TThis::IncarnationId);
    registrar.Parameter("incarnation_generation", &TThis::IncarnationGeneration)
        .Default();
    registrar.Parameter("configuration_generation", &TThis::ConfigurationGeneration)
        .Default();
    registrar.Parameter("dependencies", &TThis::Dependencies)
        .Default();
    registrar.Parameter("resource_revision", &TThis::ResourceRevision)
        .Default();
    registrar.UnrecognizedStrategy(NYTree::EUnrecognizedStrategy::Throw);
}

void TUnloadResourceCommandArg::Register(TRegistrar registrar)
{
    registrar.Parameter("incarnation_id", &TThis::IncarnationId);
    registrar.UnrecognizedStrategy(NYTree::EUnrecognizedStrategy::Throw);
}

} // namespace NYT::NFlow::NCompanion
