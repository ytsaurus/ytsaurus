#include "computation_runtime_context.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/schema.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/distributed_throttler/factory.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TComputationRuntimeContext::TComputationRuntimeContext(
    TComputationSpecPtr spec,
    TComputationStreamSpecStoragePtr streamSpecs,
    TTableSchemaPtr keySchema,
    IPayloadConverterCachePtr converterCache,
    NDistributedThrottler::IDistributedThrottlerFactoryPtr throttlerFactory)
    : Spec_(std::move(spec))
    , StreamSpecs_(std::move(streamSpecs))
    , KeySchema_(std::move(keySchema))
    , ConverterCache_(std::move(converterCache))
    , ThrottlerFactory_(std::move(throttlerFactory))
    , EmptyDynamicParametersNode_(GetEphemeralNodeFactory()->CreateMap())
{ }

void TComputationRuntimeContext::RefreshEpochState(TWatermarkStatePtr watermarkState, NYTree::IMapNodePtr dynamicParametersNode)
{
    WatermarkState_ = std::move(watermarkState);
    if (dynamicParametersNode) {
        DynamicParametersNode_ = std::move(dynamicParametersNode);
    } else {
        DynamicParametersNode_ = EmptyDynamicParametersNode_;
    }
}

TSystemTimestamp TComputationRuntimeContext::GetWatermark(const TStreamId& streamId) const
{
    return WatermarkState_->GetEventWatermark(streamId);
}

TSystemTimestamp TComputationRuntimeContext::GetInputEventWatermark() const
{
    auto watermark = InfinitySystemTimestamp;
    for (const auto& streamId : Spec_->InputStreamIds) {
        watermark = std::min(watermark, WatermarkState_->GetEventWatermark(streamId));
    }
    return watermark;
}

TWatermarkStatePtr TComputationRuntimeContext::GetEpochWatermarkState() const
{
    return WatermarkState_;
}

TSystemTimestamp TComputationRuntimeContext::GetCurrentTimestamp() const
{
    return GetEpochWatermarkState()->GetCurrentTimestamp();
}

const TComputationSpecPtr& TComputationRuntimeContext::GetSpec() const
{
    return Spec_;
}

const TComputationStreamSpecStoragePtr& TComputationRuntimeContext::GetStreamSpecs() const
{
    return StreamSpecs_;
}

const TTableSchemaPtr& TComputationRuntimeContext::GetKeySchema() const
{
    return KeySchema_;
}

TMessageBuilder TComputationRuntimeContext::MakeOutputMessageBuilder(std::optional<TStreamId> streamId) const
{
    auto resolved = GuessStreamId(std::move(streamId));
    return TMessageBuilder(resolved, StreamSpecs_->GetSchema(resolved));
}

TMessage TComputationRuntimeContext::ConvertToOutputMessage(const TMessage& message, std::optional<TStreamId> streamId) const
{
    auto resolved = GuessStreamId(std::move(streamId));
    auto outputMessage = ConvertMessageToNewSchema(message, StreamSpecs_->GetSpec(resolved)->Schema, ConverterCache_);
    outputMessage.StreamId = resolved;
    return outputMessage;
}

TMessage TComputationRuntimeContext::ConvertToMessage(const TYsonMessagePtr& ysonMessage) const
{
    auto streamId = ysonMessage->Meta->StreamId;
    if (streamId.Underlying().empty()) {
        streamId = GuessStreamId(std::nullopt);
    }
    auto spec = StreamSpecs_->GetSpec(streamId);
    if (!spec->ClassName) {
        THROW_ERROR_EXCEPTION("Impossible to convert yson message to message due to undefined \"class_name\"")
            << TErrorAttribute("stream_id", streamId);
    }
    TRegistry::Get()->ValidateYsonMessageType(*spec->ClassName, ysonMessage);
    auto message = ::NYT::NFlow::ConvertToMessage(ysonMessage, spec->Schema);
    message.StreamId = streamId;
    return message;
}

TTimer TComputationRuntimeContext::MakeTimer(
    const TKey& key,
    const TStreamId& streamId,
    TSystemTimestamp triggerTimestamp,
    TSystemTimestamp eventTimestamp) const
{
    TTimer timer;
    timer.Key = key;
    timer.KeySchema = KeySchema_;
    timer.StreamId = streamId;
    timer.TriggerTimestamp = triggerTimestamp;
    timer.EventTimestamp = eventTimestamp;
    return timer;
}

IThroughputThrottlerPtr TComputationRuntimeContext::GetThrottler(const TThrottlerId& throttlerId)
{
    YT_VERIFY(ThrottlerFactory_);
    return ThrottlerFactory_->GetClient(throttlerId.Underlying());
}

IMapNodePtr TComputationRuntimeContext::GetDynamicParametersNode() const
{
    return DynamicParametersNode_ ? DynamicParametersNode_ : EmptyDynamicParametersNode_;
}

TStreamId TComputationRuntimeContext::GuessStreamId(std::optional<TStreamId> streamId) const
{
    if (streamId) {
        return *streamId;
    }
    if (Spec_->OutputStreamIds.size() == 1) {
        return *Spec_->OutputStreamIds.begin();
    }
    THROW_ERROR_EXCEPTION("Impossible to guess output stream");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
