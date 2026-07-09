#pragma once

#include <yt/yt/flow/library/cpp/common/runtime_context.h>

#include <yt/yt/flow/library/cpp/common/payload_converter.h>

#include <yt/yt/flow/library/cpp/distributed_throttler/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TComputationRuntimeContext)

//! Production IRuntimeContext, owning its dependencies (spec, stream specs, key schema,
//! converter cache, throttler factory).
class TComputationRuntimeContext
    : public IRuntimeContext
{
public:
    TComputationRuntimeContext(
        TComputationSpecPtr spec,
        TComputationStreamSpecStoragePtr streamSpecs,
        NTableClient::TTableSchemaPtr keySchema,
        IPayloadConverterCachePtr converterCache,
        NDistributedThrottler::IDistributedThrottlerFactoryPtr throttlerFactory);

    //! Installs the state for the upcoming dispatch: |watermarkState| is the current watermark
    //! snapshot, |dynamicParametersNode| the latest dynamic `function_parameters` node (empty
    //! if absent).
    void RefreshEpochState(TWatermarkStatePtr watermarkState, NYTree::IMapNodePtr dynamicParametersNode);

    TSystemTimestamp GetWatermark(const TStreamId& streamId) const override;
    TSystemTimestamp GetInputEventWatermark() const override;

    const TComputationSpecPtr& GetSpec() const override;
    const TComputationStreamSpecStoragePtr& GetStreamSpecs() const override;
    const NTableClient::TTableSchemaPtr& GetKeySchema() const override;

    TMessageBuilder MakeOutputMessageBuilder(std::optional<TStreamId> streamId) const override;
    TMessage ConvertToOutputMessage(const TMessage& message, std::optional<TStreamId> streamId) const override;
    TMessage ConvertToMessage(const TYsonMessagePtr& ysonMessage) const override;
    TTimer MakeTimer(
        const TKey& key,
        const TStreamId& streamId,
        TSystemTimestamp triggerTimestamp,
        TSystemTimestamp eventTimestamp) const override;

    NConcurrency::IThroughputThrottlerPtr GetThrottler(const TThrottlerId& throttlerId) override;

    NYTree::IMapNodePtr GetDynamicParametersNode() const override;

private:
    const TComputationSpecPtr Spec_;
    const TComputationStreamSpecStoragePtr StreamSpecs_;
    const NTableClient::TTableSchemaPtr KeySchema_;
    const IPayloadConverterCachePtr ConverterCache_;
    const NDistributedThrottler::IDistributedThrottlerFactoryPtr ThrottlerFactory_;
    //! Stable empty map returned when a dispatch carries no dynamic parameters; the fixed pointer
    //! keeps the parameters node memoizable.
    const NYTree::IMapNodePtr EmptyDynamicParametersNode_;
    TWatermarkStatePtr WatermarkState_;
    NYTree::IMapNodePtr DynamicParametersNode_;

    TStreamId GuessStreamId(std::optional<TStreamId> streamId) const;
};

DEFINE_REFCOUNTED_TYPE(TComputationRuntimeContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
