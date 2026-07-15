#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/flow/lib/serializer/serializer.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/public.h>

#include <optional>
#include <typeindex>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Read-only, per-call context handed to every process-function hook. Exposes watermark,
//! spec and stream-metadata accessors and message/timer construction helpers.
struct IRuntimeContext
    : public TRefCounted
{
    //! Event-time watermark of |streamId|.
    virtual TSystemTimestamp GetWatermark(const TStreamId& streamId) const = 0;
    //! Minimal event-time watermark across all input streams.
    virtual TSystemTimestamp GetInputEventWatermark() const = 0;
    //! Watermark state of the epoch being processed.
    virtual TWatermarkStatePtr GetEpochWatermarkState() const = 0;
    //! YT timestamp the controller acquired for the current epoch; trails real time by controller/heartbeat lag.
    virtual TSystemTimestamp GetCurrentTimestamp() const = 0;

    virtual const TComputationSpecPtr& GetSpec() const = 0;
    virtual const TComputationStreamSpecStoragePtr& GetStreamSpecs() const = 0;
    //! Schema of the message key (equal to GetSpec()->GroupBySchema).
    virtual const NTableClient::TTableSchemaPtr& GetKeySchema() const = 0;

    //! Builds a message builder targeting an output stream. If |streamId| is omitted,
    //! uses the single output stream (throws if there is not exactly one).
    virtual TMessageBuilder MakeOutputMessageBuilder(std::optional<TStreamId> streamId = {}) const = 0;
    //! Converts a message to the output stream schema and sets its StreamId.
    virtual TMessage ConvertToOutputMessage(const TMessage& message, std::optional<TStreamId> streamId = {}) const = 0;
    //! Converts a typed YSON message into a wire message, guessing/validating the stream.
    virtual TMessage ConvertToMessage(const TYsonMessagePtr& ysonMessage) const = 0;
    virtual TTimer MakeTimer(
        const TKey& key,
        const TStreamId& streamId,
        TSystemTimestamp triggerTimestamp,
        TSystemTimestamp eventTimestamp) const = 0;

    //! Returns the distributed throttler client for |throttlerId|. Throws if it is not in
    //! the dynamic pipeline spec's ``throttlers`` (a configuration error).
    virtual NConcurrency::IThroughputThrottlerPtr GetThrottler(const TThrottlerId& throttlerId) = 0;

    //! Raw ``function_parameters`` map from the dynamic computation spec (never null; an empty
    //! map when the block is absent), reflecting the latest reconfiguration. Prefer the typed
    //! GetDynamicParameters<T>() helper.
    virtual NYTree::IMapNodePtr GetDynamicParametersNode() const = 0;

    //! Deserializes the dynamic ``function_parameters`` block into the YSON struct |T| (defaults
    //! applied if absent). Memoized: reparsed only when the parameters node or |T| changes.
    template <class T>
    TIntrusivePtr<T> GetDynamicParameters() const;

    //! Typed deserialization of an input message into a YSON struct. Context-free
    //! convenience over the free ::NYT::NFlow::ConvertToYsonMessage<T> helper.
    template <class T>
    TIntrusivePtr<T> ConvertToYsonMessage(const TInputMessageConstPtr& message) const;

    //! Typed deserialization of a message/timer key into a YSON struct, using GetKeySchema().
    template <class T>
    TIntrusivePtr<T> ConvertToYsonKey(const TKey& key) const;

private:
    //! Single-slot memo for the typed parameters parse: last node and type, type-erased result.
    struct TDynamicParametersCache
    {
        NYTree::IMapNodePtr Node;
        std::type_index Type = std::type_index(typeid(void));
        TIntrusivePtr<TRefCounted> Value;
    };

    mutable TDynamicParametersCache DynamicParametersCache_;
};

DEFINE_REFCOUNTED_TYPE(IRuntimeContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define RUNTIME_CONTEXT_INL_H_
#include "runtime_context-inl.h"
#undef RUNTIME_CONTEXT_INL_H_
