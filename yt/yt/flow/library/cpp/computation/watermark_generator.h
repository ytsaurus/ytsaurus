#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TWatermarkGeneratorState
    : public NYTree::TYsonStruct
{
    TSystemTimestamp Max;
    std::optional<TSystemTimestamp> MinAhead;
    std::optional<TSystemTimestamp> MaxAhead;

    REGISTER_YSON_STRUCT(TWatermarkGeneratorState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWatermarkGeneratorState);

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TWatermarkGeneratorCookie, std::any);

////////////////////////////////////////////////////////////////////////////////

//! Class tracks messages from source computation and estimates watermarks of source partitions.
struct IWatermarkGenerator
    : public TRefCounted
{
    virtual TWatermarkGeneratorCookie RegisterRead(const std::vector<TMessage>& messages) = 0;
    virtual void MarkPersisted(TWatermarkGeneratorCookie cookie) = 0;

    virtual THashMap<TStreamId, TInflightStreamTraverseDataPtr> Apply(THashMap<TStreamId, TInflightStreamTraverseDataPtr>&& inflights, const THashSet<TStreamId>& streamIds) = 0;

    virtual TSystemTimestamp GetPartitionPersistedWatermark(std::optional<TSystemTimestamp> sourceReadWatermark) const = 0;
    virtual TSystemTimestamp GetPartitionReadWatermark(std::optional<TSystemTimestamp> sourcePersistedWatermark) const = 0;

    virtual void Init(IInitContextPtr initContext) = 0;
};

DEFINE_REFCOUNTED_TYPE(IWatermarkGenerator);

////////////////////////////////////////////////////////////////////////////////

IWatermarkGeneratorPtr CreateWatermarkGenerator(
    TWatermarkGeneratorSpecPtr spec,
    NProfiling::TProfiler profiler,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
