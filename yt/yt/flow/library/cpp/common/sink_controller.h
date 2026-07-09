#pragma once

#include "public.h"

#include "computation_controller.h"
#include "sink.h"
#include "state.h"

#include <yt/yt/flow/library/cpp/misc/reconfigurable.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TSinkControllerContextBase
    : public TComputationControllerContextBase
{
    TSinkId SinkId;
    TSinkSpecPtr SinkSpec;
};

struct TSinkControllerContext
    : public TRefCounted
    , public TSinkControllerContextBase
{
};

DEFINE_REFCOUNTED_TYPE(TSinkControllerContext);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicSinkControllerContext
    : public TRefCounted
{
    TDynamicSinkSpecPtr DynamicSinkSpec;
};

DEFINE_REFCOUNTED_TYPE(TDynamicSinkControllerContext);

////////////////////////////////////////////////////////////////////////////////

struct ISinkController
    : public TRefCounted
    , public virtual TReconfigurable<TDynamicSinkControllerContext>
{
    // Provide TParameter[Ptr] and TDynamicParameter[Ptr] aliases. They are types of specs `Parameters` fields.
    // These types are used in sink registration for future parsing.
    // They may be shadowed by macroses YT_FLOW_EXTEND_[DYNAMIC_]PARAMETERS in derived types.
    YT_FLOW_REGISTER_PARAMETERS(ISink::TParameters);
    YT_FLOW_REGISTER_DYNAMIC_PARAMETERS(ISink::TDynamicParameters);

    virtual void Init(IInitContextPtr initContext) = 0;
    virtual void Sync() = 0;
    virtual void Commit() = 0;

    virtual void UpdateWatermarkState(TWatermarkStatePtr watermarkState) = 0;

    virtual std::optional<i64> GetReceiverChannelCount() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISinkController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
