#pragma once

#include "public.h"

#include "computation.h"
#include "describe_traits.h"
#include "distributing_tracker.h"
#include "message.h"
#include "spec_validation.h"

#include <yt/yt/flow/library/cpp/misc/public.h>
#include <yt/yt/flow/library/cpp/misc/reconfigurable.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TSinkContextBase
    : public TComputationContextBase
{
    TSinkSpecPtr SinkSpec;
};

struct TSinkContext
    : public TRefCounted
    , public TSinkContextBase
{
};

DEFINE_REFCOUNTED_TYPE(TSinkContext);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicSinkContext
    : public TRefCounted
{
    TDynamicSinkSpecPtr DynamicSinkSpec;
};

DEFINE_REFCOUNTED_TYPE(TDynamicSinkContext);

////////////////////////////////////////////////////////////////////////////////

struct ISink
    : public virtual TRefCounted
    , public virtual TReconfigurable<TDynamicSinkContext>
{
private:
    struct TParametersBase
        : public virtual NYTree::TYsonStruct
    {
        REGISTER_YSON_STRUCT(TParametersBase);

        static void Register(TRegistrar registrar);
    };

    struct TDynamicParametersBase
        : public virtual NYTree::TYsonStruct
    {
        REGISTER_YSON_STRUCT(TDynamicParametersBase);

        static void Register(TRegistrar registrar);
    };

public:
    // Provide TParameter[Ptr] and TDynamicParameter[Ptr] aliases. They are types of specs `Parameters` fields.
    // These types are used in sink registration for future parsing.
    // They may be shadowed by macroses YT_FLOW_EXTEND_[DYNAMIC_]PARAMETERS in derived types.
    YT_FLOW_REGISTER_PARAMETERS(TParametersBase);
    YT_FLOW_REGISTER_DYNAMIC_PARAMETERS(TDynamicParametersBase);

    // Default describe traits; connectors may shadow it with their own `using TDescribeTraits = ...;`.
    using TDescribeTraits = TDescribeTraitsBase;

    using TValidator = TNoopSpecValidator;

    virtual void Init(IInitContextPtr initContext) = 0;

    // Distribute the message asynchronously.
    // |onDistributed| is called when the message has been successfully delivered.
    virtual void Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed) = 0;

    // Sync should enrich |transaction| with all sync distributing messages
    // Repeat calls should do the same until Commit call.
    virtual void Sync(NApi::IDynamicTableTransactionPtr transaction) = 0;

    // Async distribution should start only after Commit call.
    virtual void Commit() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISink);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
