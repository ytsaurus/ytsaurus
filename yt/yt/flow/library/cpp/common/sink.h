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
    TSinkId SinkId;
    TSinkSpecPtr SinkSpec;
    TUniqueSeqNo JobGeneration;
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

    //! Must complete quickly: callers may invoke it before the partition's first status update, so
    //! blocking here can delay graph progress.
    virtual void Init(IInitContextPtr initContext) = 0;

    // Distribute the message asynchronously.
    // |onDistributed| allows the core to release its output copy. Any unfinished
    // external finalization must remain visible through #BuildInflight().
    virtual void Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed) = 0;

    // Completes external writes required before Sync and invokes their |onDistributed| callbacks.
    // May wait for acknowledgements; completion need not make the messages externally visible.
    // Called before any epoch Sync. Does nothing by default.
    virtual void PreSyncFlush();

    // Enriches |transaction| with synchronously distributed messages and prepares sink state for persistence.
    virtual void Sync(NApi::IDynamicTableTransactionPtr transaction) = 0;

    // Finalizes the successful Sync. Regular async sinks start distribution here;
    // sinks with prepared external distribution finalize it here.
    virtual void Commit() = 0;

    // Reports externally staged messages that have already left the core output
    // store but have not reached their terminal sink state yet.
    virtual THashMap<TStreamId, TInflightStreamTraverseDataPtr> BuildInflight() const;

    // Delivers the epoch watermark snapshot; called on creation and before each run iteration.
    virtual void UpdateWatermarkState(TWatermarkStatePtr state);
};

DEFINE_REFCOUNTED_TYPE(ISink);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
