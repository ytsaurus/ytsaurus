#pragma once

#include "computation.h"
#include "describe_traits.h"
#include "public.h"
#include "spec_validation.h"

#include <yt/yt/flow/library/cpp/common/message_batcher.h>

#include <yt/yt/flow/library/cpp/misc/reconfigurable.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

#include <any>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TSourceContextBase
    : public TComputationContextBase
{
    TStreamId SourceStreamId;
    TKey SourceKey;
    TSourceSpecPtr SourceSpec;
    IUniqueSeqNoProviderPtr UniqueSeqNoProvider;
};

struct TSourceContext
    : public TRefCounted
    , public TSourceContextBase
{
};

DEFINE_REFCOUNTED_TYPE(TSourceContext);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicSourceContext
    : public TRefCounted
{
    TDynamicSourceSpecPtr DynamicSourceSpec;
    NYTree::IMapNodePtr DynamicPartitionSpec;
};

DEFINE_REFCOUNTED_TYPE(TDynamicSourceContext);

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TSourceMessageBatchCookie, std::any);

////////////////////////////////////////////////////////////////////////////////

struct ISource
    : public TRefCounted
    , public virtual TReconfigurable<TDynamicSourceContext>
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

    struct TDynamicPartitionSpecBase
        : public virtual NYTree::TYsonStruct
    {
        REGISTER_YSON_STRUCT(TDynamicPartitionSpecBase);

        static void Register(TRegistrar registrar);
    };

public:
    // Provide TParameter[Ptr] and TDynamicParameter[Ptr] aliases. They are types of specs `Parameters` fields.
    // These types are used in source registration for future parsing.
    // They may be shadowed by macroses YT_FLOW_EXTEND_[DYNAMIC_]PARAMETERS in derived types.
    YT_FLOW_REGISTER_PARAMETERS(TParametersBase);
    YT_FLOW_REGISTER_DYNAMIC_PARAMETERS(TDynamicParametersBase);

    // Provide TDynamicPartitionSpec[Ptr] alias.
    // They may be shadowed by macroses YT_FLOW_EXTEND_DYNAMIC_PARTITION_SPEC in derived types.
    YT_FLOW_REGISTER_DYNAMIC_PARTITION_SPEC(TDynamicPartitionSpecBase);

    // Default describe traits; connectors may shadow it with their own `using TDescribeTraits = ...;`.
    using TDescribeTraits = TDescribeTraitsBase;

    using TValidator = TNoopSpecValidator;

    struct TMessageBatch
    {
        // Tiny object that can be used to marking parsed message as published/processed.
        TSourceMessageBatchCookie Cookie;
        // Original message split result.
        std::vector<TInputMessageConstPtr> Messages;
    };

    virtual void Init(IInitContextPtr initContext) = 0;

    //! Must be called at finish if Init() was called.
    virtual void Terminate() = 0;

    virtual void Sync() = 0;
    // Source should never advance offsets in remote system until Commit call.
    // Next call after Sync always would be Commit (or Terminate and destructor).
    virtual void Commit() = 0;

    // No other methods can be called until returned future is set.
    virtual TFuture<std::vector<TMessageBatch>> GetNextBatch(const TMessageBatcherSettingsPtr& batcherSettings) = 0;
    // Call after message became public, affects draining mode.
    virtual void MarkPublished(const TSourceMessageBatchCookie& cookie) = 0;
    // Call after message became persisted.
    virtual void MarkPersisted(const TSourceMessageBatchCookie& cookie) = 0;

    // TODO: Unite them?
    virtual TInflightStreamTraverseDataPtr BuildInflight() = 0;
    virtual NYTree::IMapNodePtr GetPartitionStatus() = 0;

    virtual std::optional<TSystemTimestamp> GetPersistedEventWatermark() = 0;
    virtual std::optional<TSystemTimestamp> GetReadEventWatermark() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
