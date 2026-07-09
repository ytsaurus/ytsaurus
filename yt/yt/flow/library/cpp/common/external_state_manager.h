#pragma once

#include "describe_traits.h"
#include "key.h"
#include "public.h"
#include "spec_validation.h"
#include "state.h"
#include "state_provider.h"

#include <yt/yt/flow/library/cpp/misc/public.h>
#include <yt/yt/flow/library/cpp/misc/reconfigurable.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/cache/public.h>
#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/actions/public.h>
#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <typeinfo>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TExternalStateManagerContext
    : public TRefCounted
{
    TExternalStateManagerSpecPtr ExternalStateManagerSpec;
    TJobNamedStateCachePtr StateCache;

    NTableClient::TTableSchemaPtr KeySchema;

    NClient::NCache::IClientsCachePtr ClientsCache;
    NYPath::TRichYPath PipelinePath;
    IInvokerPtr SerializedInvoker;
    IStatusProfilerPtr StatusProfiler;

    NLogging::TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TExternalStateManagerContext);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicExternalStateManagerContext
    : public TRefCounted
{
    TDynamicExternalStateManagerSpecPtr DynamicExternalStateManagerSpec;
};

DEFINE_REFCOUNTED_TYPE(TDynamicExternalStateManagerContext);

////////////////////////////////////////////////////////////////////////////////

struct IExternalStateManager
    : public IMutableStateKeyProvider
    , public virtual TReconfigurable<TDynamicExternalStateManagerContext>
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
    YT_FLOW_REGISTER_PARAMETERS(TParametersBase);
    YT_FLOW_REGISTER_DYNAMIC_PARAMETERS(TDynamicParametersBase);

    // Default describe traits; a concrete state manager may shadow it with its own `using TDescribeTraits = ...;`.
    using TDescribeTraits = TDescribeTraitsBase;

    using TValidator = TNoopSpecValidator;

    // GetState/PreloadKeyStates/GetKeySchema come from IMutableStateKeyProvider.

    virtual void Sync(IRetryableTransactionPtr transaction) = 0;

    virtual void ValidateStateClass(const std::type_info& expectedStateType) const = 0;

    struct TFilter
    {
        std::optional<TKey> LowerKey;
        std::optional<TKey> UpperKey;
    };

    struct TListResult
    {
        std::vector<TKey> Keys;
        std::optional<TKey> OffsetExclusive;
    };

    virtual TFuture<TListResult> List(TFilter filter, i64 limit, std::optional<TKey> offsetExclusive = std::nullopt);
};

DEFINE_REFCOUNTED_TYPE(IExternalStateManager);

////////////////////////////////////////////////////////////////////////////////

template <class TM>
concept CExternalStateManager = requires(TExternalStateManagerContextPtr context, TDynamicExternalStateManagerContextPtr dynamicContext) {
    typename TM::TStateHolderPtr;
    typename TM::TParametersPtr;
    typename TM::TDynamicParametersPtr;
    requires std::derived_from<TM, IExternalStateManager>;
    {
        New<TM>(context, dynamicContext)
    } -> std::same_as<TIntrusivePtr<TM>>;
};

////////////////////////////////////////////////////////////////////////////////

struct TExternalStateJoinerContext
    : public TRefCounted
{
    TExternalStateJoinerSpecPtr ExternalStateJoinerSpec;
    TJobNamedStateCachePtr StateCache;

    NTableClient::TTableSchemaPtr KeySchema;
    IPayloadConverterCachePtr ConverterCache;

    NClient::NCache::IClientsCachePtr ClientsCache;
    THashMap<TResourceId, IResourcePtr> StaticResources;
    NYPath::TRichYPath PipelinePath;
    IInvokerPtr SerializedInvoker;
    IStatusProfilerPtr StatusProfiler;
    NProfiling::TProfiler Profiler;

    NLogging::TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TExternalStateJoinerContext);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicExternalStateJoinerContext
    : public TRefCounted
{
    TDynamicExternalStateJoinerSpecPtr DynamicExternalStateJoinerSpec;
};

DEFINE_REFCOUNTED_TYPE(TDynamicExternalStateJoinerContext);

////////////////////////////////////////////////////////////////////////////////

struct IExternalStateJoiner
    : public IJoinedStateKeyProvider
    , public virtual TReconfigurable<TDynamicExternalStateJoinerContext>
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
    YT_FLOW_REGISTER_PARAMETERS(TParametersBase);
    YT_FLOW_REGISTER_DYNAMIC_PARAMETERS(TDynamicParametersBase);

    // Default describe traits; a concrete state manager may shadow it with its own `using TDescribeTraits = ...;`.
    using TDescribeTraits = TDescribeTraitsBase;

    using TValidator = TNoopSpecValidator;

    //! Whether the joiner class derives its preload keys from the key-visitor's delivered visit
    //! batch rather than from incoming messages. A visitor-driven class shadows this with `true`;
    //! the registry captures it at registration, so it is available at spec-validation time.
    static constexpr bool VisitorDriven = false;

    // GetState/PreloadKeyStates/GetKeySchema/GetConverterCache/GetKeyProviderStreams/
    // HasKeySchemaOverride come from IJoinedStateKeyProvider.

    virtual void Reset() = 0;

    virtual void ValidateStateClass(const std::type_info& expectedStateType) const = 0;

    //! Runtime accessor for #VisitorDriven; visitor-driven joiners are skipped by message-driven
    //! AutoPreload.
    virtual bool IsVisitorDriven() const;

    struct TFilter
    {
        std::optional<TKey> LowerKey;
        std::optional<TKey> UpperKey;
    };

    struct TListResult
    {
        std::vector<TKey> Keys;
        std::optional<TKey> OffsetExclusive;
    };

    virtual TFuture<TListResult> List(TFilter filter, i64 limit, std::optional<TKey> offsetExclusive = std::nullopt);
};

DEFINE_REFCOUNTED_TYPE(IExternalStateJoiner);

////////////////////////////////////////////////////////////////////////////////

template <class TJ>
concept CExternalStateJoiner = requires(TExternalStateJoinerContextPtr context, TDynamicExternalStateJoinerContextPtr dynamicContext) {
    typename TJ::TStateHolderPtr;
    typename TJ::TParametersPtr;
    typename TJ::TDynamicParametersPtr;
    requires std::derived_from<TJ, IExternalStateJoiner>;
    {
        New<TJ>(context, dynamicContext)
    } -> std::same_as<TIntrusivePtr<TJ>>;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
