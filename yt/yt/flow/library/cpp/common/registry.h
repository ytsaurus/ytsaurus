#pragma once

#include "describe_traits.h"
#include "external_state_manager.h"
#include "public.h"
#include "yt_path_option.h"

#include <library/cpp/yt/misc/preprocessor.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <util/generic/hash.h>
#include <util/generic/singleton.h>

#include <type_traits>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Default parameters type for a process function that declares none: an empty YSON struct, so
//! any field supplied in `processing_function_parameters` is reported as unrecognized.
struct TEmptyProcessFunctionParameters
    : public NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TEmptyProcessFunctionParameters);

    static void Register(TRegistrar /*registrar*/)
    { }
};

//! Detects the optional `static constexpr bool RequiresProcessingFunction` marker a computation
//! sets when it hosts a process function (defaults to false).
template <class T, class = void>
struct TComputationRequiresProcessingFunction
    : public std::false_type
{ };

template <class T>
struct TComputationRequiresProcessingFunction<T, std::void_t<decltype(T::RequiresProcessingFunction)>>
    : public std::bool_constant<T::RequiresProcessingFunction>
{ };

//! Detects the optional `static constexpr bool InvokesProcessFunctionSync` marker a computation
//! sets when it runs the hosted function's sync phase (defaults to false).
template <class T, class = void>
struct TComputationInvokesProcessFunctionSync
    : public std::false_type
{ };

template <class T>
struct TComputationInvokesProcessFunctionSync<T, std::void_t<decltype(T::InvokesProcessFunctionSync)>>
    : public std::bool_constant<T::InvokesProcessFunctionSync>
{ };

////////////////////////////////////////////////////////////////////////////////

class TRegistry
{
public:
    using TParametersFactory = std::function<NYTree::TYsonStructPtr()>;
    using TProcessFunctionFactory = std::function<IProcessFunctionBasePtr()>;
    //! Recovers the optional #ISyncProcessFunction mix-in of a function instance without RTTI (a
    //! static cast fixed at registration, where the concrete type is known).
    using TProcessFunctionSyncViewer = std::function<ISyncProcessFunction*(IProcessFunctionBase*)>;
    using TDescribeTraitsFactory = std::function<IDescribeTraitsPtr(const TDescribeTraitsContext&)>;

    struct TParameterFactories
    {
        TParametersFactory Static;
        TParametersFactory Dynamic;
    };

    TRegistry();

    static TRegistry* Get();

    template <class T>
    void RegisterComputation();

    //! Registers a process function under its TypeName, with the YSON schema factories for its
    //! static and dynamic `processing_function_parameters` (parity with computation parameters).
    template <
        class TFunction,
        class TStaticParameters = TEmptyProcessFunctionParameters,
        class TDynamicParameters = TEmptyProcessFunctionParameters>
    void RegisterProcessFunction();

    IComputationPtr CreateComputation(
        const TComputationContextPtr& context,
        const TDynamicComputationContextPtr& dynamicContext);

    //! Instantiates the process function registered under |name|. Throws if |name| is unknown.
    IProcessFunctionBasePtr CreateProcessFunction(const std::string& name) const;

    //! Returns |function|'s sync mix-in if it opted in, else null — resolved without RTTI from the
    //! registration of |name|. |function| must be an instance created by CreateProcessFunction(|name|).
    ISyncProcessFunction* ViewProcessFunctionAsSync(const std::string& name, const IProcessFunctionBasePtr& function) const;

    IComputationControllerPtr CreateComputationController(
        const TComputationControllerContextPtr& context,
        const TDynamicComputationControllerContextPtr& dynamicContext);

    NYTree::TYsonStructPtr ParseComputationParameters(const TComputationSpecPtr& spec);

    NYTree::TYsonStructPtr ParseDynamicComputationParameters(
        const TComputationSpecPtr& spec,
        const TDynamicComputationSpecPtr& dynamicSpec);

    NYTree::TYsonStructPtr ParseDynamicComputationPartitionSpec(
        const TComputationSpecPtr& spec,
        const NYTree::IMapNodePtr& dynamicPartitionSpec);

    template <class T>
    void RegisterSource();

    ISourcePtr CreateSource(
        const TSourceContextPtr& context,
        const TDynamicSourceContextPtr& dynamicContext);

    ISourceControllerPtr CreateSourceController(
        const TSourceControllerContextPtr& context,
        const TDynamicSourceControllerContextPtr& dynamicContext);

    NYTree::TYsonStructPtr ParseSourceParameters(const TSourceSpecPtr& spec);

    NYTree::TYsonStructPtr ParseDynamicSourceParameters(
        const TSourceSpecPtr& spec,
        const TDynamicSourceSpecPtr& dynamicSpec);

    NYTree::TYsonStructPtr ParseDynamicSourcePartitionSpec(
        const TSourceSpecPtr& spec,
        const NYTree::IMapNodePtr& dynamicPartitionSpec);

    template <class T>
    void RegisterSink();

    ISinkPtr CreateSink(
        const TSinkContextPtr& context,
        const TDynamicSinkContextPtr& dynamicContext);

    ISinkControllerPtr CreateSinkController(
        const TSinkControllerContextPtr& context,
        const TDynamicSinkControllerContextPtr& dynamicContext);

    NYTree::TYsonStructPtr ParseSinkParameters(const TSinkSpecPtr& spec);

    NYTree::TYsonStructPtr ParseDynamicSinkParameters(
        const TSinkSpecPtr& spec,
        const TDynamicSinkSpecPtr& dynamicSpec);


    template <class T>
    void RegisterResource();

    IResourcePtr CreateResource(
        const TResourceContextPtr& context,
        const TDynamicResourceContextPtr& dynamicContext);

    NYTree::TYsonStructPtr ParseResourceParameters(const TResourceSpecPtr& spec);

    NYTree::TYsonStructPtr ParseResourceDynamicParameters(
        const TResourceSpecPtr& spec,
        const TDynamicResourceSpecPtr& dynamicSpec);

    template <class T>
    void RegisterExternalStateManager();

    IExternalStateManagerPtr CreateExternalStateManager(
        const TExternalStateManagerContextPtr& context,
        const TDynamicExternalStateManagerContextPtr& dynamicContext);

    NYTree::TYsonStructPtr ParseExternalStateManagerParameters(
        const TExternalStateManagerSpecPtr& spec);

    NYTree::TYsonStructPtr ParseDynamicExternalStateManagerParameters(
        const TExternalStateManagerSpecPtr& spec,
        const TDynamicExternalStateManagerSpecPtr& dynamicSpec);

    template <class T>
    void RegisterExternalStateJoiner();

    IExternalStateJoinerPtr CreateExternalStateJoiner(
        const TExternalStateJoinerContextPtr& context,
        const TDynamicExternalStateJoinerContextPtr& dynamicContext);

    NYTree::TYsonStructPtr ParseExternalStateJoinerParameters(
        const TExternalStateJoinerSpecPtr& spec);

    NYTree::TYsonStructPtr ParseDynamicExternalStateJoinerParameters(
        const TExternalStateJoinerSpecPtr& spec,
        const TDynamicExternalStateJoinerSpecPtr& dynamicSpec);

    void RegisterPayloadMigrationFunction(TStringBuf name, TPayloadMigrationFunction func);

    TPayloadMigrationFunction GetPayloadMigrationFunction(TStringBuf name) const;

    template <CYsonMessage T>
    void RegisterYsonMessage();
    TYsonMessagePtr CreateYsonMessage(TStringBuf name);

    std::vector<std::string> GetComputationTypeNames() const;
    std::vector<std::string> GetResourceTypeNames() const;
    std::vector<std::string> GetSourceTypeNames() const;
    std::vector<std::string> GetSinkTypeNames() const;
    std::vector<std::string> GetPayloadMigrationFunctionNames() const;
    std::vector<std::string> GetYsonMessageTypeNames() const;

    TParameterFactories GetComputationParameterFactories(TStringBuf typeName) const;
    TParameterFactories GetSourceParameterFactories(TStringBuf typeName) const;
    TParameterFactories GetSinkParameterFactories(TStringBuf typeName) const;

    //! Creates the describe traits for a computation/source/sink/state-manager class with the given context, or nullptr if unknown.
    IDescribeTraitsPtr CreateComputationDescribeTraits(TStringBuf typeName, const TDescribeTraitsContext& context) const;
    IDescribeTraitsPtr CreateSourceDescribeTraits(TStringBuf typeName, const TDescribeTraitsContext& context) const;
    IDescribeTraitsPtr CreateSinkDescribeTraits(TStringBuf typeName, const TDescribeTraitsContext& context) const;
    IDescribeTraitsPtr CreateExternalStateManagerDescribeTraits(TStringBuf typeName, const TDescribeTraitsContext& context) const;
    IDescribeTraitsPtr CreateExternalStateJoinerDescribeTraits(TStringBuf typeName, const TDescribeTraitsContext& context) const;

    void ValidateComputationSpec(const TComputationSpecPtr& spec) const;
    void ValidateSourceSpec(const TSourceSpecPtr& spec) const;
    void ValidateSinkSpec(const TSinkSpecPtr& spec) const;
    void ValidateResourceSpec(const TResourceSpecPtr& spec) const;
    void ValidateStreamSpec(const TStreamSpecPtr& spec) const;
    void ValidateExternalStateManagerSpec(const TExternalStateManagerSpecPtr& spec) const;
    void ValidateExternalStateJoinerSpec(const TExternalStateJoinerSpecPtr& spec) const;

    //! Whether the registered joiner class |typeName| is visitor-driven; ``false`` for an
    //! unregistered class (its existence is reported by #ValidateComputationSpec separately).
    bool IsExternalStateJoinerVisitorDriven(const std::string& typeName) const;

    //! Recursively walks the (already-parsed) parameters struct and collects every
    //! YT-path ownership claim declared on a path-typed field via
    //! `.AddOption(EYTPathOwnership)`. Each claimed path is normalized.
    std::vector<TYTPathClaim> CollectYTPathClaims(const NYTree::TYsonStructPtr& parameters) const;

    void ValidateYsonMessageType(TStringBuf name, const TYsonMessagePtr& ysonMessage) const;

    // Validate spec and return list of errors.
    // Possible errors: failed parsing, unrecognized fields.
    std::vector<TError> ValidatePipelineSpecParseability(const NYTree::IMapNodePtr& specNode) const;
    std::vector<TError> ValidateDynamicPipelineSpecParseability(const TPipelineSpecPtr& spec, const NYTree::IMapNodePtr& dynamicSpecNode) const;

protected:
    NLogging::TLogger Logger;

private:
    struct TComputationDescriptor
    {
        std::function<IComputationPtr(const TComputationContextPtr& context, const TDynamicComputationContextPtr& dynamicContext)> Factory;
        std::function<IComputationControllerPtr(const TComputationControllerContextPtr& context, const TDynamicComputationControllerContextPtr& dynamicContext)> ControllerFactory;
        TParametersFactory ParametersFactory;
        TParametersFactory DynamicParametersFactory;
        TParametersFactory DynamicPartitionSpecFactory;
        TDescribeTraitsFactory DescribeTraitsFactory;
        std::function<void(const TComputationSpec&)> ValidateSpec;
        //! The computation hosts a process function, so its spec must name a registered
        //! `processing_function` (validated at spec-parse time).
        bool RequiresProcessingFunction = false;
        //! The computation runs the hosted function's sync phase (validated at spec-parse time).
        bool InvokesProcessFunctionSync = false;
    };

    struct TProcessFunctionDescriptor
    {
        TProcessFunctionFactory Factory;
        TProcessFunctionSyncViewer SyncView;
        TParametersFactory StaticParametersFactory;
        TParametersFactory DynamicParametersFactory;
        //! The function needs a sync phase, so its host must run one (validated at spec-parse time).
        bool OverridesSync = false;
    };

    struct TSourceDescriptor
    {
        std::function<ISourcePtr(const TSourceContextPtr& context, const TDynamicSourceContextPtr& dynamicContext)> Factory;
        std::function<ISourceControllerPtr(const TSourceControllerContextPtr& context, const TDynamicSourceControllerContextPtr& dynamicContext)> ControllerFactory;
        TParametersFactory ParametersFactory;
        TParametersFactory DynamicParametersFactory;
        TParametersFactory DynamicPartitionSpecFactory;
        TDescribeTraitsFactory DescribeTraitsFactory;
        std::function<void(const TSourceSpec&)> ValidateSpec;
    };

    struct TSinkDescriptor
    {
        std::function<ISinkPtr(const TSinkContextPtr& context, const TDynamicSinkContextPtr& dynamicContext)> Factory;
        std::function<ISinkControllerPtr(const TSinkControllerContextPtr& context, const TDynamicSinkControllerContextPtr& dynamicContext)> ControllerFactory;
        TParametersFactory ParametersFactory;
        TParametersFactory DynamicParametersFactory;
        TDescribeTraitsFactory DescribeTraitsFactory;
        std::function<void(const TSinkSpec&)> ValidateSpec;
    };

    struct TResourceDescriptor
    {
        std::function<IResourcePtr(const TResourceContextPtr& context, const TDynamicResourceContextPtr& dynamicContext)> Factory;
        TParametersFactory ParametersFactory;
        TParametersFactory DynamicParametersFactory;
        std::function<void(const TResourceSpec&)> ValidateSpec;
    };

    struct TExternalStateManagerDescriptor
    {
        std::function<IExternalStateManagerPtr(
            const TExternalStateManagerContextPtr& context,
            const TDynamicExternalStateManagerContextPtr& dynamicContext)>
            Factory;
        TParametersFactory ParametersFactory;
        TParametersFactory DynamicParametersFactory;
        TDescribeTraitsFactory DescribeTraitsFactory;
        std::function<void(const TExternalStateManagerSpec&)> ValidateSpec;
    };

    struct TExternalStateJoinerDescriptor
    {
        std::function<IExternalStateJoinerPtr(
            const TExternalStateJoinerContextPtr& context,
            const TDynamicExternalStateJoinerContextPtr& dynamicContext)>
            Factory;
        TParametersFactory ParametersFactory;
        TParametersFactory DynamicParametersFactory;
        TDescribeTraitsFactory DescribeTraitsFactory;
        std::function<void(const TExternalStateJoinerSpec&)> ValidateSpec;
        bool VisitorDriven = false;
    };

    struct TYsonMessageDescriptor
    {
        std::function<TYsonMessagePtr()> Factory;
        const NYTree::IYsonStructMeta* Meta;
    };

    THashMap<std::string, TComputationDescriptor> TypeNameToComputationDescriptor_;
    THashMap<std::string, TProcessFunctionDescriptor> TypeNameToProcessFunctionDescriptor_;
    THashMap<std::string, TSourceDescriptor> TypeNameToSourceDescriptor_;
    THashMap<std::string, TSinkDescriptor> TypeNameToSinkDescriptor_;
    THashMap<std::string, TResourceDescriptor> TypeNameToResourceDescriptor_;
    THashMap<std::string, TExternalStateManagerDescriptor> TypeNameToExternalStateManagerDescriptor_;
    THashMap<std::string, TExternalStateJoinerDescriptor> TypeNameToExternalStateJoinerDescriptor_;
    THashMap<std::string, TPayloadMigrationFunction> TypeNameToPayloadMigrationFunction_;
    THashMap<std::string, TYsonMessageDescriptor> TypeNameToYsonMessageDescriptor_;

    Y_DECLARE_SINGLETON_FRIEND();

    const TComputationDescriptor& GetComputationDescriptor(TStringBuf typeName) const;
    //! Descriptor for |typeName|, or nullptr if no such process function is registered.
    const TProcessFunctionDescriptor* FindProcessFunctionDescriptor(TStringBuf typeName) const;
    const TSourceDescriptor& GetSourceDescriptor(TStringBuf typeName) const;
    const TSinkDescriptor& GetSinkDescriptor(TStringBuf typeName) const;
    const TResourceDescriptor& GetResourceDescriptor(TStringBuf typeName) const;
    const TExternalStateManagerDescriptor& GetExternalStateManagerDescriptor(TStringBuf typeName) const;
    const TExternalStateJoinerDescriptor& GetExternalStateJoinerDescriptor(TStringBuf typeName) const;
    const TYsonMessageDescriptor& GetYsonMessageDescriptor(TStringBuf typeName) const;

    template <class T, class TDescriptor>
    void EmplaceDescriptorOrCrash(THashMap<std::string, TDescriptor>& descriptorMap, TDescriptor descriptor);
};

////////////////////////////////////////////////////////////////////////////////

#define YT_FLOW_DEFINE_COMPUTATION(type)                             \
    YT_STATIC_INITIALIZER({                                          \
        ::NYT::NFlow::TRegistry::Get()->RegisterComputation<type>(); \
    })

//! Registers a process function under its TypeName so a computation spec can reference it via the
//! `processing_function` field. Optional arguments declare the function's YSON parameters types,
//! used to validate its static (resp. dynamic) `processing_function_parameters` block:
//!   YT_FLOW_DEFINE_PROCESS_FUNCTION(fn)                              // parameterless
//!   YT_FLOW_DEFINE_PROCESS_FUNCTION(fn, StaticParams)                // static params
//!   YT_FLOW_DEFINE_PROCESS_FUNCTION(fn, StaticParams, DynamicParams)
#define YT_FLOW_DEFINE_PROCESS_FUNCTION_1(function)                          \
    YT_STATIC_INITIALIZER({                                                  \
        ::NYT::NFlow::TRegistry::Get()->RegisterProcessFunction<function>(); \
    })

#define YT_FLOW_DEFINE_PROCESS_FUNCTION_2(function, staticParametersType)                          \
    YT_STATIC_INITIALIZER({                                                                        \
        ::NYT::NFlow::TRegistry::Get()->RegisterProcessFunction<function, staticParametersType>(); \
    })

#define YT_FLOW_DEFINE_PROCESS_FUNCTION_3(function, staticParametersType, dynamicParametersType)                          \
    YT_STATIC_INITIALIZER({                                                                                               \
        ::NYT::NFlow::TRegistry::Get()->RegisterProcessFunction<function, staticParametersType, dynamicParametersType>(); \
    })

#define YT_FLOW_DEFINE_PROCESS_FUNCTION_DISPATCH(_1, _2, _3, NAME, ...) NAME

#define YT_FLOW_DEFINE_PROCESS_FUNCTION(...)  \
    YT_FLOW_DEFINE_PROCESS_FUNCTION_DISPATCH( \
        __VA_ARGS__,                          \
        YT_FLOW_DEFINE_PROCESS_FUNCTION_3,    \
        YT_FLOW_DEFINE_PROCESS_FUNCTION_2,    \
        YT_FLOW_DEFINE_PROCESS_FUNCTION_1)    \
    (__VA_ARGS__)

#define YT_FLOW_DEFINE_SOURCE(type)                             \
    YT_STATIC_INITIALIZER({                                     \
        ::NYT::NFlow::TRegistry::Get()->RegisterSource<type>(); \
    })

#define YT_FLOW_DEFINE_SINK(type)                             \
    YT_STATIC_INITIALIZER({                                   \
        ::NYT::NFlow::TRegistry::Get()->RegisterSink<type>(); \
    })

#define YT_FLOW_DEFINE_RESOURCE(type)                             \
    YT_STATIC_INITIALIZER({                                       \
        ::NYT::NFlow::TRegistry::Get()->RegisterResource<type>(); \
    })

#define YT_FLOW_DEFINE_EXTERNAL_STATE_MANAGER(type)                           \
    YT_STATIC_INITIALIZER({                                                   \
        ::NYT::NFlow::TRegistry::Get()->RegisterExternalStateManager<type>(); \
    })

#define YT_FLOW_DEFINE_EXTERNAL_STATE_JOINER(type)                           \
    YT_STATIC_INITIALIZER({                                                  \
        ::NYT::NFlow::TRegistry::Get()->RegisterExternalStateJoiner<type>(); \
    })

#define YT_FLOW_DEFINE_PAYLOAD_MIGRATION_FUNCTION(func)                                \
    YT_STATIC_INITIALIZER({                                                            \
        ::NYT::NFlow::TRegistry::Get()->RegisterPayloadMigrationFunction(#func, func); \
    })

#define YT_FLOW_DEFINE_YSON_MESSAGE(type)                            \
    YT_STATIC_INITIALIZER({                                          \
        ::NYT::NFlow::TRegistry::Get()->RegisterYsonMessage<type>(); \
    })

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define REGISTRY_INL_H_
#include "registry-inl.h"
#undef REGISTRY_INL_H_
