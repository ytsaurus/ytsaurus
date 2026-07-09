#pragma once

#ifndef REGISTRY_INL_H_
    #error "Direct inclusion of this file is not allowed, include registry.h"
    // For the sake of sane code completion.
    #include "registry.h"
#endif

#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <util/system/type_name.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class... TParts>
struct TCombinedYsonStruct
    : public TParts...
{
    REGISTER_YSON_STRUCT(TCombinedYsonStruct);

    static void Register(TRegistrar /*registrar*/)
    { }
};

////////////////////////////////////////////////////////////////////////////////

template <class TFirst, class TSecond>
using TUnitedYsonStruct = std::conditional_t<
    std::is_base_of_v<TSecond, TFirst>, TFirst, TCombinedYsonStruct<TFirst, TSecond>>;

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TUnitedParameters
{ };

template <class T>
struct TDynamicUnitedParameters
{ };

#define YT_FLOW_DEFINE_UNITED_PARAMETERS(Entity, OptionalDynamic)                \
    template <std::derived_from<I##Entity> T>                                    \
    struct T##OptionalDynamic##UnitedParameters<T>                               \
        : public TUnitedYsonStruct<                                              \
              typename T::T##OptionalDynamic##Parameters,                        \
              typename T::T##Entity##Controller::T##OptionalDynamic##Parameters> \
    {                                                                            \
        REGISTER_YSON_STRUCT(T##OptionalDynamic##UnitedParameters);              \
                                                                                 \
        static void Register(TRegistrar /*registrar*/)                           \
        { }                                                                      \
    };

YT_FLOW_DEFINE_UNITED_PARAMETERS(Computation, )
YT_FLOW_DEFINE_UNITED_PARAMETERS(Computation, Dynamic)
YT_FLOW_DEFINE_UNITED_PARAMETERS(Source, )
YT_FLOW_DEFINE_UNITED_PARAMETERS(Source, Dynamic)
YT_FLOW_DEFINE_UNITED_PARAMETERS(Sink, )
YT_FLOW_DEFINE_UNITED_PARAMETERS(Sink, Dynamic)

#undef YT_FLOW_DECLARE_UNITED_PARAMETERS

template <std::derived_from<IResource> T>
struct TUnitedParameters<T>
    : public T::TParameters
{
    REGISTER_YSON_STRUCT(TUnitedParameters);

    static void Register(TRegistrar /*registrar*/)
    { }
};

template <std::derived_from<IResource> T>
struct TDynamicUnitedParameters<T>
    : public T::TDynamicParameters
{
    REGISTER_YSON_STRUCT(TDynamicUnitedParameters);

    static void Register(TRegistrar /*registrar*/)
    { }
};

////////////////////////////////////////////////////////////////////////////////

template <class TParameters, class TParametersPtr>
constexpr void ValidateParametersType()
{
    static_assert(std::is_same_v<TParameters, typename TParametersPtr::TUnderlying>,
        "TParameters and TParametersPtr must be consistent. Use macroses YT_FLOW_REGISTER_[DYNAMIC_]PARAMETERS");
}

template <class T>
constexpr void ValidateParametersTypes()
{
    ValidateParametersType<typename T::TParameters, typename T::TParametersPtr>();
    ValidateParametersType<typename T::TDynamicParameters, typename T::TDynamicParametersPtr>();
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class TDescriptor>
void TRegistry::EmplaceDescriptorOrCrash(THashMap<std::string, TDescriptor>& descriptorMap, TDescriptor descriptor)
{
    auto [it, success] = descriptorMap.try_emplace(TypeName<T>(), std::move(descriptor));
    if (!success) {
        YT_LOG_FATAL("Can not emplace %Qv for type %Qv because it is already present in descriptor map. "
            "Check that YT_FLOW_DEFINE_* macro is not called in header file. Or in two source files.",
            TypeName<TDescriptor>(),
            TypeName<T>());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TRegistry::RegisterComputation()
{
    static_assert(std::is_base_of_v<IComputationController, typename T::TComputationController>,
        "Computation must define the controller");

    ValidateParametersTypes<T>();
    ValidateParametersTypes<typename T::TComputationController>();

    auto [it, success] = TypeNameToComputationDescriptor_.try_emplace(
        TypeName<T>(),
        TComputationDescriptor{
            .Factory = &New<T, const TComputationContextPtr&, const TDynamicComputationContextPtr&>,
            .ControllerFactory = &New<typename T::TComputationController, const TComputationControllerContextPtr&, const TDynamicComputationControllerContextPtr&>,
            .ParametersFactory = &New<TUnitedParameters<T>>,
            .DynamicParametersFactory = &New<TDynamicUnitedParameters<T>>,
            .DynamicPartitionSpecFactory = &New<typename T::TDynamicPartitionSpec>,
            .DescribeTraitsFactory = [] (const TDescribeTraitsContext& context) -> IDescribeTraitsPtr {
                return New<typename T::TDescribeTraits>(context);
            },
            .ValidateSpec = [] (const TComputationSpec& spec) {
                T::TValidator::Validate(spec);
            },
            .RequiresProcessingFunction = TComputationRequiresProcessingFunction<T>::value,
            .InvokesProcessFunctionSync = TComputationInvokesProcessFunctionSync<T>::value,
        });
    if (!success) {
        YT_LOG_FATAL("Can not emplace computation %Qv because it is already present in descriptor map. "
            "Check that a YT_FLOW_DEFINE_* macro is not called in a header file or in two source files.",
            TypeName<T>());
    }
}

template <class TFunction, class TStaticParameters, class TDynamicParameters>
void TRegistry::RegisterProcessFunction()
{
    EmplaceDescriptorOrCrash<TFunction>(
        TypeNameToProcessFunctionDescriptor_,
        TProcessFunctionDescriptor{
            .Factory = [] {
                return IProcessFunctionBasePtr(New<TFunction>());
            },
            .SyncView = [] (IProcessFunctionBase* function) -> ISyncProcessFunction* {
                if constexpr (std::is_base_of_v<ISyncProcessFunction, TFunction>) {
                    return static_cast<TFunction*>(function);
                } else {
                    return nullptr;
                }
            },
            .StaticParametersFactory = [] {
                return NYTree::TYsonStructPtr(New<TStaticParameters>());
            },
            .DynamicParametersFactory = [] {
                return NYTree::TYsonStructPtr(New<TDynamicParameters>());
            },
            .OverridesSync = std::is_base_of_v<ISyncProcessFunction, TFunction>,
        });
}

template <class T>
void TRegistry::RegisterSource()
{
    static_assert(std::is_base_of_v<ISourceController, typename T::TSourceController>,
        "Source must define the controller");

    ValidateParametersTypes<T>();

    static_assert(std::is_base_of_v<typename T::TSourceController::TParameters, typename T::TParameters>,
        "Source parameters must extend (or be equal) source controller parameters");
    static_assert(std::is_base_of_v<typename T::TSourceController::TDynamicParameters, typename T::TDynamicParameters>,
        "Source parameters must extend (or be equal) source controller parameters");

    EmplaceDescriptorOrCrash<T>(
        TypeNameToSourceDescriptor_,
        TSourceDescriptor{
            .Factory = &New<T, const TSourceContextPtr&, const TDynamicSourceContextPtr&>,
            .ControllerFactory = &New<typename T::TSourceController, const TSourceControllerContextPtr&, const TDynamicSourceControllerContextPtr&>,
            .ParametersFactory = &New<TUnitedParameters<T>>,
            .DynamicParametersFactory = &New<TDynamicUnitedParameters<T>>,
            .DynamicPartitionSpecFactory = &New<typename T::TDynamicPartitionSpec>,
            .DescribeTraitsFactory = [] (const TDescribeTraitsContext& context) -> IDescribeTraitsPtr {
                return New<typename T::TDescribeTraits>(context);
            },
            .ValidateSpec = [] (const TSourceSpec& spec) {
                T::TValidator::Validate(spec);
            },
        });
}

template <class T>
void TRegistry::RegisterSink()
{
    static_assert(std::is_base_of_v<ISinkController, typename T::TSinkController>,
        "Sink must define the controller");

    ValidateParametersTypes<T>();

    EmplaceDescriptorOrCrash<T>(
        TypeNameToSinkDescriptor_,
        TSinkDescriptor{
            .Factory = &New<T, const TSinkContextPtr&, const TDynamicSinkContextPtr&>,
            .ControllerFactory = &New<typename T::TSinkController, const TSinkControllerContextPtr&, const TDynamicSinkControllerContextPtr&>,
            .ParametersFactory = &New<TUnitedParameters<T>>,
            .DynamicParametersFactory = &New<TDynamicUnitedParameters<T>>,
            .DescribeTraitsFactory = [] (const TDescribeTraitsContext& context) -> IDescribeTraitsPtr {
                return New<typename T::TDescribeTraits>(context);
            },
            .ValidateSpec = [] (const TSinkSpec& spec) {
                T::TValidator::Validate(spec);
            },
        });
}

template <class T>
void TRegistry::RegisterResource()
{
    EmplaceDescriptorOrCrash<T>(
        TypeNameToResourceDescriptor_,
        TResourceDescriptor{
            .Factory = &New<T, const TResourceContextPtr&, const TDynamicResourceContextPtr&>,
            .ParametersFactory = &New<TUnitedParameters<T>>,
            .DynamicParametersFactory = &New<TDynamicUnitedParameters<T>>,
            .ValidateSpec = [] (const TResourceSpec& spec) {
                T::TValidator::Validate(spec);
            },
        });
}

template <class T>
void TRegistry::RegisterExternalStateManager()
{
    static_assert(std::is_base_of_v<IExternalStateManager, T>);

    ValidateParametersTypes<T>();

    EmplaceDescriptorOrCrash<T>(
        TypeNameToExternalStateManagerDescriptor_,
        TExternalStateManagerDescriptor{
            .Factory = &New<T, const TExternalStateManagerContextPtr&, const TDynamicExternalStateManagerContextPtr&>,
            .ParametersFactory = &New<typename T::TParameters>,
            .DynamicParametersFactory = &New<typename T::TDynamicParameters>,
            .DescribeTraitsFactory = [] (const TDescribeTraitsContext& context) -> IDescribeTraitsPtr {
                return New<typename T::TDescribeTraits>(context);
            },
            .ValidateSpec = [] (const TExternalStateManagerSpec& spec) {
                T::TValidator::Validate(spec);
            },
        });
}

template <class T>
void TRegistry::RegisterExternalStateJoiner()
{
    static_assert(std::is_base_of_v<IExternalStateJoiner, T>);

    ValidateParametersTypes<T>();

    EmplaceDescriptorOrCrash<T>(
        TypeNameToExternalStateJoinerDescriptor_,
        TExternalStateJoinerDescriptor{
            .Factory = &New<T, const TExternalStateJoinerContextPtr&, const TDynamicExternalStateJoinerContextPtr&>,
            .ParametersFactory = &New<typename T::TParameters>,
            .DynamicParametersFactory = &New<typename T::TDynamicParameters>,
            .DescribeTraitsFactory = [] (const TDescribeTraitsContext& context) -> IDescribeTraitsPtr {
                return New<typename T::TDescribeTraits>(context);
            },
            .ValidateSpec = [] (const TExternalStateJoinerSpec& spec) {
                T::TValidator::Validate(spec);
            },
            .VisitorDriven = T::VisitorDriven,
        });
}

template <CYsonMessage T>
void TRegistry::RegisterYsonMessage()
{
    EmplaceDescriptorOrCrash<T>(
        TypeNameToYsonMessageDescriptor_,
        TYsonMessageDescriptor{
            .Factory = &New<T>,
            .Meta = New<T>()->GetMeta(),
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
