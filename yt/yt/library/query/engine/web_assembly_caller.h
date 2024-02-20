#pragma once

#include <yt/yt/library/codegen/module.h>

#include <yt/yt/library/web_assembly/api/compartment.h>
#include <yt/yt/library/web_assembly/api/type_builder.h>

#include <yt/yt/core/actions/callback.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <class TSignature, class TPISignature>
class TCGWebAssemblyCaller;

template <class TResult, class... TArgs, class TPIResult, class... TPIArgs>
class TCGWebAssemblyCaller<TResult(TArgs...), TPIResult(TPIArgs...)>
    : public NYT::NDetail::TBindStateBase
{
public:
    TCGWebAssemblyCaller(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
       const NYT::TSourceLocation& location,
#endif
        NCodegen::TCGModulePtr module,
        const TString& function)
        : NYT::NDetail::TBindStateBase(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
            location
#endif
        )
        , Module_(std::move(module))
        , FunctionName_(function)
        , RuntimeType_(NWebAssembly::TFunctionTypeBuilder</*IsIntrinsic*/ false, TPIResult(TPIArgs...)>::Get())
    { }

    TResult Run(TCallArg<TArgs>... args);

    static auto StaticInvoke(TCallArg<TArgs>... args, NYT::NDetail::TBindStateBase* stateBase)
    {
        auto* state = static_cast<TCGWebAssemblyCaller*>(stateBase);
        state->Run(std::forward<TArgs>(args)...);
    }

private:
    NCodegen::TCGModulePtr Module_;
    TString FunctionName_;
    NWebAssembly::TWebAssemblyRuntimeType RuntimeType_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
