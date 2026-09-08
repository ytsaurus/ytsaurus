#include "yql_ytflow_function_registry.h"

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/concurrency/coroutine.h>

namespace NYql::NYtflow {

TFunctionRegistryHolder::TFunctionRegistryHolder(
    TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> functionRegistry)
    : FunctionRegistry_(std::move(functionRegistry))
{ }

const NKikimr::NMiniKQL::IFunctionRegistry& TFunctionRegistryHolder::GetFunctionRegistry() const
{
    return *FunctionRegistry_;
}

TIntrusivePtr<TFunctionRegistryHolder> CreateFunctionRegistryHolder(
    TVector<TString> udfPaths)
{
    auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(
        NKikimr::NMiniKQL::CreateBuiltinRegistry())->Clone();

    // NB: under debug build these calls do not fit in regular fiber stack
    // due to python udf loading
    using TSignature = void(TVector<TString>);
    auto coroutine = NYT::NConcurrency::TCoroutine<TSignature>(
        BIND([functionRegistry](
            NYT::NConcurrency::TCoroutine<TSignature>& /*self*/,
            TVector<TString> udfPaths
        ) {
            for (const auto& udfPath : udfPaths) {
                functionRegistry->LoadUdfs(udfPath, /*remappings*/ {});
            }
        }),
        NYT::NConcurrency::EExecutionStackKind::Large);

    coroutine.Run(std::move(udfPaths));
    YT_VERIFY(coroutine.IsCompleted());

    return new TFunctionRegistryHolder(std::move(functionRegistry));
}

} // namespace NYql::NYtflow
