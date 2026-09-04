#pragma once

#include <yql/essentials/minikql/mkql_function_registry.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql::NYtflow {

// Owns a function registry together with its loaded UDF modules and libraries.
// Enables sharing of the loaded registry and UDF state among computation
// patterns and graphs. The legacy path still creates one per graph.
// Exposes only the immutable registry interface after construction.
class TFunctionRegistryHolder
    : public TThrRefBase
{
public:
    // Loaded registries are read concurrently. UDF modules are required to keep
    // BuildFunctionTypeInfo reentrant when patterns are built in parallel.
    const NKikimr::NMiniKQL::IFunctionRegistry& GetFunctionRegistry() const;

private:
    friend TIntrusivePtr<TFunctionRegistryHolder> CreateFunctionRegistryHolder(
        TVector<TString> udfPaths);

    explicit TFunctionRegistryHolder(
        TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> functionRegistry);

private:
    TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FunctionRegistry_;
};

TIntrusivePtr<TFunctionRegistryHolder> CreateFunctionRegistryHolder(
    TVector<TString> udfPaths);

} // namespace NYql::NYtflow
