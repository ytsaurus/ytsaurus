#pragma once

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <util/generic/strbuf.h>
#include <util/system/tempfile.h>

namespace NYql::NYtflow::NTest {

template <class TBuildRoot>
void WriteRuntimeNode(TTempFileHandle& file, TBuildRoot buildRoot)
{
    using namespace NKikimr::NMiniKQL;

    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    auto registry = CreateFunctionRegistry(CreateBuiltinRegistry());
    TProgramBuilder programBuilder(env, *registry);
    const auto root = buildRoot(programBuilder);
    const auto serialized = SerializeRuntimeNode(root, env);
    file.Write(serialized.data(), serialized.size());
}

inline void WriteConditionalStringLambda(TTempFileHandle& file, TStringBuf result)
{
    using namespace NKikimr::NMiniKQL;

    WriteRuntimeNode(file, [result](TProgramBuilder& programBuilder) {
        return programBuilder.If(
            programBuilder.NewDataLiteral<bool>(true),
            programBuilder.NewDataLiteral<NUdf::EDataSlot::String>(result),
            programBuilder.NewDataLiteral<NUdf::EDataSlot::String>("unexpected"));
    });
}

inline void WriteZeroInputCallable(TTempFileHandle& file, TStringBuf callableName)
{
    using namespace NKikimr::NMiniKQL;

    WriteRuntimeNode(file, [callableName](TProgramBuilder& programBuilder) {
        TCallableBuilder callableBuilder(
            programBuilder.GetTypeEnvironment(),
            callableName,
            programBuilder.NewDataType(NUdf::TDataType<bool>::Id));
        return TRuntimeNode(callableBuilder.Build(), /*isImmediate*/ false);
    });
}

} // namespace NYql::NYtflow::NTest
