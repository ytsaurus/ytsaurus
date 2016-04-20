#include "function_registry.h"
#include "builtin_functions.h"
#include "user_defined_functions.h"
#include "udf/is_null_arc.h"
#include "udf/sum_arc.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

IFunctionRegistryPtr CreateBuiltinFunctionRegistry()
{
    auto registry = New<TFunctionRegistry>();

    registry->RegisterFunction(CreateIfFunction());

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "is_null",
        std::vector<TType>{0},
        EValueType::Boolean,
        TSharedRef(
            is_null_bc,
            is_null_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue));

    auto typeArg = TTypeArgument{0};
    auto sumConstraints = std::unordered_map<TTypeArgument, TUnionType>();
    sumConstraints[typeArg] = std::vector<EValueType>{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double};

    registry->RegisterAggregateFunction(New<TUserDefinedAggregateFunction>(
        "sum",
        sumConstraints,
        typeArg,
        typeArg,
        typeArg,
        TSharedRef(
            sum_bc,
            sum_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue));

    return registry;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
