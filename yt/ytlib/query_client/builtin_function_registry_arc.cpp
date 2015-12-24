#include "function_registry.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

IFunctionRegistryPtr CreateBuiltinFunctionRegistry()
{
    auto registry = New<TFunctionRegistry>();
    return registry;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
