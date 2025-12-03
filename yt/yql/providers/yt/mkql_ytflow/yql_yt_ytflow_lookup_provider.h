#pragma once

#include <yt/yql/providers/ytflow/integration/mkql_interface/yql_ytflow_lookup_provider.h>


namespace NYql {

void RegisterYtYtflowLookupProvider(IYtflowLookupProviderRegistry& registry);

} // namespace NYql
