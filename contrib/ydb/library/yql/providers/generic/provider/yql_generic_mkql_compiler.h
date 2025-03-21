#pragma once

#include "yql_generic_provider.h"

#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>
#include <contrib/ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>

namespace NYql {

    void RegisterDqGenericMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler, const TGenericState::TPtr& state);

} // namespace NYql
