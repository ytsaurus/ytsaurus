#pragma once

#include <contrib/ydb/library/yql/core/yql_graph_transformer.h>

namespace NYql {

THolder<IGraphTransformer> CreateDqDataSourceConstraintTransformer();

} // NYql
