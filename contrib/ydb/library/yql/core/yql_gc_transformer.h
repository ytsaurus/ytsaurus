#pragma once

#include <contrib/ydb/library/yql/ast/yql_gc_nodes.h>
#include "yql_graph_transformer.h"

namespace NYql {

TAutoPtr<IGraphTransformer> CreateGcNodeTransformer();

}
