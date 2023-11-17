#pragma once

#include <contrib/ydb/library/yql/core/yql_data_provider.h>
#include <contrib/ydb/library/yql/core/yql_type_annotation.h>
#include <contrib/ydb/library/yql/core/yql_graph_transformer.h>

#include <util/generic/ptr.h>

namespace NYql {

TAutoPtr<IGraphTransformer> CreateCommonOptTransformer(TTypeAnnotationContext* typeCtx);
TAutoPtr<IGraphTransformer> CreateCommonOptFinalTransformer(TTypeAnnotationContext* typeCtx);

}
