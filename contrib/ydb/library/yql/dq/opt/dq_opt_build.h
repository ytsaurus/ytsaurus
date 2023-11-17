#pragma once

#include <contrib/ydb/library/yql/core/yql_graph_transformer.h>
#include <contrib/ydb/library/yql/core/yql_type_annotation.h>
#include <contrib/ydb/library/yql/dq/common/dq_common.h>

namespace NYql::NDq {

enum EChannelMode {
    CHANNEL_SCALAR,
    CHANNEL_WIDE,
    CHANNEL_WIDE_BLOCK,
};

TAutoPtr<IGraphTransformer> CreateDqBuildPhyStagesTransformer(bool allowDependantConsumers, TTypeAnnotationContext& typesCtx, EChannelMode mode);

} // namespace NYql::NDq
