#pragma once

#include <contrib/ydb/core/kqp/host/kqp_translate.h>
#include <contrib/ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <yql/essentials/ast/yql_expr.h>

namespace NYql {

TExprNode::TPtr FindTopLevelRead(const TExprNode::TPtr& queryGraph);

TExprNode::TPtr RewriteReadFromView(
    const TExprNode::TPtr& node,
    TExprContext& ctx,
    NKikimr::NKqp::TKqpTranslationSettingsBuilder& settingsBuilder,
    IModuleResolver::TPtr moduleResolver,
    const TViewPersistedData& viewData
);

}
