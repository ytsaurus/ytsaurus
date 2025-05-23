#pragma once

#include "kqp_opt.h"

#include <contrib/ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>


namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NOpt;

/**
 * Constant folding transformer finds constant expressions in FlatMaps, evaluates them and
 * substitutes the result in the AST
*/
class TKqpConstantFoldingTransformer : public TSyncTransformerBase {
    public:
        TKqpConstantFoldingTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx,
            const TKikimrConfiguration::TPtr& config) : 
            Config(config),
            TypeCtx(typeCtx),
            KqpCtx(*kqpCtx) {}

        // Main method of the transformer
        IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;
        void Rewind() override;
        
    private:
        const TKikimrConfiguration::TPtr& Config;
        TTypeAnnotationContext& TypeCtx;
        const TKqpOptimizeContext& KqpCtx;
};

TAutoPtr<IGraphTransformer> CreateKqpConstantFoldingTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx, const TKikimrConfiguration::TPtr& config);

}
}