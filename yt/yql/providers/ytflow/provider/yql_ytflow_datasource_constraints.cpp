#include "yql_ytflow_provider_impl.h"

#include <yql/essentials/providers/common/transform/yql_visit.h>
#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/ast/yql_constraint.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>


namespace NYql {

using namespace NNodes;

namespace {

class TYtflowDataSourceConstraintTransformer : public TVisitorTransformerBase {
public:
    TYtflowDataSourceConstraintTransformer(TYtflowState::TPtr state)
        : TVisitorTransformerBase(false)
        , State_(std::move(state))
    {
#define ADD_HANDLER(nodeType, method) \
    AddHandler({nodeType::CallableName()}, Hndl(&TYtflowDataSourceConstraintTransformer::method))

        ADD_HANDLER(TYtflowReadWrap, HandleReadWrap);

#undef ADD_HANDLER
    }

    TStatus HandleReadWrap(TExprBase input, TExprContext& /*ctx*/) {
        TYtflowReadWrap readWrap = input.Cast<TYtflowReadWrap>();
        input.Ptr()->CopyConstraints(readWrap.Input().Ref());
        return TStatus::Ok;
    }

private:
    const TYtflowState::TPtr State_;
};

}

THolder<IGraphTransformer> CreateYtflowDataSourceConstraintTransformer(TYtflowState::TPtr state) {
    return MakeHolder<TYtflowDataSourceConstraintTransformer>(std::move(state));
}

}
