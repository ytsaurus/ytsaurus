#include "yql_dq_state.h"

#include <contrib/ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <contrib/ydb/library/yql/providers/common/transform/yql_visit.h>
#include <contrib/ydb/library/yql/core/yql_expr_constraint.h>
#include <contrib/ydb/library/yql/ast/yql_constraint.h>

namespace NYql {

using namespace NNodes;

namespace {

class TDqDataSourceConstraintTransformer : public TVisitorTransformerBase {
public:
    TDqDataSourceConstraintTransformer()
        : TVisitorTransformerBase(true)
    {
        AddHandler({
            TCoConfigure::CallableName(),
            TDqReadWrap::CallableName(),
            TDqReadWideWrap::CallableName(),
            TDqReadBlockWideWrap::CallableName(),
            TDqSource::CallableName(),
            TDqSourceWrap::CallableName(),
            TDqSourceWideWrap::CallableName(),
            TDqSourceWideBlockWrap::CallableName(),
            TDqPhyLength::CallableName()
        }, Hndl(&TDqDataSourceConstraintTransformer::HandleDefault));
    }

    TStatus HandleDefault(TExprBase, TExprContext&) {
        return TStatus::Ok;
    }
};

}

THolder<IGraphTransformer> CreateDqDataSourceConstraintTransformer() {
    return THolder<IGraphTransformer>(new TDqDataSourceConstraintTransformer());
}

}
