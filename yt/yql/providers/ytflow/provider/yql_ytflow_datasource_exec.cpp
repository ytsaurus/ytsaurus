#include "yql_ytflow_provider_impl.h"

#include <yql/essentials/core/yql_execution.h>
#include <yql/essentials/providers/common/transform/yql_exec.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>


namespace NYql {

using namespace NNodes;


class TYtflowDataSourceExecTransformer: public TExecTransformerBase {
public:
    TYtflowDataSourceExecTransformer(TYtflowState::TPtr state)
        : State_(std::move(state))
    {
#define HANDLER(name) \
    Hndl(&TYtflowDataSourceExecTransformer::Handle##name)

        AddHandler({TYtflowPersistentSource::CallableName()}, RequireNone(), Pass());

#undef HANDLER
    }

private:
    TYtflowState::TPtr State_;
};


THolder<TExecTransformerBase> CreateYtflowDataSourceExecTransformer(TYtflowState::TPtr state) {
    return MakeHolder<TYtflowDataSourceExecTransformer>(std::move(state));
}

} // namespace NYql
