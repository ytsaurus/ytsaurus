#pragma once

#include <yt/yql/providers/ytflow/provider/yql_ytflow_provider_impl.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>

#include <initializer_list>


namespace NYql::NYtflow::NTest {

class TPhysicalFinalizingSetup {
public:
    TPhysicalFinalizingSetup();
    virtual ~TPhysicalFinalizingSetup() = default;

    TExprNode::TPtr NewWorld();
    TExprNode::TPtr MakeReadWrap();
    TExprNode::TPtr MakeSync(std::initializer_list<TExprNode::TPtr> inputs);
    NNodes::TCoNameValueTupleList MakeSettings(TStringBuf name = {}, TStringBuf value = {});
    TExprNode::TPtr MakeSourceMap(
        TExprNode::TPtr readWrap,
        TExprNode::TPtr world,
        NNodes::TCoNameValueTupleList settings,
        ui32 outputCount = 1);
    TExprNode::TPtr MakeOutput(TExprNode::TPtr operation, ui32 outputIndex = 0);
    TExprNode::TPtr MakeLeft(TExprNode::TPtr operation);
    TExprNode::TPtr MakePersistentSink(TExprNode::TPtr world, TExprNode::TPtr output);
    TExprNode::TPtr MakeMap(
        TExprNode::TPtr source,
        TExprNode::TPtr world,
        ui32 outputCount = 1);
    TExprNode::TPtr MakeExtend(
        std::initializer_list<TExprNode::TPtr> sources,
        TExprNode::TPtr world);
    TExprNode::TPtr MakeRoot(TExprNode::TListType children);

    void AnnotateTypes(TExprNode::TPtr& root);
    virtual void Transform(TExprNode::TPtr& root);

    size_t CountSourceMaps(const TExprNode::TPtr& root) const;
    size_t CountMaps(const TExprNode::TPtr& root) const;
    NNodes::TYtflowSourceMap GetSourceMap(const TExprNode::TPtr& root) const;
    NNodes::TYtflowExtend GetExtend(const TExprNode::TPtr& root) const;

protected:
    TExprContext Ctx_;
    TTypeAnnotationContext Types_;
    TYtflowState::TPtr State_;
    TPositionHandle Position_;

private:
    NNodes::TCoLambda MakePassthroughLambda(ui32 outputCount);
};

} // namespace NYql::NYtflow::NTest
