#include <yt/yql/providers/yt/provider/yql_yt_provider_impl.h>
#include <yt/yql/providers/yt/provider/yql_yt_table.h>

#include <yql/essentials/ast/yql_constraint.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(SymlinkConstraints) {
    Y_UNIT_TEST(PreserveTargetConstraints) {
        TExprContext ctx;
        const TPositionHandle pos;
        auto types = MakeIntrusive<TTypeAnnotationContext>();
        auto state = std::make_shared<TYtState>(types.Get());

        TYtTableInfo targetInfo;
        targetInfo.Name = "Target";
        targetInfo.Cluster = "plato";
        auto target = targetInfo.ToExprNode(ctx, pos).Ptr();
        const auto rowType = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
            ctx.MakeType<TItemExprType>("key", ctx.MakeType<TDataExprType>(EDataSlot::String))});
        const auto tableType = ctx.MakeType<TListExprType>(rowType);
        target->SetTypeAnn(tableType);
        const auto sorted = ctx.MakeConstraint<TSortedConstraintNode>(
            TSortedConstraintNode::TContainerType{{{{"key"}}, true}});
        const std::vector<std::string_view> keys{"key"};
        const auto unique = ctx.MakeConstraint<TUniqueConstraintNode>(keys);
        const auto distinct = ctx.MakeConstraint<TDistinctConstraintNode>(keys);
        target->AddConstraint(sorted);
        target->AddConstraint(unique);
        target->AddConstraint(distinct);

        TYtTableInfo linkInfo;
        linkInfo.Name = "Link";
        linkInfo.Cluster = "plato";
        linkInfo.CommitEpoch = 1;
        auto link = linkInfo.ToExprNode(ctx, pos).Ptr();
        auto& next = state->TablesData->GetOrAddTable("plato", "Link", 1);
        next.IsReplaced = true;

        auto sink = ctx.NewCallable(pos, "DataSink", {ctx.NewAtom(pos, "yt"), ctx.NewAtom(pos, "plato")});
        auto settings = ctx.NewList(pos, {
            ctx.NewList(pos, {ctx.NewAtom(pos, "initial")}),
            ctx.NewList(pos, {ctx.NewAtom(pos, "mode"), ctx.NewAtom(pos, "create_symlink")})});
        auto create = ctx.NewCallable(pos, "YtCreateSymlink!", {ctx.NewWorld(pos), sink, link, target, settings});
        auto transformer = CreateYtDataSinkConstraintTransformer(state, false);
        TExprNode::TPtr output;
        UNIT_ASSERT(transformer->Transform(create, output, ctx) == IGraphTransformer::TStatus::Ok);
        UNIT_ASSERT(!next.ConstraintsReady);

        auto commit = ctx.NewCallable(pos, "Commit!", {create, sink,
            ctx.NewList(pos, {ctx.NewList(pos, {ctx.NewAtom(pos, "epoch"), ctx.NewAtom(pos, "1")})})});
        UNIT_ASSERT(transformer->Transform(commit, output, ctx) == IGraphTransformer::TStatus::Ok);
        UNIT_ASSERT(next.ConstraintsReady);

        linkInfo.Epoch = 1;
        linkInfo.CommitEpoch.Clear();
        auto readLink = linkInfo.ToExprNode(ctx, pos).Ptr();
        readLink->SetTypeAnn(tableType);
        auto sourceTransformer = CreateYtDataSourceConstraintTransformer(state);
        UNIT_ASSERT(sourceTransformer->Transform(readLink, output, ctx) == IGraphTransformer::TStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(readLink->GetConstraint<TSortedConstraintNode>(), sorted);
        UNIT_ASSERT_VALUES_EQUAL(readLink->GetConstraint<TUniqueConstraintNode>(), unique);
        UNIT_ASSERT_VALUES_EQUAL(readLink->GetConstraint<TDistinctConstraintNode>(), distinct);
    }
}

} // namespace NYql
