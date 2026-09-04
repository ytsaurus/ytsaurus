#include <yt/yql/providers/ytflow/provider/yql_ytflow_constants.h>
#include <yt/yql/providers/ytflow/provider/yql_ytflow_swift_map.h>

#include <yql/essentials/core/yql_opt_utils.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>

#include <library/cpp/testing/gtest/gtest.h>


namespace NYql::NYtflow::NPrivate {
namespace {

using namespace NNodes;

TExprNode::TPtr MakeLambda(TExprContext& ctx, TExprNode::TPtr body = {})
{
    TPositionHandle position;
    if (!body) {
        body = ctx.NewAtom(position, "body");
    }
    return ctx.NewLambda(
        position,
        ctx.NewArguments(position, TExprNode::TListType{}),
        std::move(body));
}

TExprNode::TPtr MakeExtend(
    TExprContext& ctx,
    TStringBuf sourceName = TYtflowOutput::CallableName(),
    TStringBuf sinkName = TYtflowIntermediateSink::CallableName(),
    TExprNode::TPtr lambda = {})
{
    TPositionHandle position;
    auto emptyList = ctx.NewList(position, TExprNode::TListType{});
    if (!lambda) {
        lambda = MakeLambda(ctx);
    }
    return ctx.NewCallable(
        position,
        TYtflowExtend::CallableName(),
        {
            ctx.NewWorld(position),
            ctx.NewList(position, {ctx.NewCallable(position, sourceName, {})}),
            ctx.NewList(position, {ctx.NewCallable(position, sinkName, {})}),
            emptyList,
            std::move(lambda),
            emptyList,
        });
}

bool HasExtendSetting(const TExprNode& operation)
{
    return HasSetting(
        *operation.Child(TYtflowMapBase::idx_Settings),
        EXTEND_SETTING);
}

} // anonymous namespace

TEST(TSwiftMapSelection, SelectsSwiftMapForDeterministicExtend)
{
    TExprContext ctx;
    auto operation = MakeExtend(ctx);
    const auto* original = operation.Get();

    operation = SelectExtendImplementation(operation, false, ctx);

    ASSERT_TRUE(TYtflowSwiftMap::Match(operation.Get()));
    ASSERT_TRUE(HasExtendSetting(*operation));
    ASSERT_NE(original, operation.Get());
}

TEST(TSwiftMapSelection, SelectsTransformMapForNonDeterministicPeephole)
{
    TExprContext ctx;
    auto operation = MakeExtend(ctx);
    const auto* original = operation.Get();

    operation = SelectExtendImplementation(operation, true, ctx);

    ASSERT_TRUE(TYtflowTransformMap::Match(operation.Get()));
    ASSERT_TRUE(HasExtendSetting(*operation));
    ASSERT_NE(original, operation.Get());
}

TEST(TSwiftMapSelection, KeepsSelectedSwiftMapWhenStillEligible)
{
    TExprContext ctx;
    auto operation = SelectExtendImplementation(MakeExtend(ctx), false, ctx);
    const auto* selected = operation.Get();

    operation = SelectExtendImplementation(operation, false, ctx);

    ASSERT_TRUE(TYtflowSwiftMap::Match(operation.Get()));
    ASSERT_EQ(selected, operation.Get());
}

TEST(TSwiftMapSelection, KeepsSelectedTransformMapWhenStillIneligible)
{
    TExprContext ctx;
    auto operation = SelectExtendImplementation(MakeExtend(ctx), true, ctx);
    const auto* selected = operation.Get();

    operation = SelectExtendImplementation(operation, true, ctx);

    ASSERT_TRUE(TYtflowTransformMap::Match(operation.Get()));
    ASSERT_EQ(selected, operation.Get());
}

TEST(TSwiftMapSelection, SelectsTransformMapWhenSwiftMapIsNoLongerEligible)
{
    TExprContext ctx;
    auto operation = SelectExtendImplementation(MakeExtend(ctx), false, ctx);

    operation = SelectExtendImplementation(operation, true, ctx);

    ASSERT_TRUE(TYtflowTransformMap::Match(operation.Get()));
    ASSERT_TRUE(HasExtendSetting(*operation));
}

TEST(TSwiftMapSelection, SelectsSwiftMapWhenTransformMapBecomesEligible)
{
    TExprContext ctx;
    auto operation = SelectExtendImplementation(MakeExtend(ctx), true, ctx);

    operation = SelectExtendImplementation(operation, false, ctx);

    ASSERT_TRUE(TYtflowSwiftMap::Match(operation.Get()));
    ASSERT_TRUE(HasExtendSetting(*operation));
}

TEST(TSwiftMapSelection, SelectsTransformMapForNonDeterministicExpression)
{
    TExprContext ctx;
    TPositionHandle position;
    auto random = ctx.NewCallable(
        position,
        TCoRandomNumber::CallableName(),
        {ctx.NewAtom(position, "dependency")});
    auto operation = MakeExtend(
        ctx,
        TYtflowOutput::CallableName(),
        TYtflowIntermediateSink::CallableName(),
        MakeLambda(ctx, std::move(random)));

    operation = SelectExtendImplementation(operation, false, ctx);

    ASSERT_TRUE(TYtflowTransformMap::Match(operation.Get()));
    ASSERT_TRUE(HasExtendSetting(*operation));
}

TEST(TSwiftMapSelection, SelectsTransformMapForSideEffects)
{
    for (const auto sideEffects : {ESideEffects::SemilatticeRT, ESideEffects::General}) {
        TExprContext ctx;
        auto lambda = MakeLambda(ctx);
        lambda->SetSideEffects(sideEffects);
        auto operation = MakeExtend(
            ctx,
            TYtflowOutput::CallableName(),
            TYtflowIntermediateSink::CallableName(),
            std::move(lambda));

        operation = SelectExtendImplementation(operation, false, ctx);

        ASSERT_TRUE(TYtflowTransformMap::Match(operation.Get()));
        ASSERT_TRUE(HasExtendSetting(*operation));
    }
}

TEST(TSwiftMapSelection, SelectsTransformMapForDependsOn)
{
    TExprContext ctx;
    TPositionHandle position;
    auto dependsOn = ctx.NewCallable(
        position,
        TCoDependsOn::CallableName(),
        {ctx.NewAtom(position, "value")});
    auto operation = MakeExtend(
        ctx,
        TYtflowOutput::CallableName(),
        TYtflowIntermediateSink::CallableName(),
        MakeLambda(ctx, std::move(dependsOn)));

    operation = SelectExtendImplementation(operation, false, ctx);

    ASSERT_TRUE(TYtflowTransformMap::Match(operation.Get()));
    ASSERT_TRUE(HasExtendSetting(*operation));
}

TEST(TSwiftMapSelection, SelectsTransformMapForNestedInnerDependsOn)
{
    TExprContext ctx;
    TPositionHandle position;
    auto innerDependsOn = ctx.NewCallable(
        position,
        TCoInnerDependsOn::CallableName(),
        {ctx.NewAtom(position, "value")});
    auto body = ctx.NewCallable(
        position,
        "Outer",
        {std::move(innerDependsOn)});
    auto operation = MakeExtend(
        ctx,
        TYtflowOutput::CallableName(),
        TYtflowIntermediateSink::CallableName(),
        MakeLambda(ctx, std::move(body)));

    operation = SelectExtendImplementation(operation, false, ctx);

    ASSERT_TRUE(TYtflowTransformMap::Match(operation.Get()));
    ASSERT_TRUE(HasExtendSetting(*operation));
}

TEST(TSwiftMapSelection, SelectsTransformMapForPersistentSource)
{
    TExprContext ctx;
    auto operation = MakeExtend(ctx, TYtflowPersistentSource::CallableName());

    operation = SelectExtendImplementation(operation, false, ctx);

    ASSERT_TRUE(TYtflowTransformMap::Match(operation.Get()));
    ASSERT_TRUE(HasExtendSetting(*operation));
}

TEST(TSwiftMapSelection, SelectsTransformMapForPersistentSink)
{
    TExprContext ctx;
    auto operation = MakeExtend(
        ctx,
        TYtflowOutput::CallableName(),
        TYtflowPersistentSink::CallableName());

    operation = SelectExtendImplementation(operation, false, ctx);

    ASSERT_TRUE(TYtflowTransformMap::Match(operation.Get()));
    ASSERT_TRUE(HasExtendSetting(*operation));
}

TEST(TSwiftMapSelection, DoesNotChangeUnmarkedTransformMap)
{
    TExprContext ctx;
    auto operation = ctx.RenameNode(
        *MakeExtend(ctx),
        TYtflowTransformMap::CallableName());

    auto updated = SelectExtendImplementation(operation, false, ctx);

    ASSERT_EQ(operation.Get(), updated.Get());
    ASSERT_FALSE(HasExtendSetting(*updated));
}

TEST(TSwiftMapSelection, PreservesExistingSettingsWhenSelectingImplementation)
{
    TExprContext ctx;
    auto operation = MakeExtend(ctx);
    auto settings = AddSetting(
        *operation->Child(TYtflowMapBase::idx_Settings),
        operation->Pos(),
        TString(INJECT_INPUT_MESSAGE_ID_SETTING),
        nullptr,
        ctx);
    operation = ctx.ChangeChild(
        *operation,
        TYtflowMapBase::idx_Settings,
        std::move(settings));

    operation = SelectExtendImplementation(operation, false, ctx);

    ASSERT_TRUE(HasExtendSetting(*operation));
    ASSERT_TRUE(HasSetting(
        *operation->Child(TYtflowMapBase::idx_Settings),
        INJECT_INPUT_MESSAGE_ID_SETTING));
}

TEST(TSwiftMapSelection, DoesNotChangeOtherOperations)
{
    TExprContext ctx;
    TPositionHandle position;
    auto operation = ctx.NewCallable(
        position,
        "OtherOperation",
        TExprNode::TListType{});

    auto updated = SelectExtendImplementation(operation, false, ctx);

    ASSERT_EQ(operation.Get(), updated.Get());
}

} // namespace NYql::NYtflow::NPrivate
