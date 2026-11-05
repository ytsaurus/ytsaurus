#include "yql_ytflow_physical_finalizing_setup.h"

#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_integration.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>

#include <library/cpp/testing/gtest/gtest.h>


namespace NYql::NYtflow::NTest {
namespace {

using namespace NNodes;

class TLookupJoinIntegration
    : public TEmptyYtflowIntegration
{
public:
    TMaybe<bool> CanLookupRead(
        const TExprNode& read,
        const TVector<TStringBuf>& /*keys*/,
        ERowSelectionMode /*rowSelectionMode*/,
        TExprContext& /*ctx*/) override
    {
        if (read.IsCallable("LookupRead!")) {
            return true;
        }
        return Nothing();
    }

    TExprNode::TPtr GetReadWorld(const TExprNode& read, TExprContext& /*ctx*/) override
    {
        return read.ChildPtr(0);
    }
};

class TLookupJoinDataSource
    : public TDataProviderBase
{
public:
    TStringBuf GetName() const override
    {
        return "test";
    }

    IYtflowIntegration* GetYtflowIntegration() override
    {
        return &Integration_;
    }

private:
    TLookupJoinIntegration Integration_;
};

class TLookupJoinSetup
    : public TPhysicalFinalizingSetup
{
public:
    TLookupJoinSetup()
    {
        Types_.AddDataSource("test", MakeIntrusive<TLookupJoinDataSource>());
    }

    void Optimize(TExprNode::TPtr& root)
    {
        auto transformer = CreateYtflowPhysicalOptProposalTransformer(State_);
        const auto status = SyncTransform(*transformer, root, Ctx_);
        ASSERT_TRUE(status == IGraphTransformer::TStatus::Ok)
            << Ctx_.IssueManager.GetIssues().ToString();
    }

    TExprNode::TPtr MakeReadWrap(TStringBuf callable, TExprNode::TPtr world)
    {
        auto read = Ctx_.NewCallable(Position_, callable, {
            std::move(world),
            Build<TCoDataSource>(Ctx_, Position_)
                .Category().Value("test").Build()
                .Done().Ptr(),
        });
        auto readWrap = Build<TYtflowReadWrap>(Ctx_, Position_)
            .Input(std::move(read))
            .Done().Ptr();
        const auto* rowType = Ctx_.MakeType<TStructExprType>(TVector<const TItemExprType*>{
            Ctx_.MakeType<TItemExprType>("key", Ctx_.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64)),
            Ctx_.MakeType<TItemExprType>("value", Ctx_.MakeType<TDataExprType>(NUdf::EDataSlot::String)),
        });
        readWrap->SetTypeAnn(Ctx_.MakeType<TListExprType>(rowType));
        return readWrap;
    }

    TExprNode::TPtr MakeJoinTuple(
        TStringBuf kind,
        TStringBuf leftLabel,
        TStringBuf rightLabel,
        TExprNode::TPtr leftScope = {})
    {
        if (!leftScope) {
            leftScope = Ctx_.NewAtom(Position_, leftLabel);
        }
        return Ctx_.NewList(Position_, {
            Ctx_.NewAtom(Position_, kind),
            std::move(leftScope),
            Ctx_.NewAtom(Position_, rightLabel),
            MakeAtomList(Position_, TVector<TStringBuf>{leftLabel, "key"}, Ctx_),
            MakeAtomList(Position_, TVector<TStringBuf>{rightLabel, "key"}, Ctx_),
            Ctx_.NewList(Position_, {}),
        });
    }

    TExprNode::TPtr MakeOuterJoin(
        std::initializer_list<std::pair<TStringBuf, TExprNode::TPtr>> inputs,
        TExprNode::TPtr joinTuple)
    {
        TExprNode::TListType children;
        TVector<const TItemExprType*> items;
        for (const auto& [label, input] : inputs) {
            children.push_back(Build<TCoEquiJoinInput>(Ctx_, Position_)
                .List(input)
                .Scope<TCoAtom>().Value(label).Build()
                .Done().Ptr());
            const auto* rowType = input->GetTypeAnn()->Cast<TListExprType>()
                ->GetItemType()->Cast<TStructExprType>();
            for (const auto* item : rowType->GetItems()) {
                const auto* itemType = item->GetItemType();
                if (label != "stream") {
                    itemType = Ctx_.MakeType<TOptionalExprType>(itemType);
                }
                items.push_back(Ctx_.MakeType<TItemExprType>(
                    TString::Join(label, '.', item->GetName()),
                    itemType));
            }
        }
        children.push_back(std::move(joinTuple));
        children.push_back(Ctx_.NewList(Position_, {}));
        auto join = Ctx_.NewCallable(Position_, TCoEquiJoin::CallableName(), std::move(children));
        join->SetTypeAnn(Ctx_.MakeType<TListExprType>(Ctx_.MakeType<TStructExprType>(items)));
        return join;
    }

    void ExpectIdentitySourceMap(TExprBase source, const TExprNode::TPtr& readWrap)
    {
        ASSERT_TRUE(source.Maybe<TYtflowOutput>());
        auto output = source.Cast<TYtflowOutput>();
        EXPECT_EQ(output.OutputIndex().Value(), "0");
        ASSERT_TRUE(output.Operation().Maybe<TYtflowSourceMap>());
        auto sourceMap = output.Operation().Cast<TYtflowSourceMap>();
        ASSERT_EQ(sourceMap.Sources().Size(), 1u);
        ASSERT_TRUE(sourceMap.Sources().Item(0).Maybe<TYtflowPersistentSource>());
        EXPECT_EQ(sourceMap.Sources().Item(0).Cast<TYtflowPersistentSource>().Input().Ptr(), readWrap);
        EXPECT_EQ(sourceMap.Settings().Size(), 0u);
        ASSERT_EQ(sourceMap.Lambda().Args().Size(), 1u);
        EXPECT_EQ(sourceMap.Lambda().Body().Ptr(), sourceMap.Lambda().Args().Arg(0).Ptr());

        ExpectWorldDependency(sourceMap.World(), TYtflowReadWrap(readWrap).Input().Ref().ChildPtr(0));
    }

    void ExpectWorldDependency(TExprBase world, const TExprNode::TPtr& dependency)
    {
        EXPECT_TRUE(FindNode(world.Ptr(), [&] (const TExprNode::TPtr& node) {
            return node == dependency;
        }));
    }
};

TVector<TYtflowTransformMap> FindTransformMaps(const TExprNode::TPtr& root)
{
    TVector<TYtflowTransformMap> maps;
    VisitExpr(root, [&] (const TExprNode::TPtr& node) {
        if (auto map = TMaybeNode<TYtflowTransformMap>(node)) {
            maps.push_back(map.Cast());
        }
        return true;
    });
    return maps;
}

void ExpectLookupSource(
    TYtflowTransformMap map,
    const TExprNode::TPtr& lookup,
    TStringBuf streamSide)
{
    ASSERT_TRUE(map.Lambda().Body().Maybe<TYtflowLookupJoin>());
    auto join = map.Lambda().Body().Cast<TYtflowLookupJoin>();
    EXPECT_EQ(join.LookupSource().Ptr(), lookup);
    EXPECT_EQ(join.StreamScope().Side().Value(), streamSide);
    ASSERT_EQ(map.GroupByColumns().Size(), 1u);
    EXPECT_EQ(map.GroupByColumns().Item(0).Value(), "key");
}

} // namespace

TEST(TPhysicalOptimizeLookupJoin, AddsSourceMapForRawLeftStream)
{
    TLookupJoinSetup setup;
    auto streamWorld = setup.NewWorld();
    auto lookupWorld = setup.NewWorld();
    auto stream = setup.MakeReadWrap("StreamRead!", streamWorld);
    auto lookup = setup.MakeReadWrap("LookupRead!", lookupWorld);
    auto root = setup.MakeOuterJoin(
        {{"stream", stream}, {"lookup", lookup}},
        setup.MakeJoinTuple("Left", "stream", "lookup"));

    auto originalJoin = root;
    auto originalStreamInput = originalJoin->ChildPtr(0);
    auto originalLookupInput = originalJoin->ChildPtr(1);

    ASSERT_NO_FATAL_FAILURE(setup.Optimize(root));

    EXPECT_EQ(originalJoin->ChildPtr(0), originalStreamInput);
    EXPECT_EQ(originalJoin->ChildPtr(1), originalLookupInput);
    EXPECT_EQ(TCoEquiJoinInput(originalStreamInput).List().Ptr(), stream);
    EXPECT_EQ(TCoEquiJoinInput(originalLookupInput).List().Ptr(), lookup);

    auto maps = FindTransformMaps(root);
    ASSERT_EQ(maps.size(), 1u);
    auto map = maps.front();
    ASSERT_EQ(map.Sources().Size(), 1u);
    setup.ExpectIdentitySourceMap(map.Sources().Item(0), stream);
    ExpectLookupSource(map, lookup, "left");
    setup.ExpectWorldDependency(map.World(), streamWorld);
    setup.ExpectWorldDependency(map.World(), lookupWorld);
    EXPECT_EQ(setup.CountSourceMaps(root), 1u);
}

TEST(TPhysicalOptimizeLookupJoin, AddsSourceMapForRawRightStream)
{
    TLookupJoinSetup setup;
    auto stream = setup.MakeReadWrap("StreamRead!", setup.NewWorld());
    auto lookup = setup.MakeReadWrap("LookupRead!", setup.NewWorld());
    auto root = setup.MakeOuterJoin(
        {{"lookup", lookup}, {"stream", stream}},
        setup.MakeJoinTuple("Right", "lookup", "stream"));

    ASSERT_NO_FATAL_FAILURE(setup.Optimize(root));

    auto maps = FindTransformMaps(root);
    ASSERT_EQ(maps.size(), 1u);
    auto map = maps.front();
    ASSERT_EQ(map.Sources().Size(), 1u);
    setup.ExpectIdentitySourceMap(map.Sources().Item(0), stream);
    ExpectLookupSource(map, lookup, "right");
    EXPECT_EQ(setup.CountSourceMaps(root), 1u);
}

TEST(TPhysicalOptimizeLookupJoin, KeepsExistingIntermediateStream)
{
    TLookupJoinSetup setup;
    auto streamWorld = setup.NewWorld();
    auto read = setup.MakeReadWrap("StreamRead!", streamWorld);
    auto sourceMap = setup.MakeSourceMap(read, streamWorld, setup.MakeSettings());
    auto stream = setup.MakeOutput(sourceMap);
    stream->SetTypeAnn(read->GetTypeAnn());
    auto lookup = setup.MakeReadWrap("LookupRead!", setup.NewWorld());
    auto root = setup.MakeOuterJoin(
        {{"stream", stream}, {"lookup", lookup}},
        setup.MakeJoinTuple("Left", "stream", "lookup"));

    ASSERT_NO_FATAL_FAILURE(setup.Optimize(root));

    auto maps = FindTransformMaps(root);
    ASSERT_EQ(maps.size(), 1u);
    auto map = maps.front();
    ASSERT_EQ(map.Sources().Size(), 1u);
    EXPECT_EQ(map.Sources().Item(0).Ptr(), stream);
    ExpectLookupSource(map, lookup, "left");
    EXPECT_EQ(setup.CountSourceMaps(root), 1u);
}

TEST(TPhysicalOptimizeLookupJoin, AddsSingleSourceMapForChainedLookupJoins)
{
    TLookupJoinSetup setup;
    auto streamWorld = setup.NewWorld();
    auto firstLookupWorld = setup.NewWorld();
    auto secondLookupWorld = setup.NewWorld();
    auto stream = setup.MakeReadWrap("StreamRead!", streamWorld);
    auto firstLookup = setup.MakeReadWrap("LookupRead!", firstLookupWorld);
    auto secondLookup = setup.MakeReadWrap("LookupRead!", secondLookupWorld);
    auto joinTuple = setup.MakeJoinTuple("Left", "stream", "first_lookup");
    auto root = setup.MakeOuterJoin(
        {{"stream", stream}, {"first_lookup", firstLookup}, {"second_lookup", secondLookup}},
        setup.MakeJoinTuple("Left", "stream", "second_lookup", joinTuple));

    ASSERT_NO_FATAL_FAILURE(setup.Optimize(root));

    auto maps = FindTransformMaps(root);
    ASSERT_EQ(maps.size(), 2u);
    EXPECT_EQ(setup.CountSourceMaps(root), 1u);
    bool foundFirstLookup = false;
    bool foundSecondLookup = false;
    for (const auto& map : maps) {
        ASSERT_EQ(map.Sources().Size(), 1u);
        auto source = map.Sources().Item(0);
        ASSERT_TRUE(source.Maybe<TYtflowOutput>());
        ASSERT_TRUE(map.Lambda().Body().Maybe<TYtflowLookupJoin>());
        auto lookupJoin = map.Lambda().Body().Cast<TYtflowLookupJoin>();
        if (lookupJoin.LookupSource().Ptr() == firstLookup) {
            EXPECT_FALSE(foundFirstLookup);
            foundFirstLookup = true;
            setup.ExpectIdentitySourceMap(source, stream);
            ExpectLookupSource(map, firstLookup, "left");
        } else {
            EXPECT_EQ(lookupJoin.LookupSource().Ptr(), secondLookup);
            EXPECT_FALSE(foundSecondLookup);
            foundSecondLookup = true;
            auto upstream = source.Cast<TYtflowOutput>().Operation();
            ASSERT_TRUE(upstream.Maybe<TYtflowTransformMap>());
            ExpectLookupSource(upstream.Cast<TYtflowTransformMap>(), firstLookup, "left");
            ASSERT_EQ(map.GroupByColumns().Size(), 1u);
            EXPECT_EQ(map.GroupByColumns().Item(0).Value(), "stream.key");
            setup.ExpectWorldDependency(map.World(), secondLookupWorld);
        }
        setup.ExpectWorldDependency(map.World(), streamWorld);
        setup.ExpectWorldDependency(map.World(), firstLookupWorld);
    }
    EXPECT_TRUE(foundFirstLookup);
    EXPECT_TRUE(foundSecondLookup);
}

} // namespace NYql::NYtflow::NTest
