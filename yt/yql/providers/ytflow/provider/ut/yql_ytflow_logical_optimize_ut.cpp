#include "yql_ytflow_physical_finalizing_setup.h"

#include <yt/yql/providers/ytflow/provider/yql_ytflow_constants.h>

#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <library/cpp/testing/gtest/gtest.h>


namespace NYql::NYtflow::NTest {

namespace {

using namespace NNodes;

class TLogicalOptimizeSetup : public TPhysicalFinalizingSetup {
public:
    void Optimize(TExprNode::TPtr& root)
    {
        auto transformer = CreateYtflowLogicalOptProposalTransformer(State_);
        const auto status = SyncTransform(*transformer, root, Ctx_);
        ASSERT_TRUE(status == IGraphTransformer::TStatus::Ok)
            << Ctx_.IssueManager.GetIssues().ToString();
    }

    TExprNode::TPtr MakeTransformMap(
        TExprNode::TPtr source,
        TExprNode::TPtr world,
        std::initializer_list<TStringBuf> groupByColumns,
        std::initializer_list<ui32> outputIndices = {0})
    {
        auto map = MakeMap(std::move(source), std::move(world), outputIndices);
        auto children = map->ChildrenList();
        children.push_back(MakeAtomList(
            Position_,
            TVector<TStringBuf>(groupByColumns),
            Ctx_));

        return Ctx_.NewCallable(
            Position_,
            TYtflowTransformMap::CallableName(),
            std::move(children));
    }

    TExprNode::TPtr MakeLeft(const TExprNode::TPtr& operation)
    {
        return Build<TCoLeft>(Ctx_, Position_)
            .Input(operation)
            .Done().Ptr();
    }

    void AddRoot(TExprNode::TPtr& root, TExprNode::TPtr node)
    {
        auto children = root->ChildrenList();
        children.push_back(std::move(node));
        root = Ctx_.NewCallable(Position_, "TestRoot", std::move(children));
    }

    TExprNode::TPtr MakeIntermediateSink(ui32 outputIndex)
    {
        return Build<TYtflowIntermediateSink>(Ctx_, Position_)
            .Name()
                .Value("")
                .Build()
            .OutputIndex()
                .Value(outputIndex)
                .Build()
            .RowType<TCoVoid>()
                .Build()
            .Done().Ptr();
    }

protected:
    void SetChild(TExprNode::TPtr& node, ui32 index, TExprNode::TPtr child)
    {
        node = Ctx_.ChangeChild(*node, index, std::move(child));
    }

    void SetStreamStructType(const TExprNode::TPtr& lambda)
    {
        const auto* rowType = Ctx_.MakeType<TStructExprType>(TVector<const TItemExprType*>{
            Ctx_.MakeType<TItemExprType>(
                "value",
                Ctx_.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64)),
        });

        lambda->SetTypeAnn(Ctx_.MakeType<TStreamExprType>(rowType));
    }
};

struct TTransformMapOverSourceMapGraph {
    TExprNode::TPtr SourceMap;
    TExprNode::TPtr SourceOutput;
    TExprNode::TPtr TransformMap;
    TExprNode::TPtr Root;
};

class TTransformMapOverSourceMapSetup : public TLogicalOptimizeSetup {
public:
    TTransformMapOverSourceMapGraph MakeTransformMapOverSourceMapGraph()
    {
        TTransformMapOverSourceMapGraph graph;
        graph.SourceMap = MakeSourceMap(
            MakeReadWrap(),
            NewWorld(),
            MakeSettings(INJECT_INPUT_MESSAGE_ID_SETTING));

        SetStreamStructType(graph.SourceMap->ChildPtr(TYtflowMapBase::idx_Lambda));

        graph.SourceOutput = MakeOutput(graph.SourceMap);
        graph.TransformMap = MakeTransformMap(
            graph.SourceOutput,
            NewWorld(),
            {YTFLOW_INPUT_MESSAGE_ID_FIELD});

        graph.Root = MakeRoot({graph.TransformMap});

        return graph;
    }

    void OptimizeAndExpectNotFused(TTransformMapOverSourceMapGraph& graph)
    {
        Optimize(graph.Root);

        EXPECT_TRUE(TYtflowOutput(graph.Root->Child(0))
            .Operation().Maybe<TYtflowTransformMap>());
    }

    TYtflowTransformSourceMap OptimizeAndGetFusedMap(TTransformMapOverSourceMapGraph& graph)
    {
        Optimize(graph.Root);

        return TYtflowOutput(graph.Root->Child(0))
            .Operation().Cast<TYtflowTransformSourceMap>();
    }

    void SetSourceMapLambda(TTransformMapOverSourceMapGraph& graph, TCoLambda lambda)
    {
        SetChild(graph.SourceMap, TYtflowMapBase::idx_Lambda, lambda.Ptr());
        SetStreamStructType(graph.SourceMap->ChildPtr(TYtflowMapBase::idx_Lambda));
        ReconnectSourceMap(graph);
    }

    void SetTransformMapLambda(TTransformMapOverSourceMapGraph& graph, TCoLambda lambda)
    {
        SetChild(graph.TransformMap, TYtflowMapBase::idx_Lambda, lambda.Ptr());
        ReconnectTransformMap(graph);
    }

    void SetSourceMapSettings(
        TTransformMapOverSourceMapGraph& graph,
        TCoNameValueTupleList settings)
    {
        SetChild(graph.SourceMap, TYtflowMapBase::idx_Settings, settings.Ptr());
        ReconnectSourceMap(graph);
    }

    void SetTransformMapSettings(
        TTransformMapOverSourceMapGraph& graph,
        TCoNameValueTupleList settings)
    {
        SetChild(graph.TransformMap, TYtflowMapBase::idx_Settings, settings.Ptr());
        ReconnectTransformMap(graph);
    }

    void AddSourceMapSetting(TTransformMapOverSourceMapGraph& graph, TStringBuf setting)
    {
        auto settings = AddSetting(
            *graph.SourceMap->Child(TYtflowMapBase::idx_Settings),
            Position_,
            TString(setting),
            Ctx_.NewAtom(Position_, ""),
            Ctx_);

        SetChild(graph.SourceMap, TYtflowMapBase::idx_Settings, std::move(settings));
        ReconnectSourceMap(graph);
    }

    void SetGroupByColumns(
        TTransformMapOverSourceMapGraph& graph,
        std::initializer_list<TStringBuf> columns)
    {
        SetChild(
            graph.TransformMap,
            TYtflowTransformMap::idx_GroupByColumns,
            MakeAtomList(Position_, TVector<TStringBuf>(columns), Ctx_));

        ReconnectTransformMap(graph);
    }

    void UseMultipleTransformSources(TTransformMapOverSourceMapGraph& graph)
    {
        SetChild(
            graph.TransformMap,
            TYtflowMapBase::idx_Sources,
            Ctx_.NewList(Position_, {graph.SourceOutput, graph.SourceOutput}));

        ReconnectTransformMap(graph);
    }

    void UseMultipleSourceMapSinks(TTransformMapOverSourceMapGraph& graph)
    {
        auto sinks = graph.SourceMap->Child(TYtflowMapBase::idx_Sinks)->ChildrenList();
        sinks.push_back(MakeIntermediateSink(0));

        SetChild(
            graph.SourceMap,
            TYtflowMapBase::idx_Sinks,
            Ctx_.NewList(Position_, std::move(sinks)));

        ReconnectSourceMap(graph);
    }

    void UseSourceMapOutput(TTransformMapOverSourceMapGraph& graph, ui32 index)
    {
        graph.SourceOutput = MakeOutput(graph.SourceMap, index);

        SetChild(
            graph.TransformMap,
            TYtflowMapBase::idx_Sources,
            Ctx_.NewList(Position_, {graph.SourceOutput}));

        ReconnectTransformMap(graph);
    }

    void UseDistinctWorlds(
        TTransformMapOverSourceMapGraph& graph,
        TExprNode::TPtr sourceWorld,
        TExprNode::TPtr transformWorld)
    {
        SetChild(graph.SourceMap, TYtflowMapBase::idx_World, std::move(sourceWorld));

        graph.SourceOutput = MakeOutput(graph.SourceMap);

        SetChild(
            graph.TransformMap,
            TYtflowMapBase::idx_World,
            std::move(transformWorld));

        SetChild(
            graph.TransformMap,
            TYtflowMapBase::idx_Sources,
            Ctx_.NewList(Position_, {graph.SourceOutput}));

        graph.Root = MakeRoot({graph.TransformMap});
    }

    void UseReorderedTransformOutputs(TTransformMapOverSourceMapGraph& graph)
    {
        graph.TransformMap = MakeTransformMap(
            graph.SourceOutput,
            NewWorld(),
            {YTFLOW_INPUT_MESSAGE_ID_FIELD},
            {0, 1});

        graph.Root = MakeRootFromOutputs({
            MakeOutput(graph.TransformMap, 1),
            MakeOutput(graph.TransformMap, 0),
        });
    }

    void AddSourceMapOutputUsage(TTransformMapOverSourceMapGraph& graph)
    {
        AddRoot(graph.Root, graph.SourceOutput);
    }

    void AddSourceMapWorldUsage(TTransformMapOverSourceMapGraph& graph)
    {
        AddRoot(graph.Root, MakeLeft(graph.SourceMap));
    }

    void UseVariantSourceMapOutput(TTransformMapOverSourceMapGraph& graph)
    {
        const auto* rowType = Ctx_.MakeType<TStructExprType>(
            TVector<const TItemExprType*>{});

        const auto* variantType = Ctx_.MakeType<TVariantExprType>(
            Ctx_.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{rowType}));

        graph.SourceMap->ChildPtr(TYtflowMapBase::idx_Lambda)->SetTypeAnn(
            Ctx_.MakeType<TStreamExprType>(variantType));
    }

    TCoLambda MakeSkipLambda()
    {
        return Build<TCoLambda>(Ctx_, Position_)
            .Args({"stream"})
            .Body<TCoSkip>()
                .Input("stream")
                .Count<TCoUint64>()
                    .Literal()
                        .Value("1")
                        .Build()
                    .Build()
                .Build()
            .Done();
    }

    TCoLambda MakeTakeLambda()
    {
        return Build<TCoLambda>(Ctx_, Position_)
            .Args({"stream"})
            .Body<TCoTake>()
                .Input("stream")
                .Count<TCoUint64>()
                    .Literal()
                        .Value("2")
                        .Build()
                    .Build()
                .Build()
            .Done();
    }

private:
    void ReconnectSourceMap(TTransformMapOverSourceMapGraph& graph)
    {
        graph.SourceOutput = MakeOutput(graph.SourceMap);
        SetChild(
            graph.TransformMap,
            TYtflowMapBase::idx_Sources,
            Ctx_.NewList(Position_, {graph.SourceOutput}));

        ReconnectTransformMap(graph);
    }

    void ReconnectTransformMap(TTransformMapOverSourceMapGraph& graph)
    {
        graph.Root = MakeRoot({graph.TransformMap});
    }
};

TEST(TYtflowTransformSourceMapFusion, ComposesLambdas)
{
    TTransformMapOverSourceMapSetup setup;

    auto graph = setup.MakeTransformMapOverSourceMapGraph();
    setup.SetSourceMapLambda(graph, setup.MakeSkipLambda());
    setup.SetTransformMapLambda(graph, setup.MakeTakeLambda());

    auto fusedMap = setup.OptimizeAndGetFusedMap(graph);
    auto take = fusedMap.Lambda().Body().Cast<TCoTake>();
    auto skip = take.Input().Cast<TCoSkip>();
    EXPECT_EQ("2", take.Count().Cast<TCoUint64>().Literal().Value());
    EXPECT_EQ("1", skip.Count().Cast<TCoUint64>().Literal().Value());
    EXPECT_EQ(fusedMap.Lambda().Args().Arg(0).Raw(), skip.Input().Raw());
}

TEST(TYtflowTransformSourceMapFusion, PreservesWorldDependencies)
{
    TTransformMapOverSourceMapSetup setup;

    auto graph = setup.MakeTransformMapOverSourceMapGraph();
    auto sourceWorld = setup.NewWorld();
    auto transformWorld = setup.NewWorld();
    setup.UseDistinctWorlds(graph, sourceWorld, transformWorld);

    auto world = setup.OptimizeAndGetFusedMap(graph).World().Cast<TCoSync>();
    ASSERT_EQ(2, world.Ref().ChildrenSize());
    EXPECT_EQ(transformWorld.Get(), world.Ref().Child(0));
    EXPECT_EQ(sourceWorld.Get(), world.Ref().Child(1));
}

TEST(TYtflowTransformSourceMapFusion, PreservesOutputIndices)
{
    TTransformMapOverSourceMapSetup setup;

    auto graph = setup.MakeTransformMapOverSourceMapGraph();
    setup.UseReorderedTransformOutputs(graph);

    auto fusedMap = setup.OptimizeAndGetFusedMap(graph);

    EXPECT_EQ("1", TYtflowOutput(graph.Root->Child(0)).OutputIndex().Value());
    EXPECT_EQ("0", TYtflowOutput(graph.Root->Child(1)).OutputIndex().Value());
    EXPECT_EQ(fusedMap.Raw(), TYtflowOutput(graph.Root->Child(1)).Operation().Raw());
    EXPECT_EQ(
        graph.TransformMap->Child(TYtflowMapBase::idx_Sinks),
        fusedMap.Sinks().Raw());
}

TEST(TYtflowTransformSourceMapFusion, PreservesTransformInjection)
{
    TTransformMapOverSourceMapSetup setup;

    auto graph = setup.MakeTransformMapOverSourceMapGraph();
    setup.SetTransformMapSettings(
        graph, setup.MakeSettings(INJECT_INPUT_MESSAGE_ID_SETTING));

    auto fusedMap = setup.OptimizeAndGetFusedMap(graph);
    EXPECT_TRUE(HasSetting(fusedMap.Settings().Ref(), INJECT_INPUT_MESSAGE_ID_SETTING));
}

TEST(TYtflowTransformSourceMapFusion, RejectsMissingMessageIdInjection)
{
    TTransformMapOverSourceMapSetup setup;

    auto graph = setup.MakeTransformMapOverSourceMapGraph();
    setup.SetSourceMapSettings(graph, setup.MakeSettings());

    setup.OptimizeAndExpectNotFused(graph);
}

TEST(TYtflowTransformSourceMapFusion, RejectsUnsupportedSourceSetting)
{
    TTransformMapOverSourceMapSetup setup;

    auto graph = setup.MakeTransformMapOverSourceMapGraph();
    setup.AddSourceMapSetting(graph, "unsupported");

    setup.OptimizeAndExpectNotFused(graph);
}

TEST(TYtflowTransformSourceMapFusion, RejectsUnsupportedTransformSetting)
{
    TTransformMapOverSourceMapSetup setup;

    auto graph = setup.MakeTransformMapOverSourceMapGraph();
    setup.SetTransformMapSettings(graph, setup.MakeSettings("unsupported"));

    setup.OptimizeAndExpectNotFused(graph);
}

TEST(TYtflowTransformSourceMapFusion, RejectsDifferentGroupKey)
{
    TTransformMapOverSourceMapSetup setup;

    auto graph = setup.MakeTransformMapOverSourceMapGraph();
    setup.SetGroupByColumns(graph, {"key"});

    setup.OptimizeAndExpectNotFused(graph);
}

TEST(TYtflowTransformSourceMapFusion, RejectsAdditionalGroupKey)
{
    TTransformMapOverSourceMapSetup setup;

    auto graph = setup.MakeTransformMapOverSourceMapGraph();
    setup.SetGroupByColumns(graph, {YTFLOW_INPUT_MESSAGE_ID_FIELD, "key"});

    setup.OptimizeAndExpectNotFused(graph);
}

TEST(TYtflowTransformSourceMapFusion, RejectsMultipleTransformSources)
{
    TTransformMapOverSourceMapSetup setup;

    auto graph = setup.MakeTransformMapOverSourceMapGraph();
    setup.UseMultipleTransformSources(graph);

    setup.OptimizeAndExpectNotFused(graph);
}

TEST(TYtflowTransformSourceMapFusion, RejectsMultipleSourceMapSinks)
{
    TTransformMapOverSourceMapSetup setup;

    auto graph = setup.MakeTransformMapOverSourceMapGraph();
    setup.UseMultipleSourceMapSinks(graph);

    setup.OptimizeAndExpectNotFused(graph);
}

TEST(TYtflowTransformSourceMapFusion, RejectsDifferentSourceMapOutput)
{
    TTransformMapOverSourceMapSetup setup;

    auto graph = setup.MakeTransformMapOverSourceMapGraph();
    setup.UseSourceMapOutput(graph, 1);

    setup.OptimizeAndExpectNotFused(graph);
}

TEST(TYtflowTransformSourceMapFusion, RejectsSharedSourceMapOutput)
{
    TTransformMapOverSourceMapSetup setup;

    auto graph = setup.MakeTransformMapOverSourceMapGraph();
    setup.AddSourceMapOutputUsage(graph);

    setup.OptimizeAndExpectNotFused(graph);
}

TEST(TYtflowTransformSourceMapFusion, RejectsSourceMapWorldDependency)
{
    TTransformMapOverSourceMapSetup setup;

    auto graph = setup.MakeTransformMapOverSourceMapGraph();
    setup.AddSourceMapWorldUsage(graph);

    setup.OptimizeAndExpectNotFused(graph);
}

TEST(TYtflowTransformSourceMapFusion, RejectsVariantSourceMapOutput)
{
    TTransformMapOverSourceMapSetup setup;

    auto graph = setup.MakeTransformMapOverSourceMapGraph();
    setup.UseVariantSourceMapOutput(graph);

    setup.OptimizeAndExpectNotFused(graph);
}

}
} // namespace NYql::NYtflow::NTest
