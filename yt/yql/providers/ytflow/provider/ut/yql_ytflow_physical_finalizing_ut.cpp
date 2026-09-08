#include "yql_ytflow_physical_finalizing_setup.h"

#include <yt/yql/providers/ytflow/provider/yql_ytflow_constants.h>
#include <yt/yql/providers/ytflow/provider/yql_ytflow_swift_map.h>

#include <yql/essentials/core/yql_opt_utils.h>

#include <library/cpp/testing/gtest/gtest.h>


namespace NYql::NYtflow::NTest {

using namespace NNodes;

namespace {

class TPhysicalFinalizingWithSelectionSetup final : public TPhysicalFinalizingSetup {
public:
    TExprNode::TPtr SelectExtendImplementation(
        TExprNode::TPtr operation,
        bool hasNonDeterministicFunctions)
    {
        return NPrivate::SelectExtendImplementation(
            operation,
            hasNonDeterministicFunctions,
            Ctx_);
    }
};

void AssertFusedSourceMap(
    const TPhysicalFinalizingSetup& setup,
    const TExprNode::TPtr& root,
    std::initializer_list<ui32> expectedSinkOutputIndices,
    std::initializer_list<ui32> expectedSwitchInputIndices
) {
    ASSERT_EQ(1, setup.CountSourceMaps(root));
    ASSERT_EQ(0, setup.CountMaps(root));

    const auto sourceMap = setup.GetSourceMap(root);
    const auto sinks = sourceMap.Sinks();
    ASSERT_EQ(expectedSinkOutputIndices.size(), sinks.Size());

    size_t sinkIndex = 0;
    for (const auto expectedOutputIndex : expectedSinkOutputIndices) {
        ASSERT_EQ(
            expectedOutputIndex,
            ::FromString<ui32>(sinks.Item(sinkIndex).Cast<NNodes::TYtflowSinkBase>().OutputIndex()));
        ++sinkIndex;
    }

    ASSERT_EQ(expectedSinkOutputIndices.size(), root->ChildrenSize());
    for (size_t outputIndex = 0; outputIndex < root->ChildrenSize(); ++outputIndex) {
        ASSERT_TRUE(NNodes::TYtflowOutput::Match(root->Child(outputIndex)));
        const auto output = NNodes::TYtflowOutput(root->Child(outputIndex));
        ASSERT_EQ(sourceMap.Raw(), output.Operation().Raw());
        ASSERT_EQ(outputIndex, ::FromString<ui32>(output.OutputIndex()));
    }

    const auto lambdaBody = sourceMap.Lambda().Body();
    ASSERT_TRUE(lambdaBody.Maybe<NNodes::TCoSwitch>());
    const auto switchNode = lambdaBody.Cast<NNodes::TCoSwitch>();
    ASSERT_EQ(
        2 + 2 * expectedSwitchInputIndices.size(),
        switchNode.Ref().ChildrenSize());

    size_t handlerIndex = 0;
    for (const auto expectedInputIndex : expectedSwitchInputIndices) {
        const auto* inputIndices = switchNode.Ref().Child(2 + 2 * handlerIndex);
        ASSERT_TRUE(NNodes::TCoAtomList::Match(inputIndices));
        ASSERT_EQ(1, inputIndices->ChildrenSize());
        ASSERT_EQ(
            expectedInputIndex,
            ::FromString<ui32>(inputIndices->Child(0)->Content()));
        ++handlerIndex;
    }
}

} // namespace

TEST(TYtflowPhysicalFinalizing, FusesCompatibleSourceMaps)
{
    TPhysicalFinalizingSetup setup;
    auto readWrap = setup.MakeReadWrap();
    auto worldInput = setup.NewWorld();
    auto root = setup.MakeRoot({
        setup.MakeSourceMap(readWrap, setup.MakeSync({worldInput}), setup.MakeSettings()),
        setup.MakeSourceMap(readWrap, setup.MakeSync({worldInput}), setup.MakeSettings())
    });

    setup.Transform(root);

    ASSERT_EQ(1, setup.CountSourceMaps(root));
}

TEST(TYtflowPhysicalFinalizing, DoesNotFuseSourceMapsWithDifferentSettings)
{
    // inject_input_message_id is fusion-compatible by design, so use a synthetic
    // setting to exercise the generic settings mismatch guard.
    TPhysicalFinalizingSetup setup;
    auto readWrap = setup.MakeReadWrap();
    auto world = setup.MakeSync({setup.NewWorld()});
    auto root = setup.MakeRoot({
        setup.MakeSourceMap(
            readWrap,
            world,
            setup.MakeSettings("synthetic_setting", "first")),
        setup.MakeSourceMap(
            readWrap,
            world,
            setup.MakeSettings("synthetic_setting", "second"))
    });

    setup.Transform(root);

    ASSERT_EQ(2, setup.CountSourceMaps(root));
}

TEST(TYtflowPhysicalFinalizing, DoesNotFuseSourceMapsWithDifferentSourceNames)
{
    TPhysicalFinalizingSetup setup;
    auto readWrap = setup.MakeReadWrap();
    auto world = setup.MakeSync({setup.NewWorld()});
    auto root = setup.MakeRoot({
        setup.MakeSourceMap(readWrap, world, setup.MakeSettings(), "first"),
        setup.MakeSourceMap(readWrap, world, setup.MakeSettings(), "second")
    });

    setup.Transform(root);

    ASSERT_EQ(2, setup.CountSourceMaps(root));
}

TEST(TYtflowPhysicalFinalizing, DoesNotFuseSourceMapsWithDifferentWorlds)
{
    TPhysicalFinalizingSetup setup;
    auto readWrap = setup.MakeReadWrap();
    auto commonWorld = setup.NewWorld();
    auto root = setup.MakeRoot({
        setup.MakeSourceMap(
            readWrap,
            setup.MakeSync({commonWorld, setup.NewWorld()}),
            setup.MakeSettings()),
        setup.MakeSourceMap(
            readWrap,
            setup.MakeSync({commonWorld, setup.NewWorld()}),
            setup.MakeSettings())
    });

    setup.Transform(root);

    ASSERT_EQ(2, setup.CountSourceMaps(root));
}

TEST(TYtflowPhysicalFinalizing, FusesAndPropagatesInjectInputMessageIdSetting)
{
    TPhysicalFinalizingSetup setup;
    auto readWrap = setup.MakeReadWrap();
    auto world = setup.MakeSync({setup.NewWorld()});
    auto root = setup.MakeRoot({
        setup.MakeSourceMap(readWrap, world, setup.MakeSettings()),
        setup.MakeSourceMap(
            readWrap,
            world,
            setup.MakeSettings(INJECT_INPUT_MESSAGE_ID_SETTING))
    });

    setup.Transform(root);

    ASSERT_EQ(1, setup.CountSourceMaps(root));
    ASSERT_TRUE(GetSetting(
        setup.GetSourceMap(root).Settings().Ref(),
        INJECT_INPUT_MESSAGE_ID_SETTING));
}

TEST(TYtflowPhysicalFinalizing, FusesMapsWithMultipleSinks)
{
    TPhysicalFinalizingSetup setup;
    auto readWrap = setup.MakeReadWrap();
    auto world = setup.MakeSync({setup.NewWorld()});
    auto sourceMap = setup.MakeSourceMap(
        readWrap,
        world,
        setup.MakeSettings(),
        "source",
        {0, 1});
    auto firstMap = setup.MakeMap(setup.MakeOutput(sourceMap, 0), world, {0, 1, 2});
    auto secondMap = setup.MakeMap(setup.MakeOutput(sourceMap, 1), world, {0, 1});

    TExprNode::TListType outputs;
    outputs.reserve(5);
    for (ui32 outputIndex = 0; outputIndex < 3; ++outputIndex) {
        outputs.push_back(setup.MakeOutput(firstMap, outputIndex));
    }
    for (ui32 outputIndex = 0; outputIndex < 2; ++outputIndex) {
        outputs.push_back(setup.MakeOutput(secondMap, outputIndex));
    }
    auto root = setup.MakeRootFromOutputs(std::move(outputs));

    setup.Transform(root);

    AssertFusedSourceMap(setup, root, {0, 1, 2, 3, 4}, {0, 1});
}

TEST(TYtflowPhysicalFinalizing, FusesMapsWithUnorderedAndRepeatedSinkOutputIndices)
{
    TPhysicalFinalizingSetup setup;
    auto readWrap = setup.MakeReadWrap();
    auto world = setup.MakeSync({setup.NewWorld()});
    auto sourceMap = setup.MakeSourceMap(
        readWrap,
        world,
        setup.MakeSettings(),
        "source",
        {1, 0});
    auto firstMap = setup.MakeMap(setup.MakeOutput(sourceMap, 0), world, {2, 0, 2, 1});
    auto secondMap = setup.MakeMap(setup.MakeOutput(sourceMap, 1), world, {1, 0});

    TExprNode::TListType outputs;
    outputs.reserve(6);
    for (ui32 outputIndex = 0; outputIndex < 4; ++outputIndex) {
        outputs.push_back(setup.MakeOutput(firstMap, outputIndex));
    }
    for (ui32 outputIndex = 0; outputIndex < 2; ++outputIndex) {
        outputs.push_back(setup.MakeOutput(secondMap, outputIndex));
    }
    auto root = setup.MakeRootFromOutputs(std::move(outputs));

    setup.Transform(root);

    AssertFusedSourceMap(setup, root, {2, 0, 2, 1, 4, 3}, {1, 0});
}

TEST(TYtflowPhysicalFinalizing, InjectsInputMessageIdSettingIntoEveryExtendInputOperation)
{
    TPhysicalFinalizingSetup setup;
    auto world = setup.MakeSync({setup.NewWorld()});
    auto root = setup.MakeRootFromOutputs({
        setup.MakeOutput(setup.MakeExtend({
            setup.MakeOutput(setup.MakeSourceMap(
                setup.MakeReadWrap(),
                world,
                setup.MakeSettings())),
            setup.MakeOutput(setup.MakeSourceMap(
                setup.MakeReadWrap(),
                world,
                setup.MakeSettings()))
        }, world))
    });

    setup.Transform(root);

    for (auto source : setup.GetExtend(root).Sources()) {
        auto producer = source.Cast<TYtflowOutput>().Operation().Cast<TYtflowMapBase>();
        ASSERT_TRUE(GetSetting(
            producer.Settings().Ref(),
            INJECT_INPUT_MESSAGE_ID_SETTING));
    }
}

TEST(TYtflowPhysicalFinalizing, InjectsInputMessageIdForSelectedExtendImplementations)
{
    for (const bool hasNonDeterministicFunctions : {false, true}) {
        TPhysicalFinalizingWithSelectionSetup setup;
        auto world = setup.MakeSync({setup.NewWorld()});
        auto implementation = setup.SelectExtendImplementation(
            setup.MakeExtend({
                setup.MakeOutput(setup.MakeSourceMap(
                    setup.MakeReadWrap(),
                    world,
                    setup.MakeSettings())),
                setup.MakeOutput(setup.MakeSourceMap(
                    setup.MakeReadWrap(),
                    world,
                    setup.MakeSettings()))
            }, world),
            hasNonDeterministicFunctions);
        auto root = setup.MakeRootFromOutputs({
            setup.MakeOutput(implementation)
        });

        setup.Transform(root);

        auto transformedImplementation = TYtflowOutput(root->Child(0))
            .Operation()
            .Cast<TYtflowMapBase>();
        ASSERT_EQ(
            implementation->Content(),
            transformedImplementation.Ref().Content());
        ASSERT_TRUE(GetSetting(
            transformedImplementation.Settings().Ref(),
            EXTEND_SETTING));
        for (auto source : transformedImplementation.Sources()) {
            auto producer = source.Cast<TYtflowOutput>().Operation().Cast<TYtflowMapBase>();
            ASSERT_TRUE(GetSetting(
                producer.Settings().Ref(),
                INJECT_INPUT_MESSAGE_ID_SETTING));
        }
    }
}

TEST(TYtflowPhysicalFinalizing, PreservesDirectExtendConsumerWhenFusingMap)
{
    TPhysicalFinalizingSetup setup;
    auto world = setup.MakeSync({setup.NewWorld()});
    auto producer = setup.MakeSourceMap(
        setup.MakeReadWrap(),
        world,
        setup.MakeSettings());
    auto producerOutput = setup.MakeOutput(producer);
    auto mapOutput = setup.MakeOutput(setup.MakeMap(producerOutput, world));
    auto root = setup.MakeRootFromOutputs({
        setup.MakeOutput(setup.MakeExtend({producerOutput, mapOutput}, world))
    });

    setup.Transform(root);

    ASSERT_EQ(1, setup.CountSourceMaps(root));
    ASSERT_EQ(0, setup.CountMaps(root));

    auto sourceMap = setup.GetSourceMap(root);
    ASSERT_EQ(2, sourceMap.Sinks().Size());

    auto maybeSwitch = sourceMap.Lambda().Body().Maybe<TCoSwitch>();
    ASSERT_TRUE(maybeSwitch);
    auto switchNode = maybeSwitch.Cast();
    ASSERT_EQ(6, switchNode.Ref().ChildrenSize());
    ASSERT_EQ("0", switchNode.Ref().Child(2)->Child(0)->Content());
    ASSERT_EQ("0", switchNode.Ref().Child(4)->Child(0)->Content());

    auto extend = setup.GetExtend(root);
    ASSERT_EQ(2, extend.Sources().Size());
    auto firstOutput = extend.Sources().Item(0).Cast<TYtflowOutput>();
    auto secondOutput = extend.Sources().Item(1).Cast<TYtflowOutput>();
    ASSERT_EQ(firstOutput.Operation().Raw(), secondOutput.Operation().Raw());
    ASSERT_EQ("0", firstOutput.OutputIndex().Value());
    ASSERT_EQ("1", secondOutput.OutputIndex().Value());
}

TEST(TYtflowPhysicalFinalizing, FusesDuplicateExtendInputsIntoDistinctProducerBranches)
{
    TPhysicalFinalizingSetup setup;
    auto world = setup.MakeSync({setup.NewWorld()});
    auto producer = setup.MakeSourceMap(
        setup.MakeReadWrap(),
        world,
        setup.MakeSettings());
    auto producerOutput = setup.MakeOutput(producer);
    auto equivalentProducerOutput = setup.MakeOutput(producer);
    auto root = setup.MakeRootFromOutputs({
        setup.MakeOutput(setup.MakeExtend({
            producerOutput,
            equivalentProducerOutput,
            producerOutput
        }, world))
    });

    setup.Transform(root);

    ASSERT_EQ(1, setup.CountSourceMaps(root));
    ASSERT_EQ(0, setup.CountMaps(root));

    auto sourceMap = setup.GetSourceMap(root);
    ASSERT_EQ(3, sourceMap.Sinks().Size());

    auto maybeSwitch = sourceMap.Lambda().Body().Maybe<TCoSwitch>();
    ASSERT_TRUE(maybeSwitch);
    auto switchNode = maybeSwitch.Cast();
    ASSERT_EQ(8, switchNode.Ref().ChildrenSize());
    ASSERT_EQ("0", switchNode.Ref().Child(2)->Child(0)->Content());
    ASSERT_EQ("0", switchNode.Ref().Child(4)->Child(0)->Content());
    ASSERT_EQ("0", switchNode.Ref().Child(6)->Child(0)->Content());

    auto extend = setup.GetExtend(root);
    ASSERT_EQ(3, extend.Sources().Size());
    auto firstOutput = extend.Sources().Item(0).Cast<TYtflowOutput>();
    auto secondOutput = extend.Sources().Item(1).Cast<TYtflowOutput>();
    auto thirdOutput = extend.Sources().Item(2).Cast<TYtflowOutput>();
    ASSERT_EQ(firstOutput.Operation().Raw(), secondOutput.Operation().Raw());
    ASSERT_EQ(firstOutput.Operation().Raw(), thirdOutput.Operation().Raw());
    ASSERT_EQ("0", firstOutput.OutputIndex().Value());
    ASSERT_EQ("1", secondOutput.OutputIndex().Value());
    ASSERT_EQ("2", thirdOutput.OutputIndex().Value());
}

TEST(TYtflowPhysicalFinalizing, FusesDuplicateSelectedExtendInputsIntoDistinctProducerBranches)
{
    TPhysicalFinalizingWithSelectionSetup setup;
    auto world = setup.MakeSync({setup.NewWorld()});
    auto producer = setup.MakeSourceMap(
        setup.MakeReadWrap(),
        world,
        setup.MakeSettings());
    auto producerOutput = setup.MakeOutput(producer);
    auto implementation = setup.SelectExtendImplementation(
        setup.MakeExtend({
            producerOutput,
            setup.MakeOutput(producer),
            producerOutput
        }, world),
        true);
    auto root = setup.MakeRootFromOutputs({
        setup.MakeOutput(implementation)
    });

    setup.Transform(root);

    auto transformedImplementation = TYtflowOutput(root->Child(0))
        .Operation()
        .Cast<TYtflowMapBase>();
    ASSERT_TRUE(TYtflowTransformMap::Match(transformedImplementation.Raw()));
    ASSERT_TRUE(GetSetting(
        transformedImplementation.Settings().Ref(),
        EXTEND_SETTING));
    ASSERT_EQ(3, transformedImplementation.Sources().Size());

    auto firstOutput = transformedImplementation.Sources().Item(0).Cast<TYtflowOutput>();
    auto secondOutput = transformedImplementation.Sources().Item(1).Cast<TYtflowOutput>();
    auto thirdOutput = transformedImplementation.Sources().Item(2).Cast<TYtflowOutput>();
    ASSERT_EQ(firstOutput.Operation().Raw(), secondOutput.Operation().Raw());
    ASSERT_EQ(firstOutput.Operation().Raw(), thirdOutput.Operation().Raw());
    ASSERT_EQ("0", firstOutput.OutputIndex().Value());
    ASSERT_EQ("1", secondOutput.OutputIndex().Value());
    ASSERT_EQ("2", thirdOutput.OutputIndex().Value());
}

TEST(TYtflowPhysicalFinalizing, PreservesDirectExtendConsumerAcrossMapFusionIterations)
{
    TPhysicalFinalizingSetup setup;
    auto world = setup.MakeSync({setup.NewWorld()});
    auto sourceMap = setup.MakeSourceMap(
        setup.MakeReadWrap(),
        world,
        setup.MakeSettings());
    auto producer = setup.MakeMap(setup.MakeOutput(sourceMap), world);
    auto producerOutput = setup.MakeOutput(producer);
    auto mapOutput = setup.MakeOutput(setup.MakeMap(producerOutput, world));
    auto root = setup.MakeRootFromOutputs({
        setup.MakeOutput(setup.MakeExtend({producerOutput, mapOutput}, world))
    });

    setup.Transform(root);

    ASSERT_EQ(1, setup.CountSourceMaps(root));
    ASSERT_EQ(0, setup.CountMaps(root));

    auto fusedSourceMap = setup.GetSourceMap(root);
    ASSERT_EQ(2, fusedSourceMap.Sinks().Size());

    auto extend = setup.GetExtend(root);
    ASSERT_EQ(2, extend.Sources().Size());
    auto firstOutput = extend.Sources().Item(0).Cast<TYtflowOutput>();
    auto secondOutput = extend.Sources().Item(1).Cast<TYtflowOutput>();
    ASSERT_EQ(firstOutput.Operation().Raw(), secondOutput.Operation().Raw());
    ASSERT_EQ("0", firstOutput.OutputIndex().Value());
    ASSERT_EQ("1", secondOutput.OutputIndex().Value());
}

} // namespace NYql::NYtflow::NTest
