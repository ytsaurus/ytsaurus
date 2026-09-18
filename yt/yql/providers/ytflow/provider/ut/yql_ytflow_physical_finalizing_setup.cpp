#include "yql_ytflow_physical_finalizing_setup.h"

#include <yt/yql/providers/ytflow/provider/yql_ytflow_constants.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/yql_panic.h>


namespace NYql::NYtflow::NTest {

using namespace NNodes;


TPhysicalFinalizingSetup::TPhysicalFinalizingSetup()
    : State_(MakeIntrusive<TYtflowState>())
{
    State_->Types = &Types_;
    State_->Configuration = MakeIntrusive<TYtflowConfiguration>();
    State_->Configuration->_SwitchComputationNodeBufferSizeBytes = 1;
}

TExprNode::TPtr TPhysicalFinalizingSetup::NewWorld() {
    return Ctx_.NewWorld(Position_);
}

TExprNode::TPtr TPhysicalFinalizingSetup::MakeReadWrap() {
    return Build<TYtflowReadWrap>(Ctx_, Position_)
        .Input<TCoVoid>()
            .Build()
        .Done().Ptr();
}

TExprNode::TPtr TPhysicalFinalizingSetup::MakeSync(
    std::initializer_list<TExprNode::TPtr> inputs
) {
    TExprNode::TListType children(inputs);
    return Build<TCoSync>(Ctx_, Position_)
        .Add(std::move(children))
        .Done().Ptr();
}

TCoNameValueTupleList TPhysicalFinalizingSetup::MakeSettings(
    TStringBuf name,
    TStringBuf value
) {
    auto settings = Build<TCoNameValueTupleList>(Ctx_, Position_);
    if (name) {
        settings
            .Add()
                .Name()
                    .Value(name)
                    .Build()
                .Value<TCoAtom>()
                    .Value(value)
                    .Build()
                .Build();
    }

    return settings.Done();
}

TExprNode::TPtr TPhysicalFinalizingSetup::MakeSourceMap(
    TExprNode::TPtr readWrap,
    TExprNode::TPtr world,
    TCoNameValueTupleList settings,
    TStringBuf sourceName,
    std::initializer_list<ui32> sinkOutputIndices
) {
    auto source = Build<TYtflowPersistentSource>(Ctx_, Position_)
        .Name()
            .Value(sourceName)
            .Build()
        .Input(TExprBase(std::move(readWrap)))
        .Done();

    return Build<TYtflowSourceMap>(Ctx_, Position_)
        .World(std::move(world))
        .Sources()
            .Add(std::move(source))
            .Build()
        .Sinks()
            .Add(MakeIntermediateSinks(sinkOutputIndices))
            .Build()
        .Settings(std::move(settings))
        .Lambda(MakePassthroughLambda(sinkOutputIndices))
        .Done().Ptr();
}

TExprNode::TPtr TPhysicalFinalizingSetup::MakeOutput(
    TExprNode::TPtr operation,
    ui32 outputIndex
) {
    return Build<TYtflowOutput>(Ctx_, Position_)
        .Operation(std::move(operation))
        .OutputIndex()
            .Value(outputIndex)
            .Build()
        .Done().Ptr();
}

TExprNode::TPtr TPhysicalFinalizingSetup::MakeMap(
    TExprNode::TPtr source,
    TExprNode::TPtr world,
    std::initializer_list<ui32> sinkOutputIndices
) {
    return Build<TYtflowMap>(Ctx_, Position_)
        .World(std::move(world))
        .Sources()
            .Add(TExprBase(std::move(source)))
            .Build()
        .Sinks()
            .Add(MakeIntermediateSinks(sinkOutputIndices))
            .Build()
        .Settings()
            .Build()
        .Lambda(MakePassthroughLambda(sinkOutputIndices))
        .Done().Ptr();
}

TExprNode::TPtr TPhysicalFinalizingSetup::MakeExtend(
    std::initializer_list<TExprNode::TPtr> sources,
    TExprNode::TPtr world
) {
    TVector<TExprBase> sourceNodes;
    sourceNodes.reserve(sources.size());
    for (auto source : sources) {
        sourceNodes.emplace_back(std::move(source));
    }

    return Build<TYtflowExtend>(Ctx_, Position_)
        .World(std::move(world))
        .Sources()
            .Add(std::move(sourceNodes))
            .Build()
        .Sinks()
            .Add(MakeIntermediateSinks({0}))
            .Build()
        .Settings()
            .Build()
        .Lambda(MakePassthroughLambda({0}))
        .GroupByColumns()
            .Add<TCoAtom>()
                .Value(YTFLOW_INPUT_MESSAGE_ID_FIELD)
                .Build()
            .Build()
        .Done().Ptr();
}

TExprNode::TPtr TPhysicalFinalizingSetup::MakeRoot(
    std::initializer_list<TExprNode::TPtr> sourceMaps
) {
    TExprNode::TListType outputs;
    outputs.reserve(sourceMaps.size());
    for (auto sourceMap : sourceMaps) {
        outputs.push_back(MakeOutput(std::move(sourceMap)));
    }

    return MakeRootFromOutputs(std::move(outputs));
}

TExprNode::TPtr TPhysicalFinalizingSetup::MakeRootFromOutputs(
    TExprNode::TListType outputs
) {
    return Ctx_.NewCallable(Position_, "TestRoot", std::move(outputs));
}

void TPhysicalFinalizingSetup::Transform(TExprNode::TPtr& root) {
    auto transformer = CreateYtflowPhysicalFinalizingTransformer(State_);
    const auto status = SyncTransform(*transformer, root, Ctx_);
    YQL_ENSURE(
        status == IGraphTransformer::TStatus::Ok,
        "Physical finalizing transform failed: " << Ctx_.IssueManager.GetIssues().ToString());
}

size_t TPhysicalFinalizingSetup::CountSourceMaps(const TExprNode::TPtr& root) const {
    size_t count = 0;
    VisitExpr(root, [&count](const TExprNode::TPtr& node) {
        if (TYtflowSourceMap::Match(node.Get())) {
            ++count;
        }

        return true;
    });
    return count;
}

size_t TPhysicalFinalizingSetup::CountMaps(const TExprNode::TPtr& root) const {
    size_t count = 0;
    VisitExpr(root, [&count](const TExprNode::TPtr& node) {
        if (TYtflowMap::Match(node.Get())) {
            ++count;
        }

        return true;
    });
    return count;
}

TYtflowSourceMap TPhysicalFinalizingSetup::GetSourceMap(const TExprNode::TPtr& root) const {
    const TExprNode* sourceMap = nullptr;
    VisitExpr(root, [&sourceMap](const TExprNode::TPtr& node) {
        if (TYtflowSourceMap::Match(node.Get())) {
            YQL_ENSURE(!sourceMap, "Expected exactly one YtflowSourceMap, but found multiple");
            sourceMap = node.Get();
        }

        return true;
    });
    YQL_ENSURE(sourceMap, "Expected exactly one YtflowSourceMap, but found none");
    return TYtflowSourceMap(sourceMap);
}

TYtflowExtend TPhysicalFinalizingSetup::GetExtend(const TExprNode::TPtr& root) const {
    const TExprNode* extend = nullptr;
    VisitExpr(root, [&extend](const TExprNode::TPtr& node) {
        if (TYtflowExtend::Match(node.Get())) {
            YQL_ENSURE(!extend, "Expected exactly one YtflowExtend, but found multiple");
            extend = node.Get();
        }

        return true;
    });
    YQL_ENSURE(extend, "Expected exactly one YtflowExtend, but found none");
    return TYtflowExtend(extend);
}

TVector<TExprBase> TPhysicalFinalizingSetup::MakeIntermediateSinks(
    std::initializer_list<ui32> outputIndices
) {
    TVector<TExprBase> sinks;
    sinks.reserve(outputIndices.size());
    for (const auto outputIndex : outputIndices) {
        sinks.push_back(Build<TYtflowIntermediateSink>(Ctx_, Position_)
            .Name()
                .Value("")
                .Build()
            .OutputIndex()
                .Value(outputIndex)
                .Build()
            .RowType<TCoVoid>()
                .Build()
            .Done());
    }
    return sinks;
}

TCoLambda TPhysicalFinalizingSetup::MakePassthroughLambda(
    std::initializer_list<ui32> outputIndices
) {
    ui32 outputCount = 0;
    for (const auto outputIndex : outputIndices) {
        outputCount = Max(outputCount, outputIndex + 1);
    }

    if (outputCount == 1) {
        return Build<TCoLambda>(Ctx_, Position_)
            .Args({"stream"})
            .Body("stream")
            .Done();
    }

    TVector<TExprBase> switchArgs;
    switchArgs.reserve(2 * outputCount);
    for (ui32 outputIndex = 0; outputIndex < outputCount; ++outputIndex) {
        switchArgs.push_back(Build<TCoAtomList>(Ctx_, Position_)
            .Add()
                .Value(0)
                .Build()
            .Done());
        switchArgs.push_back(Build<TCoLambda>(Ctx_, Position_)
            .Args({"item"})
            .Body("item")
            .Done());
    }

    return Build<TCoLambda>(Ctx_, Position_)
        .Args({"stream"})
        .Body<TCoSwitch>()
            .Input("stream")
            .BufferBytes()
                .Value(1)
                .Build()
            .FreeArgs()
                .Add(std::move(switchArgs))
                .Build()
            .Build()
        .Done();
}

} // namespace NYql::NYtflow::NTest
