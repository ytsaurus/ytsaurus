#include "yql_ytflow_physical_finalizing_setup.h"

#include <yt/yql/providers/ytflow/provider/yql_ytflow_constants.h>
#include <yt/yql/providers/ytflow/provider/yql_ytflow_provider.h>
#include <yt/yql/providers/ytflow/provider/yql_ytflow_utils.h>

#include <yql/essentials/core/yql_expr_csee.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/utils/yql_panic.h>

#include <utility>


namespace NYql::NYtflow::NTest {

using namespace NNodes;


TPhysicalFinalizingSetup::TPhysicalFinalizingSetup()
    : State_(MakeIntrusive<TYtflowState>())
{
    State_->Types = &Types_;
    State_->Configuration = MakeIntrusive<TYtflowConfiguration>();
    State_->Configuration->_SwitchComputationNodeBufferSizeBytes = 1;

    Types_.AddDataSource(YtflowProviderName, CreateYtflowDataSource(State_));
    Types_.AddDataSink(YtflowProviderName, CreateYtflowDataSink(State_));
}

TExprNode::TPtr TPhysicalFinalizingSetup::NewWorld() {
    return Ctx_.NewWorld(Position_);
}

TExprNode::TPtr TPhysicalFinalizingSetup::MakeReadWrap() {
    auto input = Ctx_.NewCallable(Position_, "TestInput", {});
    input->SetTypeAnn(Ctx_.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
        Ctx_.MakeType<TWorldExprType>(),
        Ctx_.MakeType<TListExprType>(Ctx_.MakeType<TStructExprType>(
            TVector<const TItemExprType*>{}))
    }));
    input->SetState(TExprNode::EState::TypeComplete);

    return Build<TYtflowReadWrap>(Ctx_, Position_)
        .Input(TExprBase(std::move(input)))
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
    ui32 outputCount
) {
    auto source = Build<TYtflowPersistentSource>(Ctx_, Position_)
        .Input(TExprBase(std::move(readWrap)))
        .Done();

    return Build<TYtflowSourceMap>(Ctx_, Position_)
        .World(std::move(world))
        .Sources()
            .Add(std::move(source))
            .Build()
        .Settings(std::move(settings))
        .Lambda(MakePassthroughLambda(outputCount))
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

TExprNode::TPtr TPhysicalFinalizingSetup::MakeLeft(TExprNode::TPtr operation)
{
    return Build<TCoLeft>(Ctx_, Position_)
        .Input(std::move(operation))
        .Done().Ptr();
}

TExprNode::TPtr TPhysicalFinalizingSetup::MakePersistentSink(
    TExprNode::TPtr world,
    TExprNode::TPtr output
) {
    return Build<TYtflowPersistentSink>(Ctx_, Position_)
        .World(std::move(world))
        .Output(std::move(output))
        .Write<TYtflowWriteWrap>()
            .Input(NewWorld())
            .Build()
        .Done().Ptr();
}

TExprNode::TPtr TPhysicalFinalizingSetup::MakeMap(
    TExprNode::TPtr source,
    TExprNode::TPtr world,
    ui32 outputCount
) {
    return Build<TYtflowMap>(Ctx_, Position_)
        .World(std::move(world))
        .Sources()
            .Add(TExprBase(std::move(source)))
            .Build()
        .Settings()
            .Build()
        .Lambda(MakePassthroughLambda(outputCount))
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
        .Settings()
            .Build()
        .Lambda(MakePassthroughLambda(/*outputCount=*/ 1))
        .GroupByColumns()
            .Add<TCoAtom>()
                .Value(YTFLOW_INPUT_MESSAGE_ID_FIELD)
                .Build()
            .Build()
        .Done().Ptr();
}

TExprNode::TPtr TPhysicalFinalizingSetup::MakeRoot(
    TExprNode::TListType children
) {
    return Ctx_.NewCallable(Position_, "TestRoot", std::move(children));
}

void TPhysicalFinalizingSetup::AnnotateTypes(TExprNode::TPtr& root)
{
    auto standardTransformer = CreateExtCallableTypeAnnotationTransformer(Types_, /*instantOnly*/ true);
    auto callableTransformer = CreateFunctorTransformer(
        [&] (const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) -> IGraphTransformer::TStatus {
            output = input;
            if (input->IsCallable("TestRoot")) {
                TTypeAnnotationNode::TListType childTypes;
                for (const auto& child : input->Children()) {
                    childTypes.push_back(child->GetTypeAnn());
                }
                input->SetTypeAnn(ctx.MakeType<TTupleExprType>(childTypes));
                return IGraphTransformer::TStatus::Ok;
            }

            if (auto maybeSink = TMaybeNode<TYtflowPersistentSink>(input)) {
                if (!EnsureArgsCount(*input, 3, ctx)) {
                    return IGraphTransformer::TStatus::Error;
                }

                auto sink = maybeSink.Cast();
                YQL_ENSURE(sink.Write().Input().Ref().IsWorld(),
                    "Expected World write input in test persistent sink");
                if (!EnsureWorldType(sink.World().Ref(), ctx) ||
                    !EnsureListOfStructs(sink.Output().Pos(), *sink.Output().Ref().GetTypeAnn(), ctx))
                {
                    return IGraphTransformer::TStatus::Error;
                }

                input->SetTypeAnn(sink.World().Ref().GetTypeAnn());
                return IGraphTransformer::TStatus::Ok;
            }

            return standardTransformer->Transform(input, output, ctx);
        });
    auto transformer = CreateTypeAnnotationTransformer(callableTransformer, Types_);
    YQL_ENSURE(SyncTransform(*transformer, root, Ctx_) == IGraphTransformer::TStatus::Ok,
        "Type annotation failed: " << Ctx_.IssueManager.GetIssues().ToString());
}

void TPhysicalFinalizingSetup::Transform(TExprNode::TPtr& root) {
    auto transformer = CreateYtflowPhysicalFinalizingTransformer(State_);

    for (ui32 iteration = 0; iteration < Ctx_.RepeatTransformLimit; ++iteration) {
        AnnotateTypes(root);
        YQL_ENSURE(UpdateCompletness(root, root, Ctx_) == IGraphTransformer::TStatus::Ok,
            "Failed to update completeness before optimization");

        TExprNode::TPtr output;
        const auto status = transformer->Transform(root, output, Ctx_);
        if (output) {
            root = std::move(output);
        }

        if (status == IGraphTransformer::TStatus::Ok) {
            return;
        }

        YQL_ENSURE(status == IGraphTransformer::TStatus::Repeat,
            "Optimization transform failed: " << Ctx_.IssueManager.GetIssues().ToString());
    }

    YQL_ENSURE(false, "Optimization transform exceeded repeat limit");
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

TCoLambda TPhysicalFinalizingSetup::MakePassthroughLambda(ui32 outputCount) {
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
