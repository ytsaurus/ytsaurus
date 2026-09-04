#include "yql_ytflow_provider_impl.h"
#include "yql_ytflow_swift_map.h"
#include "yql_ytflow_utils.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/peephole_opt/yql_opt_peephole_physical.h>
#include <yql/essentials/core/services/yql_transform_pipeline.h>
#include <yql/essentials/core/yql_execution.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/transform/yql_exec.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

#include <memory>


namespace NYql {

using namespace NNodes;

namespace {

class TExtendImplementationPipelineConfigurator final : public IPipelineConfigurator {
public:
    explicit TExtendImplementationPipelineConfigurator(bool& hasNonDeterministicFunctions)
        : HasNonDeterministicFunctions_(hasNonDeterministicFunctions)
    {}

private:
    void AfterCreate(TTransformationPipeline*) const final {}

    void AfterTypeAnnotation(TTransformationPipeline*) const final {}

    void AfterOptimize(TTransformationPipeline* pipeline) const final {
        pipeline->Add(
            CreateFunctorTransformer(
                [this](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx)
                    -> IGraphTransformer::TStatus {
                    output = NYtflow::NPrivate::SelectExtendImplementation(
                        input,
                        HasNonDeterministicFunctions_,
                        ctx);
                    if (output == input) {
                        return IGraphTransformer::TStatus::Ok;
                    }

                    return IGraphTransformer::TStatus(
                        IGraphTransformer::TStatus::Repeat,
                        true);
                }),
            "YtflowExtendImplementation",
            TIssuesIds::CORE_EXEC);
    }

    bool& HasNonDeterministicFunctions_;
};

} // namespace


class TYtflowDataSinkExecTransformer: public TExecTransformerBase {
public:
    TYtflowDataSinkExecTransformer(TYtflowState::TPtr state)
        : State(std::move(state))
    {
#define HANDLER(name) \
    Hndl(&TYtflowDataSinkExecTransformer::Handle##name)

        AddHandler({TYtflowOutput::CallableName()}, RequireFirst(), Pass());
        AddHandler(
            {
                TYtflowSourceMap::CallableName(),
                TYtflowTransformMap::CallableName(),
                TYtflowSwiftMap::CallableName(),
                TYtflowExtend::CallableName(),
                TYtflowMap::CallableName(),
                TYtflowHoppingAggregate::CallableName(),
            },
            RequireAllOf({TYtflowOpBase::idx_World, TYtflowOpBase::idx_Sources}),
            HANDLER(OpBase));

        AddHandler({TYtflowPublish::CallableName()}, RequireFirst(), HANDLER(Publish));

#undef HANDLER
    }

private:
    TStatus EnsureNamedSourcesAndSinks(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        output = input;

        auto operation = TYtflowOpBase(output);

        TVector<size_t> unnamedSourceIndices;
        for (size_t index = 0; index < operation.Sources().Size(); ++index) {
            auto source = operation.Sources().Item(index);
            auto maybeName = source.Maybe<TYtflowPersistentSource>().Name();
            if (maybeName && !maybeName.Cast().Value()) {
                unnamedSourceIndices.push_back(index);
            }
        }

        TVector<size_t> unnamedSinkIndices;
        for (size_t index = 0; index < operation.Sinks().Size(); ++index) {
            auto sink = operation.Sinks().Item(index);
            auto maybeName = sink.Maybe<TYtflowSinkBase>().Name();
            if (maybeName && !maybeName.Cast().Value()) {
                unnamedSinkIndices.push_back(index);
            }
        }

        if (!unnamedSourceIndices && !unnamedSinkIndices) {
            return TStatus::Ok;
        }

        auto children = output->ChildrenList();

        if (unnamedSourceIndices) {
            auto& sourcesNode = children[TYtflowOpBase::idx_Sources];
            auto sourceChildren = sourcesNode->ChildrenList();

            for (auto index: unnamedSourceIndices) {
                RenameNode(sourceChildren[index], TYtflowPersistentSource::idx_Name, ctx);
            }

            sourcesNode = ctx.ChangeChildren(*sourcesNode, std::move(sourceChildren));
        }

        if (unnamedSinkIndices) {
            auto& sinksNode = children[TYtflowOpBase::idx_Sinks];
            auto sinkChildren = sinksNode->ChildrenList();

            for (auto index: unnamedSinkIndices) {
                RenameNode(sinkChildren[index], TYtflowSinkBase::idx_Name, ctx);
            }

            sinksNode = ctx.ChangeChildren(*sinksNode, std::move(sinkChildren));
        }

        output = ctx.ChangeChildren(*output, std::move(children));

        return TStatus(TStatus::Repeat, true);
    }

    void RenameNode(TExprNode::TPtr& node, size_t nameIndex, TExprContext& ctx) {
        auto name = TStringBuilder() <<
            "stream_" << node->Content()
            << "_" << StreamCounters[node->Content()]++;

        node = ctx.ChangeChild(*node, nameIndex, ctx.NewAtom(TPositionHandle{}, name));
    }

    TStatus DoPeepHoleOptimization(
        const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx
    ) {
        // NOTE: peephole optimization is tricky - it's not idempotent
        // (applies common opt in earlier stages), so repeat calls should be avoided manually.
        // While doing so, one should not return new nodes with Ok status
        // (Repeat without restart, followed by Ok status is treated similarly),
        // so nodes should be marked with ExecutionComplete, and pipeline should be restarted.

        output = input;

        if (auto iterator = PeepHoleProcessedNodes.find(output.Get());
            iterator != PeepHoleProcessedNodes.end()
        ) {
            PeepHoleProcessedNodes.erase(iterator);

            output->SetState(TExprNode::EState::ExecutionComplete);
            output->SetResult(ctx.NewWorld(output->Pos()));

            return TStatus(TStatus::Repeat, true);
        }

        auto oldState = output->GetState();
        output->SetState(TExprNode::EState::ConstrComplete);

        bool hasNonDeterministicFunctions = false;
        const TExtendImplementationPipelineConfigurator pipelineConfigurator(
            hasNonDeterministicFunctions);
        TPeepholeSettings peepholeSettings;
        peepholeSettings.FinalConfig = &pipelineConfigurator;
        if (auto status = PeepHoleOptimizeNode(
                output, output, ctx,
                *State->Types, nullptr, hasNonDeterministicFunctions, peepholeSettings);
            status != TStatus::Ok
        ) {
            return status;
        }

        output->SetState(oldState);

        PeepHoleProcessedNodes.emplace(output.Get());

        return TStatus::Repeat;
    }

    TStatusCallbackPair HandleOpBase(
        const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx
    ) {
        output = input;

        if (auto status = EnsureNamedSourcesAndSinks(output, output, ctx);
            status != TStatus::Ok
        ) {
            return SyncStatus(status);
        }

        auto peepHoleStatus = DoPeepHoleOptimization(output, output, ctx);

        return SyncStatus(peepHoleStatus);
    }

    TStatusCallbackPair HandlePublish(const TExprNode::TPtr& node, TExprContext& ctx) {
        auto userFilesDownloadFilter = [](const TString& /*url*/) {
            return true;
        };

        TUserDataTable files;
        if (auto statusCallbackPair = NCommon::FreezeUsedFiles(
                *node, files, *State->Types,
                ctx, std::move(userFilesDownloadFilter));
            statusCallbackPair.first != TStatus::Ok
        ) {
            return statusCallbackPair;
        }

        auto options = IYtflowGateway::TRunOptions()
            .SessionId(State->SessionId)
            .PublicId(State->Types->TranslateOperationId(node->UniqueId()))
            .Config(std::make_shared<const TYtflowSettings>(*State->Configuration))
            .Types(State->Types)
            .UserDataBlocks(std::move(files));

        return WrapFutureCallback(
            State->Gateway->Run(node, options, ctx),
            [](
                NCommon::TOperationResult /*result*/, const TExprNode::TPtr& input,
                TExprNode::TPtr& output, TExprContext& ctx
            ) {
                output = input;

                input->SetState(TExprNode::EState::ExecutionComplete);
                input->SetResult(ctx.NewWorld(input->Pos()));

                return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Ok);
            });
    }

    void Rewind() final {
        TExecTransformerBase::Rewind();

        StreamCounters.clear();
        PeepHoleProcessedNodes.clear();
    }

private:
    TYtflowState::TPtr State;

    THashMap<TStringBuf, ui32> StreamCounters;
    TNodeSet PeepHoleProcessedNodes;
};


THolder<TExecTransformerBase> CreateYtflowDataSinkExecTransformer(TYtflowState::TPtr state) {
    return MakeHolder<TYtflowDataSinkExecTransformer>(std::move(state));
}

} // namespace NYql
