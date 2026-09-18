#include "yql_ytflow_provider_impl.h"
#include "yql_ytflow_utils.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/transform/yql_lazy_init.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>


namespace NYql {

using namespace NNodes;


class TYtflowDataSink: public TDataProviderBase {
public:
    TYtflowDataSink(TYtflowState::TPtr state)
        : State_(std::move(state))
        , TypeAnnotationTransformer_(
            [this] {
                return CreateYtflowDataSinkTypeAnnotationTransformer(State_);
            })
        , RecaptureOptProposalTransformer_(
            [this] {
                return CreateYtflowRecaptureOptProposalTransformer(State_);
            })
        , LogicalOptProposalTransformer_(
            [this] {
                return CreateYtflowLogicalOptProposalTransformer(State_);
            })
        , PhysicalOptProposalTransformer_(
            [this] {
                return CreateYtflowPhysicalOptProposalTransformer(State_);
            })
        , PhysicalFinalizingTransformer_(
            [this] {
                return CreateYtflowPhysicalFinalizingTransformer(State_);
            })
        , ExecTransformer_(
            [this] {
                return CreateYtflowDataSinkExecTransformer(State_);
            })
    {
    }

    TStringBuf GetName() const override {
        return YtflowProviderName;
    }

    bool CanParse(const TExprNode& node) override {
        return TypeAnnotationTransformer_->CanParse(node);
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSink::CallableName())) {
            if (!EnsureArgsCount(node, 2, ctx)) {
                return false;
            }

            if (node.Child(TYtflowDSink::idx_Category)->Content() == YtflowProviderName) {
                auto* clusterChild = node.Child(TYtflowDSink::idx_Cluster);
                if (!EnsureAtom(*clusterChild, ctx)) {
                    return false;
                }

                if (clusterChild->Content() != "$all") {
                    ctx.AddError(TIssue(
                        ctx.GetPosition(clusterChild->Pos()),
                        TStringBuilder()
                            << "Unknown cluster name: " << clusterChild->Content()));

                    return false;
                }

                cluster = Nothing();

                return true;
            }
        }

        ctx.AddError(TIssue(
            ctx.GetPosition(node.Pos()),
            "Invalid Ytflow DataSink parameters"));

        return false;
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool /*instantOnly*/) override {
        return *TypeAnnotationTransformer_;
    }

    void FillModifyCallables(THashSet<TStringBuf>& callables) override {
        // callables.insert(TYtflowWrite::CallableName());
        callables.insert(TYtflowPublish::CallableName());
    }

    IGraphTransformer& GetRecaptureOptProposalTransformer() override {
        return *RecaptureOptProposalTransformer_;
    }

    IGraphTransformer& GetLogicalOptProposalTransformer() override {
        return *LogicalOptProposalTransformer_;
    }

    IGraphTransformer& GetPhysicalOptProposalTransformer() override {
        return *PhysicalOptProposalTransformer_;
    }

    IGraphTransformer& GetPhysicalFinalizingTransformer() override {
        return *PhysicalFinalizingTransformer_;
    }

    bool CanExecute(const TExprNode& node) override {
        return ExecTransformer_->CanExec(node);
    }

    void GetRequiredChildren(const TExprNode& node, TExprNode::TListType& children) override {
        if (CanExecute(node)) {
            children.push_back(node.ChildPtr(0));
        }
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        return *ExecTransformer_;
    }

    bool HasCustomPlan(const TExprNode& node) override {
        return TMaybeNode<TYtflowOpBase>(&node) || TMaybeNode<TYtflowPublish>(&node);
    }

    void WriteDetails(const TExprNode& node, NYson::TYsonWriter& writer) override {
        Y_UNUSED(node, writer);
    }

    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool /*compact*/) override {
        bool hasDependencies = false;

        if (CanExecute(node)) {
            children.push_back(node.ChildPtr(0));
            hasDependencies = true;

            if (auto maybeYtflowOpBase = TMaybeNode<TYtflowOpBase>(&node)) {
                for (auto source: maybeYtflowOpBase.Cast().Sources()) {
                    if (auto maybeYtflowOutput = source.Maybe<TYtflowOutput>()) {
                        children.push_back(maybeYtflowOutput.Cast().Operation().Ptr());
                    }
                }
            }
        }

        return hasDependencies;
    }

    ui32 GetInputs(const TExprNode& node, TVector<TPinInfo>& inputs, bool withLimits) override {
        auto maybeYtflowOpBase = TMaybeNode<TYtflowOpBase>(&node);
        if (!maybeYtflowOpBase) {
            return 0;
        }

        ui32 inputCount = 0;

        auto addReadWrapInputs = [&](TExprBase readWrap) {
            auto providerInput = readWrap.Cast<TYtflowReadWrap>().Input();

            auto dataProvider = GetDataProvider(providerInput.Ref(), *State_->Types);
            YQL_ENSURE(dataProvider);
            inputCount += dataProvider->GetPlanFormatter().GetInputs(providerInput.Ref(), inputs, withLimits);
        };

        for (auto source: maybeYtflowOpBase.Cast().Sources()) {
            if (auto maybePersistentSource = source.Maybe<TYtflowPersistentSource>()) {
                addReadWrapInputs(maybePersistentSource.Cast().Input());
            }
        }

        if (auto maybeYtflowTransformMap = TMaybeNode<TYtflowTransformMap>(&node)) {
            const auto& lambda = maybeYtflowTransformMap.Cast().Lambda().Ref();
            VisitExpr(lambda, [&](const TExprNode& node) {
                if (auto maybeYtflowLookupJoin = TMaybeNode<TYtflowLookupJoin>(&node)) {
                    addReadWrapInputs(maybeYtflowLookupJoin.Cast().LookupSource());
                }

                return true;
            });
        }

        return inputCount;
    }

    ui32 GetOutputs(const TExprNode& node, TVector<TPinInfo>& outputs, bool withLimits) override {
        auto maybeYtflowOpBase = TMaybeNode<TYtflowOpBase>(&node);
        if (!maybeYtflowOpBase) {
            return 0;
        }

        ui32 outputCount = 0;
        for (auto sink: maybeYtflowOpBase.Cast().Sinks()) {
            if (auto maybePersistentSink = sink.Maybe<TYtflowPersistentSink>()) {
                auto writeWrap = maybePersistentSink.Cast().Input();
                auto providerInput = writeWrap.Cast<TYtflowWriteWrap>().Input();

                auto dataProvider = GetDataProvider(providerInput.Ref(), *State_->Types);
                YQL_ENSURE(dataProvider);

                ui32 providerOutputCount = dataProvider->GetPlanFormatter()
                    .GetOutputs(providerInput.Ref(), outputs, withLimits);

                // TODO(ngc224): drop fixup
                for (
                    ui32 outputIndex = outputCount;
                    outputIndex < outputCount + providerOutputCount;
                    ++outputIndex
                ) {
                    outputs[outputIndex].HideInBasicPlan = false;
                }

                outputCount += providerOutputCount;
            }
        }

        return outputCount;
    }

    void WritePinDetails(const TExprNode& node, NYson::TYsonWriter& writer) override {
        Y_UNUSED(node, writer);
    }

    TString GetOperationDisplayName(const TExprNode& node) override {
        return TString(node.Content());
    }

private:
    TYtflowState::TPtr State_;
    TLazyInitHolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
    TLazyInitHolder<IGraphTransformer> RecaptureOptProposalTransformer_;
    TLazyInitHolder<IGraphTransformer> LogicalOptProposalTransformer_;
    TLazyInitHolder<IGraphTransformer> PhysicalOptProposalTransformer_;
    TLazyInitHolder<IGraphTransformer> PhysicalFinalizingTransformer_;
    TLazyInitHolder<TExecTransformerBase> ExecTransformer_;
};


TIntrusivePtr<IDataProvider> CreateYtflowDataSink(TYtflowState::TPtr state) {
    return MakeIntrusive<TYtflowDataSink>(std::move(state));
}

} // namespace NYql
