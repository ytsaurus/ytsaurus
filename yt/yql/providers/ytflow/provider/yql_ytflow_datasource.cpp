#include "yql_ytflow_provider_impl.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/providers/common/config/transformer/yql_configuration_transformer.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/transform/yql_lazy_init.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>

#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/string/builder.h>


namespace NYql {

using namespace NNodes;


class TYtflowDataSource: public TDataProviderBase {
public:
    TYtflowDataSource(TYtflowState::TPtr state)
        : State_(std::move(state))
        , ConfigurationTransformer_(
            [this] {
                return MakeHolder<NCommon::TProviderConfigurationTransformer>(
                    State_->Configuration, *State_->Types,
                    TString(YtflowProviderName), THashSet<TStringBuf> {TCoConfigure::CallableName()}
                );
            })
        , TypeAnnotationTransformer_(
            [this] {
                return CreateYtflowDataSourceTypeAnnotationTransformer(State_);
            })
        , ConstraintTransformer_(
            [this] {
                return CreateYtflowDataSourceConstraintTransformer(State_);
            })
        , ExecTransformer_(
            [this] {
                return CreateYtflowDataSourceExecTransformer(State_);
            })
    {
    }

    bool Initialize(TExprContext& ctx) override {
        auto category = TString(YtflowProviderName);

        auto credential = State_->Types->Credentials->FindCredential(
            TString("default_").append(category));

        if (credential) {
            if (credential->Category != category) {
                ctx.AddError(TIssue({}, TStringBuilder()
                    << "Mismatch default credential category, expected: " << category
                    << ", but found: " << credential->Category));

                return false;
            }

            State_->Configuration->Auth = credential->Content;
        }

        return true;
    }

    TStringBuf GetName() const override {
        return YtflowProviderName;
    }

    IGraphTransformer& GetConfigurationTransformer() override {
        return *ConfigurationTransformer_;
    }

    bool CanParse(const TExprNode& node) override {
        return TypeAnnotationTransformer_->CanParse(node);
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSource::CallableName())) {
            if (!EnsureArgsCount(node, 2, ctx)) {
                return false;
            }

            if (node.Child(TYtflowDSource::idx_Category)->Content() == YtflowProviderName) {
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
            "Invalid Ytflow DataSource parameters"));

        return false;
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool /*instantOnly*/) override {
        return *TypeAnnotationTransformer_;
    }

    IGraphTransformer& GetConstraintTransformer(bool /*instantOnly*/, bool /*subGraph*/) override {
        return *ConstraintTransformer_;
    }

    bool CanExecute(const TExprNode& node) override {
        return ExecTransformer_->CanExec(node);
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        return *ExecTransformer_;
    }

private:
    TYtflowState::TPtr State_;

    TLazyInitHolder<IGraphTransformer> ConfigurationTransformer_;
    TLazyInitHolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
    TLazyInitHolder<IGraphTransformer> ConstraintTransformer_;
    TLazyInitHolder<TExecTransformerBase> ExecTransformer_;
};

TIntrusivePtr<IDataProvider> CreateYtflowDataSource(TYtflowState::TPtr state) {
    return MakeIntrusive<TYtflowDataSource>(std::move(state));
}

} // namespace NYql
