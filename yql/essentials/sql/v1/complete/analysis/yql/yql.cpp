#include "yql.h"

#include "cluster.h"
#include "table.h"

#include <library/cpp/iterator/iterate_keys.h>

namespace NSQLComplete {

    THashSet<TString> TYqlContext::Clusters() const {
        auto keys = IterateKeys(TablesByCluster);
        return {keys.begin(), keys.end()};
    }

    TMaybe<TYqlContext> IYqlAnalysis::Analyze(NYql::TAstNode& root, NYql::TIssues& issues) const {
        NYql::TExprContext ctx;
        NYql::TExprNode::TPtr expr;
        if (!NYql::CompileExpr(root, expr, ctx, /* resolver = */ nullptr, /* urlListerManager = */ nullptr)) {
            for (NYql::TIssue issue : ctx.IssueManager.GetIssues()) {
                issues.AddIssue(std::move(issue));
            }
            return Nothing();
        }
        return Analyze(expr, ctx);
    }

    namespace {

        class TYqlAnalysis: public IYqlAnalysis {
        public:
            TYqlContext Analyze(NYql::TExprNode::TPtr root, NYql::TExprContext& ctx) const override {
                Y_UNUSED(ctx);

                TYqlContext yqlCtx;

                yqlCtx.TablesByCluster = CollectTablesByCluster(*root);

                for (TString cluster : CollectClusters(*root)) {
                    Y_UNUSED(yqlCtx.TablesByCluster[std::move(cluster)]);
                }

                return yqlCtx;
            }

        private:
            static void Print(IOutputStream& out, const NYql::TExprNode& root, NYql::TExprContext& ctx) {
                auto ast = ConvertToAst(root, ctx, NYql::TExprAnnotationFlags::None, true);
                ast.Root->PrettyPrintTo(out, NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote);
            }
        };

    } // namespace

    IYqlAnalysis::TPtr MakeYqlAnalysis() {
        return new TYqlAnalysis();
    }

} // namespace NSQLComplete
