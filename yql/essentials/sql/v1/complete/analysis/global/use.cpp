#include "use.h"

namespace NSQLComplete {

    namespace {

        class TVisitor: public SQLv1Antlr4BaseVisitor {
        public:
            std::any visitSql_stmt_core(SQLv1::Sql_stmt_coreContext* ctx) override {
                if (IsFound()) {
                    return {};
                }
                return SQLv1Antlr4BaseVisitor::visitSql_stmt_core(ctx);
            }

            std::any visitUse_stmt(SQLv1::Use_stmtContext* ctx) override {
                SQLv1::Cluster_exprContext* expr = ctx->cluster_expr();
                if (!expr) {
                    return {};
                }

                Provider = "";
                Cluster = "";

                if (SQLv1::An_idContext* ctx = expr->an_id()) {
                    std::string text = ctx->getText();
                    Provider = std::move(text);
                }

                if (SQLv1::Pure_column_or_namedContext* ctx = expr->pure_column_or_named()) {
                    std::string text = ctx->getText();
                    Cluster = std::move(text);
                }

                return {};
            }

            bool IsFound() const {
                return !Cluster.empty();
            }

            TString Provider;
            TString Cluster;
        };

    } // namespace

    TMaybe<TUseContext> FindUseStatement(SQLv1::Sql_queryContext* ctx) {
        TVisitor visitor;
        visitor.visit(ctx);

        if (!visitor.IsFound()) {
            return Nothing();
        }

        return TUseContext{
            .Provider = std::move(visitor.Provider),
            .Cluster = std::move(visitor.Cluster),
        };
    }

} // namespace NSQLComplete
