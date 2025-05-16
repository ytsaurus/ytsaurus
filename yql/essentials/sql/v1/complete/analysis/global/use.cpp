#include "use.h"

namespace NSQLComplete {

    namespace {

        class Listener: public SQLv1Antlr4BaseListener {
        public:
            void exitSql_stmt_core(SQLv1::Sql_stmt_coreContext* /* ctx */) override {
                if (IsFound()) {
                    return;
                }
            }

            void exitUse_stmt(SQLv1::Use_stmtContext* ctx) override {
                SQLv1::Cluster_exprContext* expr = ctx->cluster_expr();
                if (!expr) {
                    return;
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
            }

            bool IsFound() const {
                return !Cluster.empty();
            }

            TString Provider;
            TString Cluster;
        };

    } // namespace

    TMaybe<TUseContext> FindUseStatement(SQLv1::Sql_queryContext* ctx) {
        Listener listener;
        antlr4::tree::ParseTreeWalker::DEFAULT.walk(&listener, ctx);

        if (!listener.IsFound()) {
            return Nothing();
        }

        return TUseContext{
            .Provider = std::move(listener.Provider),
            .Cluster = std::move(listener.Cluster),
        };
    }

} // namespace NSQLComplete
