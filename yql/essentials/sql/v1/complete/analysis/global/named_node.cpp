#include "named_node.h"

#include "narrowing_visitor.h"

#include <library/cpp/iterator/iterate_keys.h>
#include <library/cpp/iterator/iterate_values.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>

namespace NSQLComplete {

    namespace {

        class TVisitor: public TSQLv1NarrowingVisitor {
        public:
            TVisitor(
                antlr4::TokenStream* tokens,
                size_t cursorPosition,
                TNamedExpressions* exprs)
                : TSQLv1NarrowingVisitor(tokens, cursorPosition)
                , Exprs_(exprs)
            {
            }

            std::any visitSql_stmt_core(SQLv1::Sql_stmt_coreContext* ctx) override {
                if (ctx->declare_stmt() ||
                    ctx->import_stmt() ||
                    ctx->define_action_or_subquery_stmt() ||
                    ctx->named_nodes_stmt() ||
                    IsEnclosing(ctx)) {
                    return visitChildren(ctx);
                }
                return {};
            }

            std::any visitDeclare_stmt(SQLv1::Declare_stmtContext* ctx) override {
                Y_UNUSED(ctx, Exprs_);
                return {};
            }

            std::any visitImport_stmt(SQLv1::Import_stmtContext* ctx) override {
                Y_UNUSED(ctx, Exprs_);
                return {};
            }

            std::any visitDefine_action_or_subquery_stmt(
                SQLv1::Define_action_or_subquery_stmtContext* ctx) override {
                Y_UNUSED(ctx, Exprs_);
                return {};
            }

            std::any visitNamed_nodes_stmt(SQLv1::Named_nodes_stmtContext* ctx) override {
                Y_UNUSED(ctx, Exprs_);
                return {};
            }

            std::any visitFor_stmt(SQLv1::For_stmtContext* ctx) override {
                Y_UNUSED(ctx, Exprs_);
                return {};
            }

            std::any visitLambda(SQLv1::LambdaContext* ctx) override {
                Y_UNUSED(ctx, Exprs_);
                return {};
            }

        private:
            TNamedExpressions* Exprs_;
        };

    } // namespace

    TNamedExpressions CollectNamedNodes(
        SQLv1::Sql_queryContext* ctx,
        antlr4::TokenStream* tokens,
        size_t cursorPosition) {
        TNamedExpressions exprs;
        TVisitor(tokens, cursorPosition, &exprs).visit(ctx);
        return exprs;
    }

} // namespace NSQLComplete
