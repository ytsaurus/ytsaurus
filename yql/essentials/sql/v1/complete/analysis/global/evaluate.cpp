#include "evaluate.h"

namespace NSQLComplete {

    namespace {

        class TVisitor: public SQLv1Antlr4BaseVisitor {
        public:
            explicit TVisitor(const TEnvironment* env)
                : Env_(env)
            {
            }

            std::any visitBind_parameter(SQLv1::Bind_parameterContext* ctx) override {
                std::string id = GetId(ctx);
                if (const TValue* value = Env_->Bindings.FindPtr(id)) {
                    return *value;
                }
                return {};
            }

        private:
            std::string GetId(SQLv1::Bind_parameterContext* ctx) const {
                if (auto* x = ctx->an_id_or_type()) {
                    return x->getText();
                } else if (auto* x = ctx->TOKEN_TRUE()) {
                    return x->getText();
                } else if (auto* x = ctx->TOKEN_FALSE()) {
                    return x->getText();
                } else {
                    Y_ABORT("You should change implementation according grammar changes");
                }
            }

            const TEnvironment* Env_;
        };

    } // namespace

    TMaybe<TValue> Evaluate(antlr4::ParserRuleContext* ctx, const TEnvironment& env) {
        std::any any = TVisitor(&env).visit(ctx);
        if (!any.has_value()) {
            return Nothing();
        }
        return std::any_cast<TValue>(any);
    }

} // namespace NSQLComplete
