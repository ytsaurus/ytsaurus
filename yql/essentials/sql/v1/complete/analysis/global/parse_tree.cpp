#include "parse_tree.h"

#include <util/system/yassert.h>

namespace NSQLComplete {

    std::string GetId(SQLv1::Bind_parameterContext* ctx) {
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

} // namespace NSQLComplete
