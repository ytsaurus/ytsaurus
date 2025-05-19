#include "parse_tree.h"

#include <util/generic/yexception.h>
#include <util/generic/ylimits.h>
#include <util/stream/output.h>
#include <util/system/compiler.h>

namespace NSQLComplete {

    antlr4::tree::ParseTree* EnclosingParseTree(
        antlr4::tree::ParseTree* root,
        antlr4::TokenStream* tokens,
        ssize_t cursorPosition) {
        if (root == nullptr) {
            return nullptr;
        }

        auto tokenIndexes = root->getSourceInterval();
        if (tokenIndexes.b == -1) {
            tokenIndexes.b = 0;
        }

        antlr4::misc::Interval interval(
            tokens->get(tokenIndexes.a)->getStartIndex(),
            tokens->get(tokenIndexes.b)->getStopIndex());

        if (cursorPosition < interval.a || interval.b < cursorPosition) {
            return nullptr;
        }

        for (antlr4::tree::ParseTree* child : root->children) {
            if (auto* ctx = EnclosingParseTree(
                    child,
                    tokens,
                    cursorPosition)) {
                return ctx;
            }
        }

        return root;
    }

} // namespace NSQLComplete
