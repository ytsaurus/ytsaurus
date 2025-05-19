#include "parse_tree.h"

#include <util/generic/ylimits.h>
#include <util/stream/output.h>
#include <util/system/compiler.h>

namespace NSQLComplete {

    antlr4::ParserRuleContext* EnclosingParseTree(antlr4::ParserRuleContext* root, size_t cursorTokenIndex) {
        if (root == nullptr) {
            return nullptr;
        }

        size_t start = root->getStart()->getStartIndex();

        size_t stop = start;
        if (auto* token = root->getStop()) {
            stop = token->getStopIndex();
        }

        Cerr << "Checking " << cursorTokenIndex
             << " in [" << start << ", " << stop << "]" << Endl;

        if (cursorTokenIndex < start || stop < cursorTokenIndex) {
            return nullptr;
        }

        for (auto* child : root->children) {
            if (auto* ctx = EnclosingParseTree(
                    dynamic_cast<antlr4::ParserRuleContext*>(child),
                    cursorTokenIndex)) {
                return ctx;
            }
        }

        return root;
    }

} // namespace NSQLComplete
