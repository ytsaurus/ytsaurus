#include "lexer.h"

#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

#include <util/generic/string.h>
#include <util/string/subst.h>

namespace NSQLTranslationV1 {

    NSQLTranslation::ILexer::TPtr MakeRegexLexer(bool /* ansi */) {
        return nullptr;
    }

} // namespace NSQLTranslationV1
