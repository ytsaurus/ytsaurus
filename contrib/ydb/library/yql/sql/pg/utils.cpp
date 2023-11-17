#include "utils.h"
#include <contrib/ydb/library/yql/core/yql_expr_type_annotation.h>

namespace NSQLTranslationPG {

TString NormalizeName(TStringBuf name) {
    return NYql::NormalizeName(name);
}

}
