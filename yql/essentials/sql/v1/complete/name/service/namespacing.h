#pragma once

#include "name_service.h"

#include <util/generic/string.h>

namespace NSQLComplete {

    std::tuple<TStringBuf, TStringBuf> ParseNamespaced(const TStringBuf delim, const TStringBuf text);
    TPragmaName ParsePragma(const TStringBuf text);
    TFunctionName ParseFunction(const TStringBuf text);

} // namespace NSQLComplete
