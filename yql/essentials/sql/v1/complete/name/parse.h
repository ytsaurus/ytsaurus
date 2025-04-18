#pragma once

#include "name.h"

namespace NSQLComplete {

    TPragmaName ParsePragma(const TStringBuf text);
    TFunctionName ParseFunction(const TStringBuf text);

} // namespace NSQLComplete
