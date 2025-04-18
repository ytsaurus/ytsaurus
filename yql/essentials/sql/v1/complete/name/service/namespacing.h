#pragma once

#include "name_service.h"

#include <util/generic/string.h>

namespace NSQLComplete {

    TPragmaName ParsePragma(const TStringBuf text);
    TFunctionName ParseFunction(const TStringBuf text);

} // namespace NSQLComplete
