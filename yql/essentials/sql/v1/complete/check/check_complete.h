#pragma once

#include <util/generic/string.h>

namespace NSQLComplete {

    bool CheckComplete(TStringBuf query, TString& error);

} // namespace NSQLComplete
