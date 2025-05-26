#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

    bool CheckComplete(
        TStringBuf query,
        THashMap<TString, TVector<TString>> tablesByCluster,
        TString& error);

} // namespace NSQLComplete
