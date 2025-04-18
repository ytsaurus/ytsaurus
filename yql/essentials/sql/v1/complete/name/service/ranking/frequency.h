#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NSQLComplete {

    struct TFrequencyData {
        THashMap<TString, size_t> Keywords;
        THashMap<TString, THashMap<TString, size_t>> PragmasBySpace;
        THashMap<TString, size_t> Types;
        THashMap<TString, THashMap<TString, size_t>> FunctionsBySpace;
        THashMap<TString, size_t> Hints;
    };

    TFrequencyData ParseJsonFrequencyData(const TStringBuf text);

    TFrequencyData LoadFrequencyData();

} // namespace NSQLComplete
