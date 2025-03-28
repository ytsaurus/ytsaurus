#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

    struct TFrequencyData {
        THashMap<TString, size_t> Functions;
    };

    TFrequencyData LoadFrequencyData();

}
