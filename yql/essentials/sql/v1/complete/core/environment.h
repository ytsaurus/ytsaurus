#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NSQLComplete {

    using TValue = TString;

    struct TEnvironment {
        THashMap<TString, TValue> Bindings;
    };

}
