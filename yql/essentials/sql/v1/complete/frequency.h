#pragma once

#include "sql_antlr4.h"

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

    struct TFrequencyData {
        THashMap<TString, size_t> Functions;
        THashMap<TString, size_t> Modules;
        THashMap<TParserCallStack, size_t> Stacks;
    };

    TFrequencyData LoadFrequencyData();

}
