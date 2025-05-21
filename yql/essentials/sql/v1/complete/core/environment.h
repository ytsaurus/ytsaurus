#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <library/cpp/case_insensitive_string/case_insensitive_string.h>

namespace NSQLComplete {

    using TValue = std::variant<TString>;

    struct TEnvironment {
        THashMap<TCaseInsensitiveString, TValue> Bindings;
    };

} // namespace NSQLComplete
