#pragma once

#include <yql/essentials/sql/v1/complete/core/statement.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>

namespace NSQLComplete {

    struct TNameSet {
        TVector<TString> Pragmas;
        TVector<TString> Types;
        TVector<TString> Functions;
        THashMap<EStatementKind, TVector<TString>> Hints;
    };

    // TODO(YQL-19747): Migrate YDB CLI
    using NameSet = TNameSet;

    TNameSet MakeDefaultNameSet();

} // namespace NSQLComplete
