#pragma once

#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>

namespace NSQLComplete {

    struct NameSet {
        TVector<TString> Pragmas;
        TVector<TString> Types;
        TVector<TString> Functions;
        THashMap<EStatementKind, TVector<TString>> Hints;
        TVector<TString> Tables;
    };

    NameSet MakeDefaultNameSet();

} // namespace NSQLComplete
