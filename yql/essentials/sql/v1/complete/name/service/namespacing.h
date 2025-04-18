#pragma once

#include "name_service.h"

#include <util/generic/string.h>

namespace NSQLComplete {

    std::tuple<TStringBuf, TStringBuf> ParseNamespaced(const TStringBuf delim, const TStringBuf text);
    TPragmaName ParsePragma(const TStringBuf text);
    TFunctionName ParseFunction(const TStringBuf text);

    void InsertNamespace(TString& name, const TStringBuf delimeter, const TNamespaced& namespaced);
    void InsertNamespace(TGenericName& name, const TNameRequest& request);
    void InsertNamespace(TVector<TGenericName>& names, const TNameRequest& request);

    void RemoveNamespace(TString& name, const TStringBuf delimeter, const TNamespaced& namespaced);
    void RemoveNamespace(TGenericName& name, const TNameRequest& request);
    void RemoveNamespace(TVector<TGenericName>& names, const TNameRequest& request);

} // namespace NSQLComplete
