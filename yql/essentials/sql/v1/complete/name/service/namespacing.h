#pragma once

#include "name_service.h"

#include <util/generic/string.h>

namespace NSQLComplete {

    void InsertNamespace(TString& name, const TStringBuf delimeter, const TNamespaced& namespaced);
    void InsertNamespace(TGenericName& name, const TNameRequest& request);
    void InsertNamespace(TVector<TGenericName>& names, const TNameRequest& request);

    void RemoveNamespace(TString& name, const TStringBuf delimeter, const TNamespaced& namespaced);
    void RemoveNamespace(TGenericName& name, const TNameRequest& request);
    void RemoveNamespace(TVector<TGenericName>& names, const TNameRequest& request);

} // namespace NSQLComplete
