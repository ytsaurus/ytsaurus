#include "namespacing.h"

namespace NSQLComplete {

    void InsertNamespace(TString& name, const TStringBuf delimeter, const TNamespaced& namespaced) {
        if (!namespaced.Namespace.empty()) {
            name.prepend(delimeter);
            name.prepend(namespaced.Namespace);
        }
    }

    void InsertNamespace(TGenericName& name, const TNameRequest& request) {
        std::visit([&](auto& name) -> size_t {
            using T = std::decay_t<decltype(name)>;
            if constexpr (std::is_same_v<T, TPragmaName>) {
                InsertNamespace(name.Indentifier, ".", *request.Constraints.Pragma);
            }
            if constexpr (std::is_same_v<T, TFunctionName>) {
                InsertNamespace(name.Indentifier, "::", *request.Constraints.Function);
            }
            return 0;
        }, name);
    }

    void InsertNamespace(TVector<TGenericName>& names, const TNameRequest& request) {
        for (auto& name : names) {
            InsertNamespace(name, request);
        }
    }

    void RemoveNamespace(TString& name, const TStringBuf delimeter, const TNamespaced& namespaced) {
        if (namespaced.Namespace.empty()) {
            return;
        }
        name.remove(0, namespaced.Namespace.size() + delimeter.size());
    }

    void RemoveNamespace(TGenericName& name, const TNameRequest& request) {
        std::visit([&](auto& name) -> size_t {
            using T = std::decay_t<decltype(name)>;
            if constexpr (std::is_same_v<T, TPragmaName>) {
                RemoveNamespace(name.Indentifier, ".", *request.Constraints.Pragma);
            }
            if constexpr (std::is_same_v<T, TFunctionName>) {
                RemoveNamespace(name.Indentifier, "::", *request.Constraints.Function);
            }
            return 0;
        }, name);
    }

    void RemoveNamespace(TVector<TGenericName>& names, const TNameRequest& request) {
        for (auto& name : names) {
            RemoveNamespace(name, request);
        }
    }

} // namespace NSQLComplete
