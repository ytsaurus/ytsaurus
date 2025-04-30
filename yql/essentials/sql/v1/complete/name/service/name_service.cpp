#include "name_service.h"

namespace NSQLComplete {

    namespace {

        void SetPrefix(TString& name, const TStringBuf delimeter, const TNamespaced& namespaced) {
            if (namespaced.Namespace.empty()) {
                return;
            }

            name.prepend(delimeter);
            name.prepend(namespaced.Namespace);
        }

        void FixPrefix(TString& name, const TStringBuf delimeter, const TNamespaced& namespaced) {
            if (namespaced.Namespace.empty()) {
                return;
            }

            name.remove(0, namespaced.Namespace.size() + delimeter.size());
        }

    } // namespace

    TPragmaName TNameConstraints::Qualified(TPragmaName unqualified) const {
        SetPrefix(unqualified.Indentifier, ".", *Pragma);
        return unqualified;
    }

    TPragmaName TNameConstraints::Unqualified(TPragmaName qualified) const {
        FixPrefix(qualified.Indentifier, ".", *Pragma);
        return qualified;
    }

    TFunctionName TNameConstraints::Qualified(TFunctionName unqualified) const {
        SetPrefix(unqualified.Indentifier, "::", *Function);
        return unqualified;
    }

    TFunctionName TNameConstraints::Unqualified(TFunctionName qualified) const {
        FixPrefix(qualified.Indentifier, "::", *Function);
        return qualified;
    }

    TGenericName TNameConstraints::Qualified(TGenericName unqualified) const {
        return std::visit([&](auto&& name) -> TGenericName {
            using T = std::decay_t<decltype(name)>;
            if constexpr (std::is_same_v<T, TPragmaName> ||
                          std::is_same_v<T, TFunctionName>) {
                return Qualified(std::move(name));
            }
            return name;
        }, std::move(unqualified));
    }

    TGenericName TNameConstraints::Unqualified(TGenericName qualified) const {
        return std::visit([&](auto&& name) -> TGenericName {
            using T = std::decay_t<decltype(name)>;
            if constexpr (std::is_same_v<T, TPragmaName> ||
                          std::is_same_v<T, TFunctionName>) {
                return Unqualified(std::move(name));
            }
            return name;
        }, std::move(qualified));
    }

    TVector<TGenericName> TNameConstraints::Qualified(TVector<TGenericName> unqualified) const {
        for (auto& name : unqualified) {
            name = Qualified(std::move(name));
        }
        return unqualified;
    }

    TVector<TGenericName> TNameConstraints::Unqualified(TVector<TGenericName> qualified) const {
        for (auto& name : qualified) {
            name = Unqualified(std::move(name));
        }
        return qualified;
    }

} // namespace NSQLComplete
