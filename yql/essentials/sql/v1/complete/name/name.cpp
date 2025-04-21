#include "name.h"

#include <util/stream/output.h>

namespace {

    template <class T>
    void PrintNamespaced(IOutputStream& out, const T& name, const TStringBuf delim) {
        if (!name.Namespace.empty()) {
            out << name.Namespace << delim;
        }
        out << name.Indentifier;
    }

} // namespace

template <>
void Out<NSQLComplete::TKeyword>(IOutputStream& out, const NSQLComplete::TKeyword& name) {
    out << name.Content;
}

template <>
void Out<NSQLComplete::TPragmaName>(IOutputStream& out, const NSQLComplete::TPragmaName& name) {
    PrintNamespaced(out, name, ".");
}

template <>
void Out<NSQLComplete::TTypeName>(IOutputStream& out, const NSQLComplete::TTypeName& name) {
    out << name.Indentifier;
}

template <>
void Out<NSQLComplete::TFunctionName>(IOutputStream& out, const NSQLComplete::TFunctionName& name) {
    PrintNamespaced(out, name, "::");
}

template <>
void Out<NSQLComplete::THintName>(IOutputStream& out, const NSQLComplete::THintName& name) {
    out << name.Indentifier;
}

template <>
void Out<NSQLComplete::TFolderName>(IOutputStream& out, const NSQLComplete::TFolderName& name) {
    out << name.Indentifier;
}

template <>
void Out<NSQLComplete::TTableName>(IOutputStream& out, const NSQLComplete::TTableName& name) {
    out << name.Indentifier;
}

template <>
void Out<NSQLComplete::TGenericName>(IOutputStream& out, const NSQLComplete::TGenericName& name) {
    std::visit([&](auto&& name) { out << name; }, name);
}
