#include "schema.h"

namespace NSQLComplete {

THashSet<TString> TFolderEntry::KnownTypes = {
    TFolderEntry::Folder,
    TFolderEntry::Table,
    TFolderEntry::View,
};

} // namespace NSQLComplete

template <>
void Out<NSQLComplete::TFolderEntry>(IOutputStream& out, const NSQLComplete::TFolderEntry& value) {
    out << "{" << value.Type << ", " << value.Name << "}";
}
