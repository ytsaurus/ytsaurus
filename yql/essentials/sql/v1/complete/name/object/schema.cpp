#include "schema.h"

namespace NSQLComplete {

    bool operator<(const TFolderEntry& lhs, const TFolderEntry& rhs) {
        return std::tie(lhs.Type, lhs.Name) < std::tie(rhs.Type, rhs.Name);
    }

} // namespace NSQLComplete

template <>
void Out<NSQLComplete::TFolderEntry>(IOutputStream& out, const NSQLComplete::TFolderEntry& value) {
    out << "{" << value.Type << ", " << value.Name << "}";
}
