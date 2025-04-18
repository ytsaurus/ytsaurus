#include "parse.h"

namespace NSQLComplete {

    std::tuple<TStringBuf, TStringBuf> ParseNamespaced(const TStringBuf delim, const TStringBuf text) {
        TStringBuf space, name;
        text.Split(delim, space, name);
        if (name.empty()) {
            name = space;
            space = "";
        }
        return {space, name};
    }

    TPragmaName ParsePragma(const TStringBuf text) {
        auto [space, name] = ParseNamespaced(".", text);

        TPragmaName pragma;
        pragma.Namespace = space;
        pragma.Indentifier = name;

        return pragma;
    }

    TFunctionName ParseFunction(const TStringBuf text) {
        auto [space, name] = ParseNamespaced("::", text);

        TFunctionName pragma;
        pragma.Namespace = space;
        pragma.Indentifier = name;

        return pragma;
    }

} // namespace NSQLComplete
