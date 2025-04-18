#pragma once

#include <yql/essentials/sql/v1/complete/core/statement.h>

#include <util/generic/string.h>

namespace NSQLComplete {

    struct TIndentifier {
        TString Indentifier;
    };

    struct TNamespaced {
        TString Namespace;
    };

    struct TKeyword {
        TString Content;
    };

    struct TPragmaName: TIndentifier, TNamespaced {
        struct TConstraints: TNamespaced {};
    };

    struct TTypeName: TIndentifier {
        struct TConstraints {};
    };

    struct TFunctionName: TIndentifier, TNamespaced {
        struct TConstraints: TNamespaced {};
    };

    struct THintName: TIndentifier {
        struct TConstraints {
            EStatementKind Statement;
        };
    };

    struct TTableName: TIndentifier {
        struct TConstraints {};
    };

    using TGenericName = std::variant<
        TKeyword,
        TPragmaName,
        TTypeName,
        TFunctionName,
        THintName,
        TTableName>;

} // namespace NSQLComplete
