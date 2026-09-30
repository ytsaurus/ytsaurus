#pragma once

#include "../base/string_view.h"
#include "types.h"

#include <list>
#include <stack>
#include <string>

namespace clickhouse {

struct TypeAst {
    enum Meta {
        Array,
        Assign,
        Null,
        Nullable,
        Number,
        String,
        Terminal,
        Tuple,
        Enum,
        LowCardinality,
        SimpleAggregateFunction,
        Map
    };

    /// Type's category.
    Meta meta;
    Type::Code code;
    /// Type's name.
    /// Need to cache TypeAst, so can't use StringView for name.
    std::string name;
    /// Name of this element inside its parent (e.g. field name inside a named
    /// Tuple). Empty for unnamed elements.
    std::string element_name;
    /// Value associated with the node,
    /// used for fixed-width types and enum values.
    int64_t value = 0;
    std::string value_string;
    /// Subelements of the type.
    /// Used to store enum's names and values as well.
    std::vector<TypeAst> elements;

    bool operator==(const TypeAst & other) const;
    inline bool operator!=(const TypeAst & other) const {
        return !(*this == other);
    }
};


class TypeParser {

    struct Token {
        enum Type {
            Invalid = 0,
            Assign,
            Name,
            Number,
            String,
            LPar,
            RPar,
            Comma,
            QuotedString, // string with quotation marks included
            QuotedIdentifier,
            EOS,
        };

        Type type;
        StringView value;
    };

public:
    explicit TypeParser(const StringView& name);
    ~TypeParser();

    bool Parse(TypeAst* type);

private:
    Token NextToken();

private:
    const char* cur_;
    const char* end_;

    TypeAst* type_;
    std::stack<TypeAst*> open_elements_;
    // Backing storage for unescaped QuotedIdentifier token values. When a
    // quoted identifier contains escape sequences the unescaped content is
    // written here and the returned StringView points into this string.
    // Valid only until the next NextToken() call.
    std::string scratch_;
};


const TypeAst* ParseTypeName(const std::string& type_name);

}
