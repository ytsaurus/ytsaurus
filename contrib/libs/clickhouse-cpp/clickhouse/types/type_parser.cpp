#include "type_parser.h"

#include "clickhouse/exceptions.h"
#include "clickhouse/base/platform.h" // for _win_

#include <algorithm>
#include <cmath>
#include <map>
#include <mutex>
#include <unordered_map>

#if defined _win_
#include <string.h>
#else
#include <strings.h>
#endif


namespace clickhouse {

bool TypeAst::operator==(const TypeAst & other) const {
    return meta == other.meta
        && code == other.code
        && name == other.name
        && element_name == other.element_name
        && value == other.value
        && value_string == other.value_string
        && std::equal(elements.begin(), elements.end(), other.elements.begin(), other.elements.end());
}

static const std::unordered_map<std::string, Type::Code> kTypeCode = {
    { "Void",        Type::Void },
    { "Int8",        Type::Int8 },
    { "Int16",       Type::Int16 },
    { "Int32",       Type::Int32 },
    { "Int64",       Type::Int64 },
#if CH_MAP_BOOL_TO_UINT8
    { "Bool",        Type::UInt8 },
#else
    { "Bool",        Type::Bool },
#endif
    { "UInt8",       Type::UInt8 },
    { "UInt16",      Type::UInt16 },
    { "UInt32",      Type::UInt32 },
    { "UInt64",      Type::UInt64 },
    { "Float32",     Type::Float32 },
    { "Float64",     Type::Float64 },
    { "String",      Type::String },
    { "FixedString", Type::FixedString },
    { "DateTime",    Type::DateTime },
    { "DateTime64",  Type::DateTime64 },
    { "Date",        Type::Date },
    { "Date32",      Type::Date32 },
    { "Array",       Type::Array },
    { "Nullable",    Type::Nullable },
    { "Tuple",       Type::Tuple },
    { "Enum8",       Type::Enum8 },
    { "Enum16",      Type::Enum16 },
    { "UUID",        Type::UUID },
    { "IPv4",        Type::IPv4 },
    { "IPv6",        Type::IPv6 },
    { "Int128",      Type::Int128 },
    { "UInt128",     Type::UInt128 },
    { "Decimal",     Type::Decimal },
    { "Decimal32",   Type::Decimal32 },
    { "Decimal64",   Type::Decimal64 },
    { "Decimal128",  Type::Decimal128 },
    { "LowCardinality", Type::LowCardinality },
    { "Map",         Type::Map },
    { "Point",       Type::Point },
    { "Ring",        Type::Ring },
    { "Polygon",     Type::Polygon },
    { "MultiPolygon", Type::MultiPolygon },
    { "Time",        Type::Time },
    { "Time64",      Type::Time64 },
    { "JSON",        Type::JSON },
};

template <typename L, typename R>
inline int CompateStringsCaseInsensitive(const L& left, const R& right) {
    int64_t size_diff = left.size() - right.size();
    if (size_diff != 0)
        return size_diff > 0 ? 1 : -1;

#if defined _win_
    return _strnicmp(left.data(), right.data(), left.size());
#else
    return strncasecmp(left.data(), right.data(), left.size());
#endif
}

static Type::Code GetTypeCode(const std::string& name) {
    auto it = kTypeCode.find(name);
    if (it != kTypeCode.end()) {
        return it->second;
    }

    return Type::Void;
}

static TypeAst::Meta GetTypeMeta(const StringView& name) {
    if (name == "Array") {
        return TypeAst::Array;
    }

    if (name == "Null") {
        return TypeAst::Null;
    }

    if (name == "Nullable") {
        return TypeAst::Nullable;
    }

    if (name == "Tuple") {
        return TypeAst::Tuple;
    }

    if (name == "Enum8" || name == "Enum16") {
        return TypeAst::Enum;
    }

    if (name == "LowCardinality") {
        return TypeAst::LowCardinality;
    }

    if (name == "SimpleAggregateFunction") {
        return TypeAst::SimpleAggregateFunction;
    }

    if (name == "Map") {
        return TypeAst::Map;
    }

    return TypeAst::Terminal;
}

bool ValidateAST(const TypeAst& ast) {
    // Void terminal that is not actually "void" produced when unknown type is encountered.
    if (ast.meta == TypeAst::Terminal
            && ast.code == Type::Void
            && CompateStringsCaseInsensitive(ast.name, std::string_view("void")) != 0)
        //throw UnimplementedError("Unsupported type: " + ast.name);
        return false;

    return true;
}


TypeParser::TypeParser(const StringView& name)
    : cur_(name.data())
    , end_(name.data() + name.size())
    , type_(nullptr)
{
}

TypeParser::~TypeParser() = default;

bool TypeParser::Parse(TypeAst* type) {
    type_ = type;
    open_elements_.push(type_);

    size_t processed_tokens = 0;
    do {
        const Token & token = NextToken();
        switch (token.type) {
            case Token::QuotedString:
            {
                type_->meta = TypeAst::Terminal;
                if (token.value.length() < 1)
                    type_->value_string = {};
                else
                    type_->value_string = token.value.substr(1, token.value.length() - 2).to_string();
                type_->code = Type::String;
                break;
            }
            case Token::QuotedIdentifier:
            case Token::Name:
                if (!type_->name.empty()) {
                    // A second Name token on the same element means the
                    // previous one was a field name in a named-tuple element
                    // (e.g. "a" in "Tuple(a Int32, …)").
                    type_->element_name = std::move(type_->name);
                }
                type_->meta = GetTypeMeta(token.value);
                type_->name = token.value.to_string();
                type_->code = GetTypeCode(type_->name);
                break;
            case Token::Number:
                type_->meta = TypeAst::Number;
                type_->value = std::stol(token.value.to_string());
                break;
            case Token::String:
                type_->meta = TypeAst::String;
                type_->value_string = std::string(token.value);
                break;
            case Token::LPar:
                type_->elements.emplace_back(TypeAst());
                open_elements_.push(type_);
                type_ = &type_->elements.back();
                break;
            case Token::RPar:
                type_ = open_elements_.top();
                open_elements_.pop();
                break;
            case Token::Assign:
            case Token::Comma:
                type_ = open_elements_.top();
                open_elements_.pop();
                type_->elements.emplace_back(TypeAst());
                open_elements_.push(type_);
                type_ = &type_->elements.back();
                break;
            case Token::EOS:
            {
                // Ubalanced braces, brackets, etc is an error.
                if (open_elements_.size() != 1)
                    return false;

                // Empty input string, no tokens produced
                if (processed_tokens == 0)
                    return false;

                return ValidateAST(*type);
            }
            case Token::Invalid:
                return false;
        }
        ++processed_tokens;
    } while (true);
}

TypeParser::Token TypeParser::NextToken() {
    for (; cur_ < end_; ++cur_) {
        switch (*cur_) {
            case ' ':
            case '\n':
            case '\t':
            case '\0':
                continue;
            case '=':
                return Token{Token::Assign, StringView(cur_++, 1)};
            case '(':
                return Token{Token::LPar, StringView(cur_++, 1)};
            case ')':
                return Token{Token::RPar, StringView(cur_++, 1)};
            case ',':
                return Token{Token::Comma, StringView(cur_++, 1)};
            case '\'':
            {
                const auto end_quote_length = 1;
                const StringView end_quote{cur_, end_quote_length};
                // Fast forward to the closing quote.
                const auto start = cur_++;
                for (; cur_ < end_ - end_quote_length; ++cur_) {
                    // TODO (nemkov): handle escaping ?
                    if (end_quote == StringView{cur_, end_quote_length}) {
                        cur_ += end_quote_length;

                        return Token{Token::QuotedString, StringView{start, cur_}};
                    }
                }
                return Token{Token::QuotedString, StringView(cur_++, 1)};
            }
            case '"':
            case '`':
            {
                const auto quote = *cur_;
                ++cur_;
                // Two escape forms are recognised, both quote-specific (e.g.
                // inside a backtick-quoted identifier only backtick escapes
                // apply; a doubled double-quote is treated as two literals):
                //   \q  – backslash followed by the opening quote character
                //   qq  – two consecutive opening quote characters
                scratch_.clear();
                for (; cur_ < end_; ++cur_) {
                    if (*cur_ == '\\' && cur_ + 1 < end_ && *(cur_ + 1) == quote) {
                        scratch_ += quote;
                        ++cur_;
                    } else if (*cur_ == quote) {
                        if (cur_ + 1 < end_ && *(cur_ + 1) == quote) {
                            scratch_ += quote;
                            ++cur_;
                        } else {
                            ++cur_;
                            return Token{Token::QuotedIdentifier, StringView{scratch_}};
                        }
                    } else {
                        scratch_ += *cur_;
                    }
                }
                return Token{Token::Invalid, StringView()};
            }

            default: {
                const char* st = cur_;

                if (*cur_ == '\'') {
                    for (st = ++cur_; cur_ < end_; ++cur_) {
                        if (*cur_ == '\'') {
                            return Token{Token::String, StringView(st, cur_++ - st)};
                        }
                    }

                    return Token{Token::Invalid, StringView()};
                }

                if (isalpha(*cur_) || *cur_ == '_') {
                    for (; cur_ < end_; ++cur_) {
                        if (!isalpha(*cur_) && !isdigit(*cur_) && *cur_ != '_') {
                            break;
                        }
                    }

                    return Token{Token::Name, StringView(st, cur_)};
                }

                if (isdigit(*cur_) || *cur_ == '-') {
                    for (++cur_; cur_ < end_; ++cur_) {
                        if (!isdigit(*cur_)) {
                            break;
                        }
                    }

                    return Token{Token::Number, StringView(st, cur_)};
                }

                return Token{Token::Invalid, StringView()};
            }
        }
    }

    return Token{Token::EOS, StringView()};
}


const TypeAst* ParseTypeName(const std::string& type_name) {
    // Cache for type_name.
    // Usually we won't have too many type names in the cache, so do not try to
    // limit cache size.
    static std::map<std::string, TypeAst> ast_cache;
    static std::mutex lock;

    std::lock_guard<std::mutex> guard(lock);
    auto it = ast_cache.find(type_name);
    if (it != ast_cache.end()) {
        return &it->second;
    }

    auto& ast = ast_cache[type_name];
    if (TypeParser(type_name).Parse(&ast)) {
        return &ast;
    }
    ast_cache.erase(type_name);
    return nullptr;
}

}
