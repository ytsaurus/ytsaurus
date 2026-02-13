#include "helpers.h"

#include <yt/yt/client/table_client/row_base.h>

#include <yt/yt/library/query/base/query_preparer.h>

namespace NYT::NOrm::NQuery {

using NTableClient::EValueType;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TConstantTypeResolver
    : public ITypeResolver
{
public:
    constexpr explicit TConstantTypeResolver(NTableClient::EValueType type)
       : Type_(type)
    { }

    NTableClient::EValueType ResolveType(NYPath::TYPathBuf suffixPath = {}) const override
    {
        if (!suffixPath.Empty()) {
            if (Type_ != EValueType::Any) {
                THROW_ERROR_EXCEPTION(
                    "Attribute path of type %Qlv does not support nested attributes",
                    Type_);
            }
        }

        return Type_;
    }

private:
    NTableClient::EValueType Type_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

const ITypeResolver* GetTypeResolver(NTableClient::EValueType type)
{
    static const TConstantTypeResolver DoubleResolver(EValueType::Double);
    static const TConstantTypeResolver Int64Resolver(EValueType::Int64);
    static const TConstantTypeResolver Uint64Resolver(EValueType::Uint64);
    static const TConstantTypeResolver BooleanResolver(EValueType::Boolean);
    static const TConstantTypeResolver StringResolver(EValueType::String);
    static const TConstantTypeResolver AnyResolver(EValueType::Any);

    switch (type) {
        case EValueType::Double:
            return &DoubleResolver;
        case EValueType::Int64:
            return &Int64Resolver;
        case EValueType::Uint64:
            return &Uint64Resolver;
        case EValueType::Boolean:
            return &BooleanResolver;
        case EValueType::String:
            return &StringResolver;
        case EValueType::Any:
            return &AnyResolver;
        case EValueType::Max:
        case EValueType::Min:
        case EValueType::Null:
        case EValueType::Composite:
        case EValueType::TheBottom:
            THROW_ERROR_EXCEPTION("Attribute type %Qlv is not supported",
                type);
    }
}

const std::string& GetYsonExtractFunction(EValueType type)
{
    static const std::string TryGetAny("try_get_any");
    static const std::string TryGetDouble("try_get_double");
    static const std::string TryGetInt64("try_get_int64");
    static const std::string TryGetUint64("try_get_uint64");
    static const std::string TryGetBoolean("try_get_boolean");
    static const std::string TryGetString("try_get_string");

    switch (type) {
        case EValueType::Double:
            return TryGetDouble;
        case EValueType::Int64:
            return TryGetInt64;
        case EValueType::Uint64:
            return TryGetUint64;
        case EValueType::Boolean:
            return TryGetBoolean;
        case EValueType::String:
            return TryGetString;
        case EValueType::Composite:
        case EValueType::Null:
        case EValueType::Any:
            return TryGetAny;
        case EValueType::Min:
        case EValueType::TheBottom:
        case EValueType::Max:
            THROW_ERROR_EXCEPTION("Unable to provide extract function for type %Qv",
                type);
    }
}

////////////////////////////////////////////////////////////////////////////////

TExpressionParsedSource::TExpressionParsedSource(std::string source)
    : Source(std::move(source))
{
    auto parsedSource = NQueryClient::ParseSource(Source, NQueryClient::EParseMode::Expression);
    auto expressionPtr = std::get_if<NQueryClient::NAst::TExpressionPtr>(&parsedSource->AstHead.Ast);
    YT_VERIFY(expressionPtr);
    Expression = *expressionPtr;
    Merge(std::move(parsedSource->AstHead));
}
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
