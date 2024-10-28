#include "helpers.h"

#include <yt/yt/client/table_client/row_base.h>

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
    const static TConstantTypeResolver DoubleResolver(EValueType::Double);
    const static TConstantTypeResolver Int64Resolver(EValueType::Int64);
    const static TConstantTypeResolver Uint64Resolver(EValueType::Uint64);
    const static TConstantTypeResolver BooleanResolver(EValueType::Boolean);
    const static TConstantTypeResolver StringResolver(EValueType::String);
    const static TConstantTypeResolver AnyResolver(EValueType::Any);

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

const TString& GetYsonExtractFunction(EValueType type)
{
    const static TString TryGetAny("try_get_any");
    const static TString TryGetDouble("try_get_double");
    const static TString TryGetInt64("try_get_int64");
    const static TString TryGetUint64("try_get_uint64");
    const static TString TryGetBoolean("try_get_boolean");
    const static TString TryGetString("try_get_string");

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

} // namespace NYT::NOrm::NQuery
