#include "type_translation.h"

#include <yt/ytlib/table_client/schema.h>

#include <yt/client/table_client/row_base.h>
#include <yt/client/table_client/logical_type.h>

#include <yt/core/misc/error.h>

#include <util/generic/hash.h>

#include <DataTypes/DataTypeNullable.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

// YT native types

NTableClient::TLogicalTypePtr AdaptTypeToClickHouse(const NTableClient::TLogicalTypePtr& valueType)
{
    switch (valueType->GetMetatype()) {
        case ELogicalMetatype::Simple: {
            auto simpleLogicalType = valueType->AsSimpleTypeRef().GetElement();
            switch (simpleLogicalType) {
                case ESimpleLogicalValueType::Int64:
                    return SimpleLogicalType(ESimpleLogicalValueType::Int64);
                case ESimpleLogicalValueType::Int32:
                    return SimpleLogicalType(ESimpleLogicalValueType::Int32);
                case ESimpleLogicalValueType::Int16:
                    return SimpleLogicalType(ESimpleLogicalValueType::Int16);
                case ESimpleLogicalValueType::Int8:
                    return SimpleLogicalType(ESimpleLogicalValueType::Int8);

                case ESimpleLogicalValueType::Uint64:
                    return SimpleLogicalType(ESimpleLogicalValueType::Uint64);
                case ESimpleLogicalValueType::Uint32:
                    return SimpleLogicalType(ESimpleLogicalValueType::Uint32);
                case ESimpleLogicalValueType::Uint16:
                    return SimpleLogicalType(ESimpleLogicalValueType::Uint16);
                case ESimpleLogicalValueType::Uint8:
                    return SimpleLogicalType(ESimpleLogicalValueType::Uint8);

                case ESimpleLogicalValueType::Double:
                    return SimpleLogicalType(ESimpleLogicalValueType::Double);

                case ESimpleLogicalValueType::Boolean:
                    return SimpleLogicalType(ESimpleLogicalValueType::Boolean);

                case ESimpleLogicalValueType::String:
                case ESimpleLogicalValueType::Utf8:
                    return SimpleLogicalType(ESimpleLogicalValueType::String);

                case ESimpleLogicalValueType::Any:
                    return SimpleLogicalType(ESimpleLogicalValueType::Any);

                case ESimpleLogicalValueType::Date:
                    return SimpleLogicalType(ESimpleLogicalValueType::Date);

                case ESimpleLogicalValueType::Datetime:
                    return SimpleLogicalType(ESimpleLogicalValueType::Datetime);

                case ESimpleLogicalValueType::Timestamp:
                    // TODO(dakovalkov): https://github.com/yandex/ClickHouse/pull/7170.
                    // return SimpleLogicalType(ESimpleLogicalValueType::Timestamp);
                    return SimpleLogicalType(ESimpleLogicalValueType::Uint64);

                case ESimpleLogicalValueType::Interval:
                    return SimpleLogicalType(ESimpleLogicalValueType::Int64);

                default:
                    THROW_ERROR_EXCEPTION("YT value type %Qlv not supported", simpleLogicalType);
            };
        }
        case ELogicalMetatype::Optional:
            return OptionalLogicalType(AdaptTypeToClickHouse(valueType->GetElement()));
        case ELogicalMetatype::List:
        case ELogicalMetatype::Struct:
        case ELogicalMetatype::Tuple:
        case ELogicalMetatype::VariantStruct:
        case ELogicalMetatype::VariantTuple:
        case ELogicalMetatype::Dict:
        case ELogicalMetatype::Tagged:
            return SimpleLogicalType(ESimpleLogicalValueType::Any);
        default:
            THROW_ERROR_EXCEPTION("YT value metatype %Qlv not supported", valueType->GetMetatype());
    };
}

bool IsYtTypeSupported(const TLogicalTypePtr& valueType)
{
    if (valueType->GetMetatype() == ELogicalMetatype::Optional) {
        return IsYtTypeSupported(valueType->GetElement());
    } else if (valueType->GetMetatype() == ELogicalMetatype::Simple) {
        switch (valueType->AsSimpleTypeRef().GetElement()) {
            case ESimpleLogicalValueType::Int64:
            case ESimpleLogicalValueType::Uint64:
            case ESimpleLogicalValueType::Double:
            case ESimpleLogicalValueType::Boolean:

            case ESimpleLogicalValueType::String:
            case ESimpleLogicalValueType::Any:

            case ESimpleLogicalValueType::Int8:
            case ESimpleLogicalValueType::Uint8:

            case ESimpleLogicalValueType::Int16:
            case ESimpleLogicalValueType::Uint16:

            case ESimpleLogicalValueType::Int32:
            case ESimpleLogicalValueType::Uint32:

            case ESimpleLogicalValueType::Utf8:

            case ESimpleLogicalValueType::Date:
            case ESimpleLogicalValueType::Datetime:
            case ESimpleLogicalValueType::Timestamp:
            case ESimpleLogicalValueType::Interval:
                return true;
            default:
                return false;
        };
    } else {
        // "Any" type.
        return true;
    }
}

EClickHouseColumnType RepresentYtType(const TLogicalTypePtr& valueType)
{
    if (valueType->GetMetatype() == ELogicalMetatype::Optional) {
        return RepresentYtType(valueType->GetElement());
    } else if (valueType->GetMetatype() == ELogicalMetatype::Simple) {
        auto simpleLogicalType = valueType->AsSimpleTypeRef().GetElement();
        switch (simpleLogicalType) {
            case ESimpleLogicalValueType::Int64:
                return EClickHouseColumnType::Int64;

            case ESimpleLogicalValueType::Int32:
                return EClickHouseColumnType::Int32;

            case ESimpleLogicalValueType::Int16:
                return EClickHouseColumnType::Int16;

            case ESimpleLogicalValueType::Int8:
                return EClickHouseColumnType::Int8;

            case ESimpleLogicalValueType::Uint64:
                return EClickHouseColumnType::UInt64;

            case ESimpleLogicalValueType::Uint32:
                return EClickHouseColumnType::UInt32;

            case ESimpleLogicalValueType::Uint16:
                return EClickHouseColumnType::UInt16;

            case ESimpleLogicalValueType::Uint8:
                return EClickHouseColumnType::UInt8;

            case ESimpleLogicalValueType::Double:
                return EClickHouseColumnType::Double;

            case ESimpleLogicalValueType::Boolean:
                return EClickHouseColumnType::Boolean;

            case ESimpleLogicalValueType::String:
            case ESimpleLogicalValueType::Any:
                return EClickHouseColumnType::String;

            case ESimpleLogicalValueType::Date:
                return EClickHouseColumnType::Date;

            case ESimpleLogicalValueType::Datetime:
                return EClickHouseColumnType::DateTime;

            case ESimpleLogicalValueType::Timestamp:
                // TODO(dakovalkov): https://github.com/yandex/ClickHouse/pull/7170.
                // return EClickHouseColumnType::DateTime64;
                return EClickHouseColumnType::UInt64;

            default:
                THROW_ERROR_EXCEPTION("YT value type %Qlv not supported", simpleLogicalType);
        };
    } else {
        THROW_ERROR_EXCEPTION("Unexpected YT value metatype %Qlv, did you forget to call AdaptSchemaToClickHouse?",
            valueType->GetMetatype());
    }
}

TLogicalTypePtr RepresentClickHouseType(const DB::DataTypePtr& type)
{
    if (type->isNullable()) {
        return OptionalLogicalType(RepresentClickHouseType(DB::removeNullable(type)));
    }
    switch (type->getTypeId()) {
        case DB::TypeIndex::Int64:
            return SimpleLogicalType(ESimpleLogicalValueType::Int64);
        case DB::TypeIndex::Int32:
            return SimpleLogicalType(ESimpleLogicalValueType::Int32);
        case DB::TypeIndex::Int16:
            return SimpleLogicalType(ESimpleLogicalValueType::Int16);
        case DB::TypeIndex::Int8:
            return SimpleLogicalType(ESimpleLogicalValueType::Int8);
        case DB::TypeIndex::UInt64:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint64);
        case DB::TypeIndex::UInt32:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint32);
        case DB::TypeIndex::UInt16:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint16);
        case DB::TypeIndex::UInt8:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint8);
        case DB::TypeIndex::Float32:
        case DB::TypeIndex::Float64:
            return SimpleLogicalType(ESimpleLogicalValueType::Double);
        case DB::TypeIndex::String:
        case DB::TypeIndex::FixedString:
            return SimpleLogicalType(ESimpleLogicalValueType::String);
        case DB::TypeIndex::Date:
            return SimpleLogicalType(ESimpleLogicalValueType::Date);
        case DB::TypeIndex::DateTime:
            return SimpleLogicalType(ESimpleLogicalValueType::Datetime);
        // TODO(dakovalkov): https://github.com/yandex/ClickHouse/pull/7170.
        // case DB::TypeIndex::DateTime64:
        //     return SimpleLogicalType(ESimpleLogicalValueType::Timestamp);
        default:
            THROW_ERROR_EXCEPTION("Data type %v is not representable in YT", type->getFamilyName());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
