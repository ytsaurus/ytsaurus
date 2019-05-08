#include "type_translation.h"

#include <yt/ytlib/table_client/schema.h>

#include <yt/client/table_client/row_base.h>

#include <yt/core/misc/error.h>

#include <util/generic/hash.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

// YT native types

bool IsYtTypeSupported(EValueType valueType)
{
    switch (valueType) {
        case EValueType::Int64:
        case EValueType::Uint64:
        case EValueType::Double:
        case EValueType::Boolean:
        case EValueType::String:
        case EValueType::Any:
            return true;

        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            return false;
    };

    THROW_ERROR_EXCEPTION("Unexpected YT value type: %Qlv", valueType);
}

EClickHouseColumnType RepresentYtType(EValueType valueType)
{
    switch (valueType) {
        /// Signed integer value.
        case EValueType::Int64:
            return EClickHouseColumnType::Int64;

        /// Unsigned integer value.
        case EValueType::Uint64:
            return EClickHouseColumnType::UInt64;

        /// Floating point value.
        case EValueType::Double:
            return EClickHouseColumnType::Double;

        /// Boolean value.
        case EValueType::Boolean:
            return EClickHouseColumnType::Boolean;

        /// String value.
        case EValueType::String:
        case EValueType::Any:
            return EClickHouseColumnType::String;

        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;
    }

    THROW_ERROR_EXCEPTION("YT value type %Qlv not supported", valueType);
}

EValueType RepresentClickHouseType(const DB::DataTypePtr& type)
{
    switch (type->getTypeId()) {
        case DB::TypeIndex::Int64:
        case DB::TypeIndex::Int32:
        case DB::TypeIndex::Int16:
        case DB::TypeIndex::Int8:
            return EValueType::Int64;
        case DB::TypeIndex::UInt64:
        case DB::TypeIndex::UInt32:
        case DB::TypeIndex::UInt16:
        case DB::TypeIndex::UInt8:
            return EValueType::Uint64;
        case DB::TypeIndex::Float32:
        case DB::TypeIndex::Float64:
            return EValueType::Double;
        case DB::TypeIndex::String:
        case DB::TypeIndex::FixedString:
            return EValueType::String;
        default:
            THROW_ERROR_EXCEPTION("Data type %v is not representable in YT", type->getFamilyName());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
