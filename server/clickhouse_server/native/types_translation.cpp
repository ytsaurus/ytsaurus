#include "types_translation.h"

#include <yt/ytlib/table_client/schema.h>

#include <yt/client/table_client/row_base.h>

#include <yt/core/misc/error.h>

#include <util/generic/hash.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

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
            return true;

        case EValueType::Null:
        case EValueType::Any:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            return false;
    };

    THROW_ERROR_EXCEPTION("Unexpected YT value type: %Qlv", valueType);
}

EColumnType RepresentYtType(EValueType valueType)
{
    switch (valueType) {
        /// Signed integer value.
        case EValueType::Int64:
            return EColumnType::Int64;

        /// Unsigned integer value.
        case EValueType::Uint64:
            return EColumnType::UInt64;

        /// Floating point value.
        case EValueType::Double:
            return EColumnType::Double;

        /// Boolean value.
        case EValueType::Boolean:
            return EColumnType::Boolean;

        /// String value.
        case EValueType::String:
            return EColumnType::String;

        case EValueType::Null:
        case EValueType::Any:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;
    }

    THROW_ERROR_EXCEPTION("YT value type %Qlv not supported", valueType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
