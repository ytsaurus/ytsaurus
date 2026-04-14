#ifndef YSON_EXTRACT_TREE_INL_H_
#error "Direct inclusion of this file is not allowed, include yson_extract_tree.h"
// For the sake of sane code completion.
#include "yson_extract_tree.h"
#endif

#include <library/cpp/yt/error/error.h>

#include <base/extended_types.h>
#include <base/TypeName.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <Core/AccurateComparison.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

template <typename NumberType>
TError TryGetNumericValueFromYsonElement(NumberType& value, const TYsonParserAdapter::Element& element, bool convertBoolToNumber, bool allowTypeConversion)
{
    switch (element.type()) {
        case DB::ElementType::DOUBLE: {
            if constexpr (is_floating_point<NumberType>) {
                value = static_cast<NumberType>(element.getDouble());
            } else if (!allowTypeConversion || !accurate::convertNumeric<Float64, NumberType, false>(element.getDouble(), value)) {
                return TError("Cannot convert double value %v to %v", element.getDouble(), DB::TypeName<NumberType>);
            }
            break;
        }

        case DB::ElementType::UINT64: {
            if (!accurate::convertNumeric<UInt64, NumberType, false>(element.getUInt64(), value)) {
                return TError("Cannot convert UInt64 value %v to %v", element.getUInt64(), DB::TypeName<NumberType>);
            }
            break;
        }

        case DB::ElementType::INT64: {
            if (!accurate::convertNumeric<Int64, NumberType, false>(element.getInt64(), value)) {
                return TError("Cannot convert Int64 value %v to %v", element.getInt64(), DB::TypeName<NumberType>);
            }
            break;
        }

        case DB::ElementType::BOOL: {
            if (convertBoolToNumber && allowTypeConversion) {
                value = static_cast<NumberType>(element.getBool());
                break;
            }
            return TError("Cannot convert bool value to %v", DB::TypeName<NumberType>);
        }

        case DB::ElementType::STRING: {
            if (!allowTypeConversion) {
                return TError("");
            }

            auto buffer = DB::ReadBufferFromMemory{element.getString()};
            if constexpr (is_floating_point<NumberType>) {
                if (!DB::tryReadFloatText(value, buffer) || !buffer.eof()) {
                    return TError("Cannot parse %v value here: %Qv", DB::TypeName<NumberType>, element.getString());
                }
            } else {
                if (tryReadIntText(value, buffer) && buffer.eof()) {
                    break;
                }

                Float64 floatVal;
                buffer.position() = buffer.buffer().begin();
                if (!DB::tryReadFloatText(floatVal, buffer) || !buffer.eof()) {
                    return TError("Cannot parse %v value here: %Qv", DB::TypeName<NumberType>, element.getString());
                }

                if (!accurate::convertNumeric<Float64, NumberType, false>(floatVal, value)) {
                    return TError("Cannot parse %v value here: %Qv", DB::TypeName<NumberType>, element.getString());
                }
            }
            break;
        }

        default:
            return TError("");
    }

    return {};
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
