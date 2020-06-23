#pragma once

#include <util/system/types.h>

#include <type_traits>

namespace NSkiff {

enum class EWireType : ui8 {
    Nothing /* "nothing" */,
    Int64 /* "int64" */,
    Uint64 /* "uint64" */,
    Double /* "double" */,
    Boolean /* "boolean" */,
    String32 /* "string32" */,
    Yson32 /* "yson32" */,

    Tuple /* "tuple" */,
    Variant8 /* "variant8" */,
    Variant16 /* "variant16" */,
    RepeatedVariant8 /* "repeated_variant8" */,
    RepeatedVariant16 /* "repeated_variant16" */,
};

template <typename T>
constexpr T EndOfSequenceTag() {
    static_assert(std::is_integral<T>::value && std::is_unsigned<T>::value, "T must be unsigned integer");
    return T(-1);
}

}
