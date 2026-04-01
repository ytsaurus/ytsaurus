#pragma once
#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////ъ

template<NTableClient::ESimpleLogicalValueType LogicalType>
TTzIntegerType<LogicalType> GetTimestampFromTzString(std::string_view tzString) {
    auto [timestamp, _] = NTzTypes::ParseTzValue<TTzIntegerType<LogicalType>>(tzString);
    return timestamp;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
