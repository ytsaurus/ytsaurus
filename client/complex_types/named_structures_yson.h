#pragma once

#include "public.h"

#include <yt/client/table_client/public.h>
#include <yt/core/yson/pull_parser.h>

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////


//
// There are two modes of representing complex types in yson: named and positional.
// They are similar for all types but Struct and VariantStruct.
//
// Consider Struct({{"field0", type0}, {"field1", type1}, ...})
// Its positional representation might be:
//   [v0; v1; ...] (v0 is of type0, v1 is of type1)
//
// Named representation of the same structure:
//   {field0=v0; field1=v1}
//
//
// Consider Struct({{"field0", type0}, {"field1", type1}, ...})
// Its positional representation might be:
//   [1, v1]
//
// Named representation of the same structure:
//   [field1; v1]
//
// Functions in this file create convertors between these two representations.

using TYsonConverter = std::function<void(NYson::TYsonPullParserCursor*, NYson::IYsonConsumer*)>;

struct TPositionalToNamedConfig
{
    // When SkipNullValues is true converters doesn't write
    // structure fields that have `#` value (i.e. they are Null).
    bool SkipNullValues = false;
};

TYsonConverter CreateNamedToPositionalYsonConverter(const NTableClient::TComplexTypeFieldDescriptor& descriptor);
TYsonConverter CreatePositionalToNamedYsonConverter(
    const NTableClient::TComplexTypeFieldDescriptor& descriptor,
    const TPositionalToNamedConfig& config);


// Helper method to reduce boilerplate code.
void ApplyYsonConverter(const TYsonConverter& converter, TStringBuf inputYson, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes