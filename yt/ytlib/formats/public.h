#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

struct TDsvFormatConfig;
typedef TIntrusivePtr<TDsvFormatConfig> TDsvFormatConfigPtr;

struct TJsonFormatConfig;
typedef TIntrusivePtr<TJsonFormatConfig> TJsonFormatConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
