#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

struct TDsvFormatConfig;
typedef TIntrusivePtr<TDsvFormatConfig> TDsvFormatConfigPtr;

struct TJsonFormatConfig;
typedef TIntrusivePtr<TJsonFormatConfig> TJsonFormatConfigPtr;

struct TYamrFormatConfig;
typedef TIntrusivePtr<TYamrFormatConfig> TYamrFormatConfigPtr;

struct TYamredDsvFormatConfig;
typedef TIntrusivePtr<TYamredDsvFormatConfig> TYamredDsvFormatConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
