#pragma once

#include <core/misc/common.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TYsonFormatConfig;
typedef TIntrusivePtr<TYsonFormatConfig> TYsonFormatConfigPtr;

class TDsvFormatConfig;
typedef TIntrusivePtr<TDsvFormatConfig> TDsvFormatConfigPtr;

class TJsonFormatConfig;
typedef TIntrusivePtr<TJsonFormatConfig> TJsonFormatConfigPtr;

class TYamrFormatConfig;
typedef TIntrusivePtr<TYamrFormatConfig> TYamrFormatConfigPtr;

class TYamredDsvFormatConfig;
typedef TIntrusivePtr<TYamredDsvFormatConfig> TYamredDsvFormatConfigPtr;

class TSchemafulDsvFormatConfig;
typedef TIntrusivePtr<TSchemafulDsvFormatConfig> TSchemafulDsvFormatConfigPtr;

struct IYamrConsumer;
typedef TIntrusivePtr<IYamrConsumer> IYamrConsumerPtr;

struct IParser;

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
