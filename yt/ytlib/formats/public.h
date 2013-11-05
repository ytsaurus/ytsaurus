#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TDsvFormatConfig;
typedef TIntrusivePtr<TDsvFormatConfig> TDsvFormatConfigPtr;

class TJsonFormatConfig;
typedef TIntrusivePtr<TJsonFormatConfig> TJsonFormatConfigPtr;

class TYamrFormatConfig;
typedef TIntrusivePtr<TYamrFormatConfig> TYamrFormatConfigPtr;

class TYamredDsvFormatConfig;
typedef TIntrusivePtr<TYamredDsvFormatConfig> TYamredDsvFormatConfigPtr;

class TSchemedDsvFormatConfig;
typedef TIntrusivePtr<TSchemedDsvFormatConfig> TSchemedDsvFormatConfigPtr;

struct IYamrConsumer;
typedef TIntrusivePtr<IYamrConsumer> IYamrConsumerPtr;

struct IParser;

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
