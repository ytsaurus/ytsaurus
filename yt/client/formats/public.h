#pragma once

#include <yt/core/misc/public.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TYsonFormatConfig)
DECLARE_REFCOUNTED_CLASS(TTableFormatConfigBase)
DECLARE_REFCOUNTED_CLASS(TYamrFormatConfig)
DECLARE_REFCOUNTED_CLASS(TYamrFormatConfigBase)
DECLARE_REFCOUNTED_CLASS(TDsvFormatConfig)
DECLARE_REFCOUNTED_CLASS(TDsvFormatConfigBase)
DECLARE_REFCOUNTED_CLASS(TYamredDsvFormatConfig)
DECLARE_REFCOUNTED_CLASS(TSchemafulDsvFormatConfig)
DECLARE_REFCOUNTED_CLASS(TProtobufColumnConfig)
DECLARE_REFCOUNTED_CLASS(TProtobufTableConfig)
DECLARE_REFCOUNTED_CLASS(TProtobufFormatConfig)
DECLARE_REFCOUNTED_CLASS(TProtobufFormatDescription)
DECLARE_REFCOUNTED_CLASS(TSchemalessWebJsonFormatConfig)
DECLARE_REFCOUNTED_CLASS(TSkiffFormatConfig)

DECLARE_REFCOUNTED_STRUCT(IYamrConsumer)

DECLARE_REFCOUNTED_STRUCT(ISchemalessFormatWriter)

DECLARE_REFCOUNTED_CLASS(TControlAttributesConfig)

struct IParser;

class TEscapeTable;

class TFormat;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
