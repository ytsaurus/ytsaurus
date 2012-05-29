#pragma once

#include "public.h"

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

struct TTsvFormatConfig
    : public TConfigurable
{
    char NewLineSeparator;
    char KeyValueSeparator;
    char ItemSeparator;

    TTsvFormatConfig()
    {
        Register("newline", NewLineSeparator).Default('\n');
        Register("key_value", KeyValueSeparator).Default('=');
        Register("item", ItemSeparator).Default('\t');
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
