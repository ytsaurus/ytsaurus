#pragma once

#include "public.h"
#include "symbols.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

struct TDsvTable
{
    TLookupTable KeyStops;
    TLookupTable ValueStops;
    TEscapeTable Escapes;

    TDsvTable(const TDsvFormatConfigPtr& config)
    {
        const char keyStopSymbols[] = {
            config->RecordSeparator,
            config->FieldSeparator,
            config->KeyValueSeparator,
            config->EscapingSymbol,
            '\0'
        };

        const char valueStopSymbols[] = {
            config->RecordSeparator,
            config->FieldSeparator,
            config->EscapingSymbol,
            '\0'
        };

        KeyStops.Fill(
            keyStopSymbols,
            keyStopSymbols + sizeof(keyStopSymbols));
        ValueStops.Fill(
            valueStopSymbols,
            valueStopSymbols + sizeof(valueStopSymbols));
    }
};

} // namespace NFormats
} // namespace NYT

