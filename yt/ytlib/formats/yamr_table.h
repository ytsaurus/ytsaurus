#pragma once

#include "public.h"
#include "symbols.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

struct TYamrTable
{
    TLookupTable KeyStops;
    TLookupTable ValueStops;
    TEscapeTable Escapes;

    TYamrTable(
        char fieldSeparator,
        char recordSeparator,
        bool enableKeyEscaping,
        bool enableValueEscaping,
        char escapingSymbol,
        bool escapeCarriageReturn)
            : Escapes(escapeCarriageReturn)
    {
        std::vector<char> valueStopSymbols;
        valueStopSymbols.push_back(recordSeparator);
        valueStopSymbols.push_back('\0');
        
        std::vector<char> keyStopSymbols = valueStopSymbols;
        keyStopSymbols.push_back(fieldSeparator);
        
        if (enableKeyEscaping) {
            keyStopSymbols.push_back(escapingSymbol);
        }
        
        if (enableValueEscaping) {
            valueStopSymbols.push_back(escapingSymbol);
        }

        if (escapeCarriageReturn) {
            keyStopSymbols.push_back('\r');
            valueStopSymbols.push_back('\r');
        }

        KeyStops.Fill(keyStopSymbols);
        ValueStops.Fill(valueStopSymbols);
    }
};

} // namespace NFormats
} // namespace NYT


