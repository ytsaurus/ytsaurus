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
        bool escapingForWriter)
    {
        std::vector<char> valueStopSymbols;
        valueStopSymbols.push_back(recordSeparator);

        std::vector<char> keyStopSymbols = valueStopSymbols;
        keyStopSymbols.push_back(fieldSeparator);

        if (enableKeyEscaping) {
            if (escapingForWriter) {
                keyStopSymbols.push_back('\0');
                keyStopSymbols.push_back('\r');
            }
            keyStopSymbols.push_back(escapingSymbol);
        }

        if (enableValueEscaping) {
            if (escapingForWriter) {
                valueStopSymbols.push_back('\0');
                valueStopSymbols.push_back('\r');
            }
            valueStopSymbols.push_back(escapingSymbol);
        }

        KeyStops.Fill(keyStopSymbols);
        ValueStops.Fill(valueStopSymbols);
    }
};

} // namespace NFormats
} // namespace NYT


